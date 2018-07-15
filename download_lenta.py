import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from itertools import islice
from multiprocessing import cpu_count

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(name="LentaParser")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s @ %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)


class LentaParser:
    def __init__(
            self, max_workers=cpu_count(), max_async_tasks=25, outfile_name="news_lenta.csv"
    ):
        self.queue = asyncio.Queue()
        self._endpoint = "https://lenta.ru/news"
        self._sess = None
        self.loop = asyncio.get_event_loop()
        self.dates_countdown = self.dates_countdown()
        self.max_async_tasks = max_async_tasks
        self.max_workers = max_workers
        self.outfile_name = outfile_name
        self.news_fetched = []

    def dates_countdown(self):
        date = datetime(year=2000, day=1, month=1)
        # date = datetime.today()
        while True:
            yield date.strftime("%Y/%m/%d")
            try:
                date -= timedelta(days=1)
            except OverflowError:
                return

    @property
    def session(self):
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession()
        return self._sess

    async def fetch(self, url):
        response = await self.session.get(url, allow_redirects=False)

        logger.debug(f"{url} ({response.status})")

        text = None
        if response.status == 200:
            text = await response.text()
        return text

    @staticmethod
    def parse_article_html(html):
        doc_tree = BeautifulSoup(html, "lxml")
        tags = doc_tree.find("a", "item dark active")
        tags = tags.get_text() if tags else None
        paragraphs = doc_tree.find("div", attrs={"itemprop": "articleBody"}).find_all(
            "p"
        )
        text = " ".join([p.get_text() for p in paragraphs])
        topic = doc_tree.find("a", "b-header-inner__block").get_text()
        title = doc_tree.find("h1", attrs={"itemprop": "headline"}).get_text()

        return {"title": title, "text": text, "topic": topic, "tags": tags}

    async def fetch_all_news_on_page(self, feth_news_page_coro):
        html = await feth_news_page_coro
        if html:
            doc_tree = BeautifulSoup(html, "lxml")
            news_list = doc_tree.find_all("div", "item news b-tabloid__topic_news")
            news_urls = [
                f"https://lenta.ru{news.find('a')['href']}" for news in news_list
            ]

            tasks = (asyncio.ensure_future(self.fetch(url)) for url in news_urls)

            fetched_news_html = await asyncio.gather(*tasks)

            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for article_html in fetched_news_html:
                    futures.append(
                        executor.submit(self.parse_article_html, article_html)
                    )

                for article_url, future in zip(news_urls, as_completed(futures)):
                    try:
                        processed_artile = future.result()
                        processed_artile["url"] = article_url
                        self.news_fetched.append(processed_artile)

                    except Exception:
                        logger.exception(f"Error while processing {article_url}")

    async def producer(self):
        for date in islice(self.dates_countdown, self.max_async_tasks):
            news_page_url = f"{self._endpoint}/{date}"
            fut = asyncio.ensure_future(self.fetch(news_page_url))
            await self.fetch_all_news_on_page(fut)

    def run(self):
        try:
            self.loop.run_until_complete(self.producer())
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt, exiting...")
        finally:
            if self._sess:
                self.loop.run_until_complete(self._sess.close())
            self.loop.stop()
            if self.news_fetched:
                logger.info(f"{len(self.news_fetched)} news total processed. "
                            f"Writing to {self.outfile_name}...")
                pd.DataFrame(self.news_fetched).to_csv(
                    self.outfile_name, encoding="utf-8", index=None
                )


def main():
    parser = LentaParser()
    parser.run()


if __name__ == "__main__":
    main()
