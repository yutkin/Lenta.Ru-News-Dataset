import asyncio
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import cpu_count
import argparse

import aiohttp
from bs4 import BeautifulSoup
import csv


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s @ %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger(name="LentaParser")


class LentaParser:
    def __init__(self, *, max_workers: int, outfile_name: str):
        self._endpoint = "https://lenta.ru/news"

        self._sess = None
        self._connector = None
        self._read_timeout = 10
        self._conn_timeout = 10

        self._executor = ProcessPoolExecutor(max_workers=max_workers)

        self._outfile_name = outfile_name
        self._outfile = None
        self._csv_writer = None

        self._n_downloaded = 0

    @property
    def dates_countdown(self):
        date = datetime.today()
        while True:
            yield date.strftime("%Y/%m/%d")
            try:
                date -= timedelta(days=1)
            except OverflowError:
                return

    @property
    def writer(self):
        if self._csv_writer is None:
            self._outfile = open(self._outfile_name, "w", 1)
            self._csv_writer = csv.DictWriter(
                self._outfile, fieldnames=["url", "title", "text", "topic", "tags"]
            )
            self._csv_writer.writeheader()

        return self._csv_writer

    @property
    def session(self):
        if self._sess is None or self._sess.closed:

            self._connector = aiohttp.TCPConnector(
                use_dns_cache=True, ttl_dns_cache=60 * 60, limit=512
            )

            self._sess = aiohttp.ClientSession(
                connector=self._connector,
                read_timeout=self._read_timeout,
                conn_timeout=self._conn_timeout,
            )

        return self._sess

    async def fetch(self, url: str):
        response = await self.session.get(url, allow_redirects=False)
        response.raise_for_status()
        text = await response.text(encoding="utf-8")
        return text

    @staticmethod
    def parse_article_html(html: str):
        doc_tree = BeautifulSoup(html, "lxml")
        tags = doc_tree.find("a", "item dark active")
        tags = tags.get_text() if tags else None

        body = doc_tree.find("div", attrs={"itemprop": "articleBody"})

        if not body:
            raise RuntimeError(f"Could not find div with itemprop=articleBody")

        text = " ".join([p.get_text() for p in body.find_all("p")])

        topic = doc_tree.find("a", "b-header-inner__block")
        topic = topic.get_text() if topic else None

        title = doc_tree.find("h1", attrs={"itemprop": "headline"})
        title = title.get_text() if title else None

        return {"title": title, "text": text, "topic": topic, "tags": tags}

    @staticmethod
    def _extract_news_urls_from_raw_html(html: str):
        doc_tree = BeautifulSoup(html, "lxml")
        news_list = doc_tree.find_all("div", "item news b-tabloid__topic_news")
        news_urls = [f"https://lenta.ru{news.find('a')['href']}" for news in news_list]
        return news_urls

    async def _fetch_all_news_on_page(self, html: str):
        loop = asyncio.get_running_loop()

        # Get news URLs from raw html
        news_urls = await loop.run_in_executor(
            self._executor, self._extract_news_urls_from_raw_html, html
        )

        # Fetching news
        tasks = []
        for url in news_urls:
            tasks.append(asyncio.create_task(self.fetch(url)))

        fetched_raw_news = []

        for i, task in enumerate(tasks):
            try:
                res = await task
            except Exception as exc:
                logger.error(f"Cannot fetch {news_urls[i]}: {exc}")
                news_urls.pop(i)
            else:
                fetched_raw_news.append(res)

        tasks.clear()

        for raw_news in fetched_raw_news:
            tasks.append(
                loop.run_in_executor(self._executor, self.parse_article_html, raw_news)
            )

        parsed_news = []

        for i, task in enumerate(tasks):
            try:
                res = await task
            except Exception as exc:
                logger.error(f"Error while parse {news_urls[i]}: {exc}")
                news_urls.pop(i)
            else:
                parsed_news.append(res)

        n_futures = 0

        for i, news in enumerate(parsed_news):
            news["url"] = news_urls[i]
            n_futures += 1

        if n_futures > 0:
            self.writer.writerows(parsed_news)
            self._n_downloaded += n_futures

        return n_futures

    async def _shutdown(self):
        if self._sess is not None:
            await self._sess.close()

        await asyncio.sleep(0.250)

        if self._outfile is not None:
            self._outfile.close()

        self._executor.shutdown(wait=True)

        logger.info(f"{self._n_downloaded} news saved at {self._outfile_name}")

    async def _producer(self):
        for date in self.dates_countdown:
            news_page_url = f"{self._endpoint}/{date}"

            try:
                html = await asyncio.create_task(self.fetch(news_page_url))
            except aiohttp.ClientResponseError as exc:
                logger.error(f"Cannot fetch {news_page_url} [{exc.status}]")
            else:
                n_proccessed_news = await self._fetch_all_news_on_page(html)

                if n_proccessed_news == 0:
                    logger.info(f"News not found on {news_page_url}. Stopping...")
                    return

                logger.info(
                    f"{news_page_url} processed ({n_proccessed_news} news). "
                    f"{self._n_downloaded} news saved totally."
                )

    async def run(self):
        try:
            await self._producer()
        finally:
            await self._shutdown()


def main():
    parser = argparse.ArgumentParser(description="Downloads news from Lenta.Ru")

    parser.add_argument(
        "--outfile", default="lenta-ru-news.csv", help="name of result file"
    )

    parser.add_argument(
        "--cpu-workers", default=cpu_count(), type=int, help="number of cpu workers"
    )

    args = parser.parse_args()

    try:
        asyncio.run(
            LentaParser(max_workers=args.cpu_workers, outfile_name=args.outfile).run()
        )
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, exiting...")


if __name__ == "__main__":
    main()
