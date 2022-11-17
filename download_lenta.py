import argparse
import asyncio
import csv
import logging
import os
import typing
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from itertools import chain
from multiprocessing import cpu_count
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s @ %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger(name="LentaParser")


class LentaParser:

    # lxml is much faster but error prone
    default_parser = "html.parser"

    def __init__(self, *, max_workers: int, outfile_name: str, from_date: str):
        self._endpoint = "https://lenta.ru/"

        self._sess = None
        self._connector = None

        self._executor = ProcessPoolExecutor(max_workers=max_workers)

        self._outfile_name = outfile_name
        self._is_outfile_exists = os.path.exists(self._outfile_name)
        self._outfile = None
        self._csv_writer = None
        self.timeouts = aiohttp.ClientTimeout(total=60, connect=60)

        self._existing_urls = set()
        if self._is_outfile_exists:
            with open(self._outfile_name, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self._existing_urls.add(row["url"])

        self._n_downloaded = len(self._existing_urls)
        self._from_date = datetime.strptime(from_date, "%d.%m.%Y")

        self._ignored_news_prefixes = (
            "https://lenta.ru/extlink/",
            "/extlink/",
            "/sport/",
            "/themes/",
            "/tags/",
        )

    @property
    def dates_countdown(self):
        date_start, date_end = self._from_date, datetime.today()

        while date_start <= date_end:
            yield date_start
            date_start += timedelta(days=1)

    @property
    def writer(self):
        if self._csv_writer is None:
            mode = "a" if self._is_outfile_exists else "w"
            self._outfile = open(self._outfile_name, mode=mode, buffering=1, encoding="utf-8")
            self._csv_writer = csv.DictWriter(
                self._outfile, fieldnames=["date", "url", "topic", "tags", "title", "text"]
            )
            if not self._is_outfile_exists:
                self._csv_writer.writeheader()

        return self._csv_writer

    @property
    def session(self):
        if self._sess is None or self._sess.closed:

            self._connector = aiohttp.TCPConnector(
                use_dns_cache=True, ttl_dns_cache=60 * 60, limit=1024
            )
            self._sess = aiohttp.ClientSession(
                connector=self._connector, timeout=self.timeouts
            )

        return self._sess

    async def fetch(self, url: str):
        response = await self.session.get(url, allow_redirects=False)
        response.raise_for_status()
        return await response.text(encoding="utf-8")

    @staticmethod
    def parse_article_html(html: str):
        doc_tree = BeautifulSoup(html, LentaParser.default_parser)
        tags = doc_tree.find("a", "rubric-header__link _active")
        tags = tags.get_text() if tags else None

        body = doc_tree.find("div", "topic-body__content")

        if not body:
            raise RuntimeError("Article body is not found")

        text = " ".join([p.get_text() for p in body.find_all("p", "topic-body__content-text")])

        topic = doc_tree.find("a", "topic-header__item topic-header__rubric")
        topic = topic.get_text() if topic else None

        title = doc_tree.find("h1", "topic-body__titles")
        title = title.get_text() if title else None

        return {"title": title, "text": text, "topic": topic, "tags": tags}

    def _fetch_next_page_url(self, html: str) -> typing.Optional[str]:
        doc_tree = BeautifulSoup(html, LentaParser.default_parser)
        nex_url_buttons = doc_tree.find_all("a", "loadmore js-loadmore _two-buttons")
        for item in nex_url_buttons:
            if item.find(string="Дальше"):
                return urljoin(self._endpoint, item.attrs["href"])

    @staticmethod
    def _extract_urls_from_html(endpoint: str, ignored_news_prefixes: typing.Tuple[str], html: str):
        doc_tree = BeautifulSoup(html, LentaParser.default_parser)
        news_list_top = doc_tree.find_all("a", "card-full-other _archive")
        news_list_bottom = doc_tree.find_all("a", "card-full-news _archive")
        return tuple(
            news["href"] if news["href"].startswith("http") else urljoin(endpoint, news["href"])
            for news in chain(news_list_top, news_list_bottom)
            if not news["href"].startswith(ignored_news_prefixes)
        )

    async def _fetch_all_news_on_page(self, date: datetime, html: str):
        # Get news URLs from raw html
        loop = asyncio.get_running_loop()
        news_urls = await loop.run_in_executor(
            self._executor, 
            self._extract_urls_from_html, 
            self._endpoint, 
            self._ignored_news_prefixes, 
            html
        )

        # Fetching news
        tasks = tuple(asyncio.create_task(self.fetch(url)) for url in news_urls)

        fetched_raw_news = dict()

        for i, task in enumerate(tasks):
            try:
                fetch_res = await task
            except aiohttp.ClientResponseError as exc:
                logger.error(f"Cannot fetch {exc.request_info.url}: {exc}")
            except asyncio.TimeoutError:
                logger.exception("Cannot fetch. Timout")
            else:
                fetched_raw_news[news_urls[i]] = fetch_res

        for url, html in fetched_raw_news.items():
            fetched_raw_news[url] = loop.run_in_executor(
                self._executor, self.parse_article_html, html
            )

        parsed_news = []

        for url, task in fetched_raw_news.items():
            try:
                parse_res = await task
            except Exception:
                logger.exception(f"Cannot parse {url}")
            else:
                if url not in self._existing_urls:
                    parse_res["url"] = url
                    parse_res["date"] = date.strftime("%Y/%m/%d")
                    parsed_news.append(parse_res)

        if parsed_news:
            self.writer.writerows(parsed_news)
            self._n_downloaded += len(parsed_news)

        return len(parsed_news)

    async def shutdown(self):
        if self._sess is not None:
            await self._sess.close()

        await asyncio.sleep(0.5)

        if self._outfile is not None:
            self._outfile.close()

        self._executor.shutdown(wait=True)

        logger.info(f"{self._n_downloaded} news saved at {self._outfile_name}")

    async def _producer(self):
        for date in self.dates_countdown:
            news_page_url = urljoin(self._endpoint, date.strftime("%Y/%m/%d"))

            while news_page_url:
                try:
                    html = await asyncio.create_task(self.fetch(news_page_url))
                except aiohttp.ClientResponseError:
                    logger.exception(f"Cannot fetch {news_page_url}")
                except aiohttp.ClientConnectionError:
                    logger.exception(f"Cannot fetch {news_page_url}")
                else:
                    n_proccessed_news = await self._fetch_all_news_on_page(date, html)

                    if n_proccessed_news == 0:
                        logger.info(f"News not found at {news_page_url}.")

                    logger.info(
                        f"{news_page_url} processed ({n_proccessed_news} news). "
                        f"{self._n_downloaded} news saved totally."
                    )

                    news_page_url = self._fetch_next_page_url(html)

    async def run(self):
        try:
            await self._producer()
        finally:
            await self.shutdown()


def main():
    parser = argparse.ArgumentParser(description="Downloads news from Lenta.Ru")

    parser.add_argument(
        "--outfile", default="lenta-ru-news.csv", help="name of result file"
    )

    parser.add_argument(
        "--cpu-workers", default=cpu_count(), type=int, help="number of workers"
    )

    parser.add_argument(
        "--from-date",
        default="30.08.1999",
        type=str,
        help="download news from this date. Example: 30.08.1999",
    )

    args = parser.parse_args()

    parser = LentaParser(
        max_workers=args.cpu_workers,
        outfile_name=args.outfile,
        from_date=args.from_date,
    )

    try:
        asyncio.run(parser.run())
    except KeyboardInterrupt:
        asyncio.run(parser.shutdown())
        logger.info("KeyboardInterrupt, exiting...")


if __name__ == "__main__":
    main()