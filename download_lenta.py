import asyncio
import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from multiprocessing import cpu_count

import aiohttp
import uvloop
from bs4 import BeautifulSoup
import csv

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logger = logging.getLogger(name="LentaParser")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s @ %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)


class LentaParser:

  def __init__(self, max_workers=cpu_count(), outfile_name=None):
    self.queue = asyncio.Queue()
    self._endpoint = "https://lenta.ru/news"
    self._sess = None
    self.loop = asyncio.get_event_loop()
    self.max_workers = max_workers
    self.outfile_name = outfile_name
    if not outfile_name:
      unixtime = int(time.time())
      self.outfile_name = f"news_lenta_{unixtime}.csv"

    self.outfile = open(self.outfile_name, "w", 1)
    self.csv_writer = csv.DictWriter(
        self.outfile, fieldnames=["url", "title", "text", "topic", "tags"])
    self.csv_writer.writeheader()
    self.n_downloaded = 0

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
    else:
      logger.warning(f"Cannot fetch {url}")
    return text

  @staticmethod
  def parse_article_html(html, url):
    doc_tree = BeautifulSoup(html, "lxml")
    tags = doc_tree.find("a", "item dark active")
    tags = tags.get_text() if tags else None

    body = doc_tree.find("div", attrs={"itemprop": "articleBody"})
    if not body:
      raise RuntimeError(
          f"Could not find div with itemprop=articleBody in {url}")

    text = " ".join([p.get_text() for p in body.find_all("p")])

    topic = doc_tree.find("a", "b-header-inner__block")
    topic = topic.get_text() if topic else None

    title = doc_tree.find("h1", attrs={"itemprop": "headline"})
    title = title.get_text() if title else None

    return {
        "title": title,
        "text": text,
        "topic": topic,
        "tags": tags,
        "url": url
    }

  async def fetch_all_news_on_page(self, feth_news_page_coro):
    html = await feth_news_page_coro
    if html:
      doc_tree = BeautifulSoup(html, "lxml")
      news_list = doc_tree.find_all("div", "item news b-tabloid__topic_news")
      news_urls = [
          f"https://lenta.ru{news.find('a')['href']}" for news in news_list
      ]

      tasks = []
      for url in news_urls:
        task = asyncio.Task(self.fetch(url))
        task.url = url
        tasks.append(task)

      if not tasks:
        logger.warning(f"There no articles at {feth_news_page_coro.url}")
        return True  # If no any task, try to fetch new one

      done, _ = await asyncio.wait(tasks)

      with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
        futures = []
        for article_html, article_url in ((feat.result(), feat.url)
                                          for feat in done):
          futures.append(
              executor.submit(self.parse_article_html, article_html,
                              article_url))

      for fut in as_completed(futures):
        try:
          processed_artile = fut.result()
          self.csv_writer.writerow(processed_artile)
          self.n_downloaded += 1

        except Exception:
          logger.exception("Cannot parse...")
      logger.info(f"#{self.n_downloaded} news processed.")
    return html is not None

  async def producer(self):
    for date in self.dates_countdown:
      news_page_url = f"{self._endpoint}/{date}"
      fut = asyncio.Task(self.fetch(news_page_url))
      fut.url = news_page_url
      ok = await self.fetch_all_news_on_page(fut)
      if not ok:
        break

  def run(self):
    try:
      self.loop.run_until_complete(self.producer())
    except KeyboardInterrupt:
      logger.info("KeyboardInterrupt, exiting...")
    finally:
      if self._sess:
        self.loop.run_until_complete(self._sess.close())
      self.loop.stop()
      if self.n_downloaded:
        logger.info(
            f"{self.n_downloaded} news processed totally. Saved at {self.outfile_name}"
        )

      self.outfile.close()


def main():
  LentaParser().run()


if __name__ == "__main__":
  main()
