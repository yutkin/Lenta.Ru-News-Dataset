# Corpus of news articles of Lenta.Ru
* Size: 337 Mb (2 Gb uncompressed)
* News articles: 800K+
* Dates: 30/08/1999 - 14/12/2019

+ [Script](../master/download_lenta.py) for news downloading (Python **3.7**+ is required).


# Download
* [GitHub](https://github.com/yutkin/Lenta.Ru-News-Dataset/releases)
* [Kaggle](https://www.kaggle.com/yutkin/corpus-of-russian-news-articles-from-lenta/)

# Decompression
`bzip2 -d lenta-ru-news.csv.bz2`

# Howto merge existing dataset with a new one
```
import pandas as pd

df1 = pd.read_csv("./lenta-ru-news-base.csv")
df1["datetime"] = pd.to_datetime(df1["date"], format="%Y/%m/%d")

df2 = pd.read_csv("./lenta-ru-news-patch.csv")
df2["datetime"] = pd.to_datetime(df2["date"], format="%Y/%m/%d")
 
df = pd.concat([df1, df2], ignore_index=True).sort_index()

df.drop_duplicates(subset="url", keep="last", inplace=True)

df.sort_values(by="datetime")

df[["date", "url", "topic", "tags", "title", "text"]].to_csv("./lenta-ru-news.csv", index=False)
```