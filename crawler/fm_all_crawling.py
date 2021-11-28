from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from pymysql import connect

import time
import random
import datetime
import sys
import logging
import logging.config
import logging.handlers
import warnings


class Crawling:
    def __init__(self):
        self.post_list = []
        self.url_num_tuple_list = []
        self.reply_list = []
        self.len_url_tuple_list = []

    def execute(self, page, cnt) -> None:
        self.get_post_list(page)
        self.insert_post_list()
        del self.post_list
        for _ in range(cnt):
            if self.url_num_tuple_list:
                self.get_content(self.url_num_tuple_list.pop())
        self.insert_reply()
        self.update_content_len()

    def connect_to_db(self) -> connect:
        conn = connect(
            host="db-community.chytu2uaulrn.ap-northeast-2.rds.amazonaws.com",
            user="leesoomok",
            password="!zx1421568400",
            db="crawler_data",
            charset="utf8",
        )
        return conn

    def get_post_list(self, page) -> None:
        base_url = "https://www.fmkorea.com/index.php?mid=best&listStyle=list&page="
        try:
            reqUrl = Request(
                base_url + str(page),
                headers={"User-Agent": "Mozilla/5.0"},
            )

            html = urlopen(reqUrl)
            soup = BeautifulSoup(html, "html.parser")

            soup = soup.find("tbody")
            for i in soup.find_all("tr"):
                url = "https://www.fmkorea.com" + i.find("a", "hx")["href"]

                title = i.find("a", "hx").text.strip()

                replyNum = i.find("a", "replyNum").text.strip().replace(",", "")

                timeString = i.find("td", "time").text.strip().split(":")

                if len(timeString) == 1:
                    timeValue = timeString[0] + " 00:00:00"

                else:
                    timeValue = datetime.datetime.combine(
                        datetime.date.today(),
                        datetime.time(int(timeString[0]), int(timeString[1])),
                    ).strftime("%Y-%m-%d %H:%M:%S")

                voteNum = i.find_all("td", "m_no")[0].text.strip().replace(",", "")

                viewNum = i.find_all("td", "m_no")[1].text.strip().replace(",", "")

                document_num = url.find("document_srl=")
                num = url[document_num:].replace("document_srl=", "").strip()

                self.post_list.append(
                    (
                        num,
                        url,
                        title,
                        replyNum,
                        viewNum,
                        voteNum,
                        timeValue,
                        url,
                        title,
                        replyNum,
                        viewNum,
                        voteNum,
                        timeValue,
                    )
                )
                self.url_num_tuple_list.append((url, num))

        except Exception as e:
            logging.error(f"Failed to crawl post_list: {str(e)}")

        finally:
            logging.debug(f"{len(self.post_list)} Posts Crawled")

    def get_content(self, url_num_tuple) -> None:
        time.sleep(random.randint(2, 3))
        url, num = url_num_tuple
        try:
            reqUrl = Request(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            html = urlopen(reqUrl)
            soup = BeautifulSoup(html, "html.parser")
            content = soup.select_one("#bd_capture > div.rd_body.clear > article")
            content_len = len(content.text.replace("\xa0", "").replace(" ", ""))

            reply_list = []
            soup = soup.find("ul", "fdb_lst_ul").find_all("li")
            for e in soup:
                reply_element = e.find_all("div")[1]
                if reply_element.a:
                    reply_element.a.decompose()
                reply = reply_element.text.strip()
                reply_list.append(reply)

            reply_list = list(set(reply_list))
            self.reply_list += [(x, y, y) for x in [num] for y in reply_list]

        except Exception as e:
            logging.error(f"Failed to get content: {str(e)}")
            logging.error(f'Site: "FM" Url: {url} Num: {num}')

        finally:
            logging.debug("Content_len, Replies Crawled")
            self.len_url_tuple_list.append((content_len, url))

    def insert_post_list(self) -> None:
        try:
            insert_post_list_sql = "INSERT INTO post_table (site, num, url, title, replyNum, viewNum, voteNum, timeUpload) VALUES ('FM', %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE url = %s, title = %s, replyNum = %s, viewNum = %s, voteNum = %s, timeUpload = %s"
            conn = self.connect_to_db()
            cursor = conn.cursor()
            cursor.executemany(insert_post_list_sql, self.post_list)

        except Exception as e:
            logging.error(f"Failed to insert post_list: {str(e)}")

        finally:
            conn.commit()
            conn.close()
            logging.debug("Post_list Inserted")

    def insert_reply(self) -> None:
        try:
            insert_reply_sql = "INSERT IGNORE INTO reply_table (site, num, reply, reply_hash) VALUES ('FM', %s, %s, UNHEX(MD5(%s)))"
            conn = self.connect_to_db()
            cursor = conn.cursor()
            cursor.executemany(
                insert_reply_sql,
                self.reply_list,
            )

        except Exception as e:
            logging.error(f"Failed to save reply: {str(e)}")

        finally:
            conn.commit()
            conn.close()
            logging.debug(f"{len(self.reply_list)} Replies Inserted")

    def update_content_len(self) -> None:
        try:
            update_len_sql = "UPDATE post_table SET len = %s WHERE url = %s"
            conn = self.connect_to_db()
            cursor = conn.cursor()
            cursor.executemany(update_len_sql, self.len_url_tuple_list)

        except Exception as e:
            logging.error(f"Failed to update content_len: {str(e)}")

        finally:
            conn.commit()
            conn.close()
            logging.debug("Content_len Updated")


if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    config = {
        "version": 1,
        "formatters": {
            "complex": {
                "format": "%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s"
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "complex",
                "level": "DEBUG",
            },
            "file": {
                "class": "logging.FileHandler",
                "filename": "fm_all_error.log",
                "formatter": "complex",
                "encoding": "utf-8",
                "level": "ERROR",
            },
        },
        "root": {"handlers": ["console", "file"], "level": "DEBUG"},
    }
    logging.config.dictConfig(config)
    root_logger = logging.getLogger()

    with open("fm_count.txt", "r") as file:
        data = file.read().splitlines()[-1]
        if data == "0":
            logging.info("SOP")
            sys.exit(0)

    data = int(data) - 1
    c = Crawling()
    start = time.time()
    c.execute(page=data, cnt=20)
    end = time.time()
    logging.debug(f"{(end - start):.1f}s")
    with open("fm_count.txt", "w") as file:
        file.write(f"{data}")

# 2021-11-22: page 10000
