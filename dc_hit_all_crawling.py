from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from pymysql import connect

from multiprocessing import Pool, Manager
import time
import sys
import logging
import logging.config
import logging.handlers
import warnings


class Crawling:
    def __init__(self):
        self.post_list = []
        self.url_num_tuple_list = []
        manager = Manager()
        self.reply_list = manager.list()
        self.len_url_tuple_list = manager.list()

    def execute(self, page, cnt) -> None:
        self.get_post_list(page)
        self.insert_post_list()
        self.post_list = []

        for _ in range((cnt - 1) // 6 + 1):
            pool = Pool(processes=3)
            tmp = []
            for _ in range(6):
                if self.url_num_tuple_list:
                    tmp.append(self.url_num_tuple_list.pop())
            pool.map(self.get_content, tmp)
            logging.debug("Replies Crawled")
            pool.close()
            pool.join()
            self.insert_reply()
            self.reply_list[:] = []
        self.update_content_len()
        self.len_url_tuple_list[:] = []

    def connect_to_db(self) -> connect:
        conn = connect(
            host="db-community.chytu2uaulrn.ap-northeast-2.rds.amazonaws.com",
            user="leesoomok",
            password="!zx1421568400",
            db="crawler_data",
            charset="utf8",
        )
        return conn

    def open_driver(self) -> webdriver:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--dns-prefetch-disable")
        prefs = {
            "profile.default_content_setting_values": {
                "cookies": 2,
                "images": 2,
                "plugins": 2,
                "popups": 2,
                "geolocation": 2,
                "notifications": 2,
                "auto_select_certificate": 2,
                "fullscreen": 2,
                "mouselock": 2,
                "mixed_script": 2,
                "media_stream": 2,
                "media_stream_mic": 2,
                "media_stream_camera": 2,
                "protocol_handlers": 2,
                "ppapi_broker": 2,
                "automatic_downloads": 2,
                "midi_sysex": 2,
                "push_messaging": 2,
                "ssl_cert_decisions": 2,
                "metro_switch_to_desktop": 2,
                "protected_media_identifier": 2,
                "app_banner": 2,
                "site_engagement": 2,
                "durable_storage": 2,
            }
        }
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        driver = webdriver.Chrome(options=options)
        driver.implicitly_wait(5)
        return driver

    def get_post_list(self, page) -> None:
        base_url = "https://gall.dcinside.com/board/lists/?id=hit&list_num=100&sort_type=N&exception_mode=recommend&search_head=&page="
        try:
            reqUrl = Request(
                base_url + str(page),
                headers={"User-Agent": "Mozilla/5.0"},
            )

            html = urlopen(reqUrl)
            soup = BeautifulSoup(html, "html.parser")

            soup = soup.find("tbody")
            for i in soup.find_all("tr"):
                if (
                    i.find("td", "gall_num").text.strip() == "설문"
                    or i.find("td", "gall_num").text.strip() == "공지"
                    or i.find("td", "gall_num").text.strip() == "이슈"
                    or i.find("td", "gall_num").text.strip() == "AD"
                ):
                    continue

                url = (
                    "https://gall.dcinside.com"
                    + i.find(
                        "td",
                        {
                            "class": [
                                "gall_tit ub-word",
                                "gall_tit ub-word voice_tit",
                            ]
                        },
                    ).find_all("a")[0]["href"]
                )

                title = (
                    i.find(
                        "td",
                        {
                            "class": [
                                "gall_tit ub-word",
                                "gall_tit ub-word voice_tit",
                            ]
                        },
                    )
                    .find_all("a")[0]
                    .text.strip()
                )

                replyNum = (
                    i.find(
                        "td",
                        {
                            "class": [
                                "gall_tit ub-word",
                                "gall_tit ub-word voice_tit",
                            ]
                        },
                    )
                    .find_all("a")[1]
                    .text.strip()
                    .replace("[", "")
                    .replace("]", "")
                    .replace(",", "")
                )

                timeString = i.find("td", "gall_date")["title"]
                timeValue = datetime.strptime(timeString, "%Y-%m-%d %H:%M:%S")

                voteNum = i.find("td", "gall_recommend").text.strip().replace(",", "")

                viewNum = i.find("td", "gall_count").text.strip().replace(",", "")

                num = i.find("td", "gall_num").text.strip().replace(",", "")

                self.post_list.append(
                    (
                        num,
                        url,
                        title,
                        replyNum,
                        viewNum,
                        voteNum,
                        timeValue.strftime("%Y-%m-%d %H:%M:%S"),
                        url,
                        title,
                        replyNum,
                        viewNum,
                        voteNum,
                        timeValue.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )
                self.url_num_tuple_list.append((url, num))

        except Exception as e:
            logging.error(f"Failed to crawl post_list: {str(e)}")

        finally:
            logging.debug(f"{len(self.post_list)} Posts Crawled")

    def get_content(self, url_num_tuple) -> None:
        url, num = url_num_tuple
        driver = self.open_driver()
        try:
            driver.get(url)
        except TimeoutException as te:
            print(str(te))
            driver.navigate().refresh()
        try:
            content_element = driver.find_element_by_css_selector(
                "main#container > section > article:nth-child(3) > div.view_content_wrap > div > div.inner.clear > div.writing_view_box > div",
            )
            content = content_element.text.strip()
            content = (
                ((content.replace("\xa0", "")).replace(" ", "")).replace("\n", "")
            ).replace("-dcofficialApp", "")
            content = content.split("출처:")
            content.pop()

            reply_list = []
            i = 1
            btn_xpath = "/html/body/div[2]/div[2]/main/section/article[2]/div[3]/div[1]/div[2]/div/div[1]/a"
            while True:
                try:
                    reply_elements = driver.find_elements_by_class_name("usertxt")
                    for e in reply_elements:
                        text = e.text
                        text = text.replace("\n", " ")
                        if text[-9:] == " - dc App":
                            text = text[:-9]
                        reply_list.append(text)
                    btn_element = driver.find_element_by_xpath(btn_xpath + f"[{i}]")
                    driver.execute_script("arguments[0].click();", btn_element)
                    i += 1
                except Exception:
                    break

            reply_list = list(set(reply_list))
            self.reply_list += [[x, y, y] for x in [num] for y in reply_list]
            self.len_url_tuple_list.append((len(content[0]), url))

        except Exception as e:
            logging.error(f"Failed to get content: {str(e)}")
            logging.error(f'Site: "힛갤" Url: {url} Num: {num}')

        finally:
            driver.quit()

    def insert_post_list(self) -> None:
        try:
            insert_post_list_sql = "INSERT INTO post_table (site, num, url, title, replyNum, viewNum, voteNum, timeUpload) VALUES ('힛갤', %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE url = %s, title = %s, replyNum = %s, viewNum = %s, voteNum = %s, timeUpload = %s"
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
            insert_reply_sql = "INSERT IGNORE INTO reply_table (site, num, reply, reply_hash) VALUES ('힛갤', %s, %s, UNHEX(MD5(%s)))"
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
                "filename": "dc_hit_error.log",
                "formatter": "complex",
                "encoding": "utf-8",
                "level": "ERROR",
            },
        },
        "root": {"handlers": ["console", "file"], "level": "DEBUG"},
    }
    logging.config.dictConfig(config)
    root_logger = logging.getLogger()

    with open("dc_hit_count.txt", "r") as file:
        data = file.read().splitlines()[-1]
        if data == "1":
            logging.info("SOP")
            sys.exit(0)

    data = int(data) - 1
    c = Crawling()
    start = time.time()
    c.execute(page=1, cnt=1)
    end = time.time()
    logging.debug(f"{(end - start):.1f}s")
    with open("dc_hit_count.txt", "w") as file:
        file.write(f"{data}")

# 2021-11-22: page 188
