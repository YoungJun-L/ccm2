from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from pymysql import connect

from multiprocessing import Pool, Manager
import time
import sys
import logging
import logging.config
import logging.handlers


class Crawling:
    def __init__(self):
        self.url_num_tuple_list = []
        manager = Manager()
        self.reply_list = manager.list()
        self.len_url_tuple_list = manager.list()

    def map_pool(self, cnt) -> None:
        pool = Pool(processes=4)
        tmp = []
        for _ in range(cnt):
            tmp.append(self.url_num_tuple_list.pop())
        pool.map(self.get_content, tmp)
        logging.debug("Replies Crawled")
        pool.close()
        pool.join()

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
        base_url = "https://gall.dcinside.com/board/lists/?id=dcbest&list_num=100&sort_type=N&exception_mode=recommend&search_head=&page="
        post_list = []
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

                post_list.append(
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
            logging.debug(f"{len(post_list)} Posts Crawled")
            self.insert_post_list(post_list)

    def get_content(self, url_num_tuple) -> None:
        url, num = url_num_tuple
        driver = self.open_driver()
        driver.get(url)
        try:
            content_element = driver.find_element_by_css_selector(
                "main#container > section > article:nth-child(3) > div.view_content_wrap > div > div.inner.clear > div.writing_view_box > div"
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
            logging.error(f'Site: "실베" Url: {url} Num: {num}')

        finally:
            driver.quit()

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

    def insert_post_list(self, post_list) -> None:
        try:
            insert_post_list_sql = "INSERT INTO post_table (site, num, url, title, replyNum, viewNum, voteNum, timeUpload) VALUES ('실베', %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE url = %s, title = %s, replyNum = %s, viewNum = %s, voteNum = %s, timeUpload = %s"
            conn = self.connect_to_db()
            cursor = conn.cursor()

            cursor.executemany(insert_post_list_sql, post_list)

        except Exception as e:
            logging.error(f"Failed to insert post_list: {str(e)}")

        finally:
            conn.commit()
            conn.close()
            logging.debug("Post_list Inserted")

    def insert_reply(self) -> None:
        try:
            insert_reply_sql = "INSERT IGNORE INTO reply_table (site, num, reply, reply_hash) VALUES ('실베', %s, %s, UNHEX(MD5(%s)))"
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


if __name__ == "__main__":
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
                "filename": "dc_realtime_error.log",
                "formatter": "complex",
                "encoding": "utf-8",
                "level": "ERROR",
            },
        },
        "root": {"handlers": ["console", "file"], "level": "DEBUG"},
    }
    logging.config.dictConfig(config)
    root_logger = logging.getLogger()

    with open("dc_realtime_count.txt", "r") as file:
        data = file.read().splitlines()[-1]
        # if data == "1":
        #     logging.info("SOP")
        #     sys.exit(0)

    data = int(data) - 1
    c = Crawling()
    start = time.time()
    c.get_post_list(1)
    c.map_pool(4)
    c.update_content_len()
    c.insert_reply()
    end = time.time()
    logging.debug(f"{(end - start):.1f}s")
    with open("dc_realtime_count.txt", "w") as file:
        file.write(f"{data}")
