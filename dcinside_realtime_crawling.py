from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from pymysql import connect

from multiprocessing import Pool, Manager
import time
import logging

import pandas as pd

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("dc_realtime.log", "w", "utf-8")
root_logger.addHandler(handler)


class Crawling:
    def __init__(self):
        start = time.time()
        self.url_num_tuple_list = []
        manager = Manager()
        self.reply_list = manager.list()
        self.len_url_tuple_list = manager.list()
        self.get_post_list()

        pool = Pool(processes=4)
        pool.map(self.get_content, self.url_num_tuple_list)
        pool.close()
        pool.join()

        self.update_content_len()
        self.save_reply()

        end = time.time()
        print()
        print("********************************************")
        print("********************************************")
        print("전체 수행 시간: ", end - start)
        print("********************************************")
        print("********************************************")
        print()
        logging.debug(f"전체 수행 시간: {end - start}s")

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
        options.add_argument("--headless")  # headless 모드 설정
        options.add_argument("--no-sandbox")  # 화면크기(전체화면)
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

    def get_post_list(self) -> None:
        base_url = "https://gall.dcinside.com/board/lists/?id=dcbest&list_num=100&sort_type=N&exception_mode=recommend&search_head=&page="
        post_list = []
        startPage = 0
        while True:
            startPage = startPage + 1

            reqUrl = Request(
                base_url + str(startPage),
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
                # timeStandard = i.find("td", "gall_date").text
                # timeStandard.find(".") != -1 or
                if startPage == 10:
                    print("********************************************")
                    print("********************************************")
                    print("긁어온 게시글 수: ", len(post_list))
                    print("********************************************")
                    print("********************************************")
                    self.insert_post_list(post_list)
                    return

                print()

                voteNum = i.find("td", "gall_recommend").text.strip().replace(",", "")

                viewNum = i.find("td", "gall_count").text.strip().replace(",", "")

                num = i.find("td", "gall_num").text.strip().replace(",", "")

                print(
                    "실베-",
                    num,
                    " URL : ",
                    url,
                    "제목 : ",
                    title,
                    " 댓글수 : ",
                    replyNum,
                    " 시간 : ",
                    timeValue.strftime("%Y-%m-%d %H:%M:%S"),
                    " 추천수 : ",
                    voteNum,
                    " 조회수 : ",
                    viewNum,
                )

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
            self.reply_list.append(
                [[x, y, z] for x in ["실베"] for y in [num] for z in reply_list]
            )

            print()
            print("********************************************")
            print("긁어온 게시글: ", url)
            print("긁어온 댓글 수: ", len(reply_list))
            print("********************************************")
            print()

            self.len_url_tuple_list.append((len(content[0]), url))

        except Exception:
            logging.error(f'사이트: "실베" 주소: {url} 번호: {num}')
            print("********************************************")
            print(Exception)
            print("********************************************")
            pass

        finally:
            driver.quit()

    def update_content_len(self) -> None:
        update_len_sql = "UPDATE post_table SET len = %s WHERE url = %s"
        conn = self.connect_to_db()
        cursor = conn.cursor()

        start = time.time()
        cursor.executemany(update_len_sql, self.len_url_tuple_list)
        end = time.time()
        print("본문 길이 전체 업데이트 쿼리 시간: ", end - start)

        conn.commit()
        conn.close()

    def insert_post_list(self, post_list) -> None:
        insert_post_list_sql = "INSERT INTO post_table (site, num, url, title, replyNum, viewNum, voteNum, timeUpload) VALUES ('실베', %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE url = %s, title = %s, replyNum = %s, viewNum = %s, voteNum = %s, timeUpload = %s"
        conn = self.connect_to_db()
        cursor = conn.cursor()

        cursor.executemany(insert_post_list_sql, post_list)
        list_length = len(post_list)
        print("게시글 리스트 추가 완료")
        logging.debug(f"게시글 {list_length}개 추가")

        conn.commit()
        conn.close()

    def save_reply(self) -> None:
        start = time.time()
        df = pd.DataFrame()
        for row in self.reply_list:
            tmp = pd.DataFrame(row, columns=["site", "num", "reply"])
            df = df.append(tmp)
        df.to_parquet("dc_realtime.parquet", engine="pyarrow", compression="gzip")
        end = time.time()
        print("댓글 전체 추가 시간: ", end - start)
        row_length = len(df)
        print(f"댓글 {row_length}개 추가 완료")
        logging.debug(f"댓글 {row_length}개 추가")


if __name__ == "__main__":
    Crawling()
