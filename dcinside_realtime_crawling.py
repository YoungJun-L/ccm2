from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from pymysql import connect

from itertools import zip_longest
from multiprocessing import Pool
import time


class Crawling:
    def __init__(self):
        start = time.time()
        self.url_num_tuple_list = []
        self.get_post_list()

        pool = Pool(processes=8)
        pool.map(self.get_content, self.url_num_tuple_list)
        pool.close()
        pool.join()

        end = time.time()
        print("********************************************")
        print("********************************************")
        print("전체 수행 시간: ", end - start)
        print("********************************************")
        print("********************************************")

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
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(5)
        return driver

    def get_post_list(self) -> None:
        base_url = "https://gall.dcinside.com/board/lists/?id=dcbest&list_num=100&sort_type=N&exception_mode=recommend&search_head=&page="
        post_list = []
        startPage = 0
        while True:
            cnt = 0
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
                timeStandard = i.find("td", "gall_date").text

                if timeStandard.find(".") != -1 or cnt == 1:
                    print(timeStandard.find("."))
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
                cnt += 1

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
                        if text[-9:] == " - dc App":
                            text = text[:-9]
                        reply_list.append(text)
                    btn_element = driver.find_element_by_xpath(btn_xpath + f"[{i}]")
                    driver.execute_script("arguments[0].click();", btn_element)
                    i += 1
                except Exception:
                    break

            reply_list = set(reply_list)
        except TimeoutException:
            print("********************************************")
            print("Time Out")
            print("********************************************")
            pass

        print()
        print("********************************************")
        print("긁어온 게시글: ", url)
        print("긁어온 댓글 수: ", len(reply_list))
        print("********************************************")
        print()

        self.insert_len_reply(len(content[0]), url, reply_list, num)

    def insert_len_reply(self, len, url, reply_list, num) -> None:
        update_len_sql = "UPDATE post_table SET len = %s WHERE url = %s"
        insert_reply_sql = "INSERT IGNORE INTO reply_table (site, num, reply, reply_hash) VALUES ('실베', %s, %s, UNHEX(MD5(%s)))"

        conn = self.connect_to_db()
        cursor = conn.cursor()

        start = time.time()
        cursor.execute(update_len_sql, (len, url))
        end = time.time()
        print("본문 길이 업데이트 쿼리 시간: ", end - start)

        start = time.time()
        cursor.executemany(
            insert_reply_sql,
            zip_longest([], reply_list, reply_list, fillvalue=num),
        )
        end = time.time()
        print("댓글 추가 쿼리 시간: ", end - start)

        conn.commit()
        conn.close()

    def insert_post_list(self, post_list) -> None:
        insert_post_list_sql = "INSERT INTO post_table (site, num, url, title, replyNum, viewNum, voteNum, timeUpload) VALUES ('실베', %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE url = %s, title = %s, replyNum = %s, viewNum = %s, voteNum = %s, timeUpload = %s"

        conn = self.connect_to_db()
        cursor = conn.cursor()

        start = time.time()
        cursor.executemany(insert_post_list_sql, post_list)
        end = time.time()
        print("게시글 리스트 추가 쿼리 시간: ", end - start)

        conn.commit()
        conn.close()


if __name__ == "__main__":
    Crawling()
