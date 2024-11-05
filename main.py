from bs4 import BeautifulSoup
import sqlite3
import requests
from urllib.parse import urljoin
import re
from pymorphy2 import MorphAnalyzer

class Crawler:

    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        self.dbFileName = dbFileName
        self.conn = sqlite3.connect(self.dbFileName)

    # 0. Деструктор
    def __del__(self):
        print("Fin")
        self.conn.close()

    # 1. Индексирование одной страницы
    def addToIndex(self, soup, url):
        listUnwantedItems = ['script', 'style']
        for script in soup.find_all(listUnwantedItems):
            script.decompose()
        text = soup.get_text()
        words = text.split()

        # Убираем знаки препинания, переводим в нижний регистр и удаляем пустые элементы
        # Можно потом сделать с помощью библиотеки pymorphy2(?)
        words = [re.sub(r'[^\w\s]', '', word) for word in words]
        words = [item.lower() for item in words if item != '']

        curs = self.conn.cursor()

        # Находим rowid для ссылки
        curs.execute("""SELECT rowid from URLList WHERE URL = (?);""", (url,))
        link_rowid = curs.fetchone()
        location = 0

        morph = MorphAnalyzer()

        # Делаем запрос в базу данных и проверяем наличие слова
        for word in words:
            word = morph.normal_forms(word)[0]

            # Собираем записанные в таблицу wordlist из бд слова в results
            curs.execute("""SELECT word FROM wordList;""")
            results = [word_[0] for word_ in curs.fetchall()]

            if not word in results:
                curs.execute("""INSERT INTO wordlist VALUES (?, ?, ?);""", (None, word, 0))
            
            # Ищем данное слово в wordlist, чтобы занести в wordlocation
            curs.execute("""SELECT rowid FROM wordList WHERE word = (?);""", (word,))
            word_rowid = curs.fetchone()
            curs.execute("""INSERT INTO wordLocation VALUES (?, ?, ?, ?);""", (None, word_rowid[0], link_rowid[0], location))
            location += 1

        self.conn.commit()

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        return False
 
    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo, linkText):
        pass


    def crawl(self, urlList, maxDepth = 1):
        curs = self.conn.cursor()
        curs.execute("""DROP TABLE IF EXISTS wordlist;""")
        curs.execute("""DROP TABLE IF EXISTS URLList;""")
        curs.execute("""DROP TABLE IF EXISTS wordLocation;""")
        curs.execute("""DROP TABLE IF EXISTS linkBetweenURL;""")
        curs.execute("""DROP TABLE IF EXISTS linkWord;""")

        curs.execute("""CREATE TABLE IF NOT EXISTS wordlist  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    word TEXT NOT NULL,
                                    isFiltred INTEGER NOT NULL
                ); """
            )
        curs.execute("""CREATE TABLE IF NOT EXISTS URLList  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    URL TEXT NOT NULL
                ); """
            )
        curs.execute("""CREATE TABLE IF NOT EXISTS wordLocation  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    fk_wordId INTEGER NOT NULL,
                                    fk_URLId INTEGER NOT NULL,
                                    location INTEGER NOT NULL,
                                    FOREIGN KEY (fk_wordId) REFERENCES wordList(rowId),
                                    FOREIGN KEY (fk_URLId) REFERENCES URLList(rowId)
                ); """
            )
        curs.execute("""CREATE TABLE IF NOT EXISTS linkBetweenURL  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    fk_FromURL_Id INTEGER NOT NULL,
                                    fk_ToURLId INTEGER NOT NULL,
                                    FOREIGN KEY (fk_FromURL_Id) REFERENCES URLList(rowId),
                                    FOREIGN KEY (fk_ToURLId) REFERENCES URLList(rowId)
                ); """
            )
        curs.execute("""CREATE TABLE IF NOT EXISTS linkWord  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    fk_wordId INTEGER NOT NULL,
                                    fk_linkId INTEGER NOT NULL,
                                    FOREIGN KEY (fk_wordId) REFERENCES wordList(rowId),
                                    FOREIGN KEY (fk_linkId) REFERENCES linkBetweenURL(rowId)
                ); """
            )
        
        # Нельзя: изменять значения в списке python
        # Можно: использовать генераторы списков (list comprehension) и перезаписать изначальный список
        urlList = [url_.rstrip('/') for url_ in urlList]


        # Вставка первых двух ссылок
        for url_ in urlList:
            curs.execute("""INSERT INTO URLList (URL) VALUES (?);""", (url_,))
        
        counter = 1

        for _ in range(0, maxDepth):
            new_URLs = []
            for url_ in urlList:
                curs.execute("""SELECT rowid FROM URLList WHERE URL = (?);""", (url_,))
                fk_FromURL = curs.fetchone()

                # Счётчик для контроля
                print("parse", counter,") ", url_)
                counter += 1
                if counter == 10: exit()
                ######################

                html_doc = requests.get(url_).text
                soup = BeautifulSoup(html_doc, "html.parser")
            
                for link in soup.find_all('a'):
                    new_link = link.get('href')
                    if type(new_link) is str:
                        if not "#" in new_link:
                            new_link = new_link.rstrip("/")
                            if new_link.startswith('/'):
                                new_link = urljoin(url_, new_link)
                            if new_link.startswith('http://') or new_link.startswith('https://'):

                                curs.execute("""SELECT rowid FROM URLList WHERE URL = (?);""", (new_link,))
                                res_of_search = curs.fetchone()

                                # Проверяем наличие ссылки в базе - если есть, идём к следующей
                                if not res_of_search is None: continue

                                # Вставляем новую ссылку в таблицу URLList
                                curs.execute("""INSERT INTO URLList VALUES (?, ?);""", (None, new_link))
                                # Смотрим положение этой ссылки
                                curs.execute("""SELECT rowid FROM URLList WHERE URL = (?)""", (new_link,))
                                fk_ToURL = curs.fetchone()
                                # Вставляем эту ссылку в таблицу linkBetweenURL
                                curs.execute("""INSERT INTO linkBetweenURL VALUES (?, ?, ?);""", (None, fk_FromURL[0], fk_ToURL[0]))

                                new_URLs.append(new_link)
                                # print(new_link)
                self.addToIndex(soup, url_)

            self.conn.commit()
            urlList = new_URLs


if __name__ == '__main__':
    crawler = Crawler('DB.db')
    links = ['https://history.eco']
    # links = ['https://history.eco/', 'https://elementy.ru/']

    crawler.crawl(links, 2)