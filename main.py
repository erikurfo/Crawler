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
        
        for currDepth in range(0, maxDepth):
            new_URLs = []
            for url_ in urlList:
                curs.execute("""INSERT INTO URLList (rowid, URL) VALUES (?, ?);""", (None, url_))
                html_doc = requests.get(url_).text
                soup = BeautifulSoup(html_doc, "html.parser")
            
                for link in soup.find_all('a'):
                    new_link = link.get('href')
                    if type(new_link) is str:
                        if not "#" in new_link:
                            if new_link.startswith('http://') or new_link.startswith('https://'):
                                new_URLs.append(new_link)
                                # print(new_link)
                            elif new_link.startswith('/'):
                                new_link = urljoin(url_, new_link)
                                new_URLs.append(new_link)
                                # print(new_link)
                self.addToIndex(soup, url_)
            self.conn.commit()    


if __name__ == '__main__':
    crawler = Crawler('DB.db')
    links = ['https://www.tadviser.ru/']
    # links = ['https://history.eco/']
    # links = ['https://history.eco/', 'https://elementy.ru/']

    crawler.crawl(links, 1)