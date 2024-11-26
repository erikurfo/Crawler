from bs4 import BeautifulSoup
import sqlite3
import re
import requests
from urllib.parse import urljoin

class Crawler:

    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        self.dbFileName = dbFileName
        self.conn = sqlite3.connect(self.dbFileName)
        self.initDB()

    # 0. Деструктор
    def __del__(self):
        self.conn.close()
        print('fin')

    # Индексирование одной страницы
    def addToIndex(self, soup, url):
        cursor = self.conn.cursor()
        link_rowid = self.getEntryId('URLList', 'URL', url)

        listOfUnwantedItems = ['script', 'style']
        for script in soup.find_all(listOfUnwantedItems):
            script.decompose()
        text = soup.get_text()

        words = self.separateWords(text)

        word_location = 0
        for word in words:
            word_rowid = self.getEntryId('wordList', 'word', word)
            if not word_rowid: 
                # Реализовать isFiltered для третьей колонки
                cursor.execute('INSERT INTO wordList VALUES (?, ?, ?)', (None, word, 0))
                word_rowid = cursor.lastrowid
            cursor.execute('INSERT INTO wordLocation VALUES (?, ?, ?, ?)', 
                           (None, word_rowid, link_rowid, word_location))
            word_location += 1
        self.conn.commit()


    # Разбиение текста на слова
    # Функция возвращает список слов
    def separateWords(self, text):
        words = text.split()
        # Убираем знаки препинания, 
        # переводим в нижний регистр и удаляем пустые элементы
        words = [re.sub(r'[^\w\s]', '', word) for word in words]
        words = [item.lower() for item in words if item != '']
        return words
 
    # Инициализация таблиц в БД
    def initDB(self):
        cursor = self.conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS wordlist;')
        cursor.execute('DROP TABLE IF EXISTS URLList;')
        cursor.execute('DROP TABLE IF EXISTS wordLocation;')
        cursor.execute('DROP TABLE IF EXISTS linkBetweenURL;')
        cursor.execute('DROP TABLE IF EXISTS linkWord;')

        cursor.execute('''CREATE TABLE IF NOT EXISTS wordList  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    word TEXT NOT NULL,
                                    isFiltred INTEGER NOT NULL
                ); '''
            )
        cursor.execute('''CREATE TABLE IF NOT EXISTS URLList  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    URL TEXT NOT NULL
                ); '''
            )
        cursor.execute('''CREATE TABLE IF NOT EXISTS wordLocation  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    fk_wordId INTEGER NOT NULL,
                                    fk_URLId INTEGER NOT NULL,
                                    location INTEGER NOT NULL,
                                    FOREIGN KEY (fk_wordId) REFERENCES wordList(rowId),
                                    FOREIGN KEY (fk_URLId) REFERENCES URLList(rowId)
                ); '''
            )
        cursor.execute('''CREATE TABLE IF NOT EXISTS linkBetweenURL  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    fk_FromURL_Id INTEGER NOT NULL,
                                    fk_ToURLId INTEGER NOT NULL,
                                    FOREIGN KEY (fk_FromURL_Id) REFERENCES URLList(rowId),
                                    FOREIGN KEY (fk_ToURLId) REFERENCES URLList(rowId)
                ); '''
            )
        cursor.execute('''CREATE TABLE IF NOT EXISTS linkWord  (
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    fk_wordId INTEGER NOT NULL,
                                    fk_linkId INTEGER NOT NULL,
                                    FOREIGN KEY (fk_wordId) REFERENCES wordList(rowId),
                                    FOREIGN KEY (fk_linkId) REFERENCES linkBetweenURL(rowId)
                ); '''
            )

    # Вспомогательная функция для получения идентификатора записи
    def getEntryId(self, tableName, fieldName, value):
        cursor = self.conn.cursor()
        cursor.execute('SELECT rowid FROM ' + tableName + 
                       ' WHERE ' + fieldName + 
                       ' = (?);', (value,))
        row_id = cursor.fetchone()
        return None if row_id is None else row_id[0]

    # Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        result = self.getEntryId('URLList', 'URL', url)
        return False if result is None else True
 
    # Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo, linkText):
        cursor = self.conn.cursor()
        cursor.execute('INSERT INTO linkBetweenURL VALUES (?, ?, ?);', 
                       (None, urlFrom, urlTo))
        last_entry = cursor.lastrowid
        linkwords_list = self.separateWords(linkText)
        for _word_ in linkwords_list:
            word_rowid = self.getEntryId('wordList', 'word', _word_)
            if word_rowid:
                cursor.execute('INSERT INTO linkWord VALUES (?, ?, ?);',
                               (None, word_rowid, last_entry))
            
    def filteredLinks(self, links_list, sourceURL):
        cursor = self.conn.cursor()
        new_URLs = []
        for link in links_list:
            # текст в ссылке
            _a_tag_text = link.get_text().strip()
            new_link = link.get('href').rstrip('/')
            if '#' in new_link: continue
            if new_link.startswith('/'): 
                new_link = urljoin(sourceURL, new_link)
            if self.isIndexed(new_link): continue

            cursor.execute('INSERT INTO URLList VALUES (?, ?);', (None, new_link))
            self.addLinkRef(sourceURL, new_link, _a_tag_text)
            new_URLs.append(new_link)
        return new_URLs


    # Непосредственно сам метод сбора данных.
    def crawl(self, urlList, maxDepth = 1):
        cursor = self.conn.cursor()

        urlList = [url_.rstrip('/') for url_ in urlList]

        # Вставка первых двух ссылок
        for url_ in urlList:
            cursor.execute('INSERT INTO URLList (URL) VALUES (?);', (url_,))

        for _ in range(0, maxDepth):
            for url_ in urlList:
                print(url_)
                html_doc = requests.get(url_)
                html_doc.encoding = 'utf-8'
                soup = BeautifulSoup(html_doc.text, 'html.parser')

                self.addToIndex(soup, url_)

                all_links = soup.find_all('a')
                urlList = self.filteredLinks(all_links, url_)
                
                self.conn.commit()


if __name__ == '__main__':

    crawler = Crawler('DB.db')
    # links = ['https://history.eco']
    # links = ['https://www.reddit.com/?rdt=35077']
    # links = ['https://history.eco/', 'https://elementy.ru/']

    links = ['http://127.0.0.1:8080/1_leguria.html']

    crawler.crawl(links, 2)