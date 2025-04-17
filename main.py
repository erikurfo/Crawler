from bs4 import BeautifulSoup
import sqlite3
import re
import requests
from urllib.parse import urljoin, urlparse, urlunparse
import pandas as pd
import matplotlib.pyplot as plt
from requests.exceptions import ReadTimeout
import pymorphy2

class Crawler:

    def __init__(self, dbFileName):
        self.morph = pymorphy2.MorphAnalyzer()
        self.dbFileName = dbFileName
        self.conn = sqlite3.connect(self.dbFileName)
        self.initDB()
        self.df = pd.DataFrame(columns = ['Number of indexed links',
                                          'URLlist rows',
                                          'wordList rows',
                                          'wordLocation rows',
                                          'linkBetweenURL rows',
                                          'linkWord rows'])

    def __del__(self):
        self.conn.close()
        print('final')

    def addToIndex(self, soup, url):

        link_rowid = self.getEntryId('URLList', 'URL', url)

        listOfUnwantedItems = ['script', 'style']
        for script in soup.find_all(listOfUnwantedItems):
            script.decompose()
        text = soup.get_text()

        words = self.separateWords(text)

        # индексация всего текста
        self.indexingWords(words, link_rowid)

        # работа со ссылками на странице
        filtered_links = self.indexingLinks(soup, link_rowid, url)
            
        self.conn.commit()
        return filtered_links
    
    def indexingWords(self, words, source_link_rowid):
        cursor = self.conn.cursor()
        word_location = 0
        for word in words:
            if self.isFiltered(word):
                continue  # пропускаем отфильтрованные слова

            word_rowid = self.getEntryId('wordList', 'word', word)
            if not word_rowid:
                cursor.execute('INSERT INTO wordList VALUES (?, ?)', (None, word))
                word_rowid = cursor.lastrowid

            cursor.execute('INSERT INTO wordLocation VALUES (?, ?, ?, ?)', 
                        (None, word_rowid, source_link_rowid, word_location))
            word_location += 1
    
    def indexingLinks(self, soup, source_link_rowid, url):
        links_from_soup = soup.find_all('a')
        filtered_links = []

        for every_link in links_from_soup:
            filtered_link = self.filteredLink(every_link, url) 
            if filtered_link:
                # Если ссылки нет в базе - добавляем
                if not self.isAdded(filtered_link):
                    self.insertLink(filtered_link)

                filtered_links.append(filtered_link)
                _a_tag_text = every_link.get_text().strip()

                # Получаем идентификатор ссылки и записываем в linkBetweenURL
                filtered_link_fk = self.getEntryId('URLList', 'URL', filtered_link)
                self.addLinkRef(source_link_rowid, filtered_link_fk, _a_tag_text)
        return filtered_links

    def separateWords(self, text):
        words = text.split()
        words = [re.sub(r'[^\w\s]', '', word) for word in words]
        words = [item.lower() for item in words if item != '']
        # Лемматизация
        words = [self.morph.parse(word)[0].normal_form for word in words]
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
                                    word TEXT NOT NULL
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
    def isAdded(self, url):
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
            
    def filteredLink(self, link, sourceURL):
        new_link = link.get('href')
        if not new_link: return
        new_link = self.normalizeURL(new_link, sourceURL)
        if (not new_link) or ('#' in new_link): return
        self.conn.commit()
        return new_link

    def normalizeURL(self, url, sourceURL):
        url = url.rstrip('/')
        parsed = urlparse(url)
        if url.startswith('/'): 
            url = urljoin(sourceURL, url)
        if not parsed.scheme or not parsed.netloc: return
        netloc = parsed.netloc.lstrip('www.').lower()
        scheme = parsed.scheme.lower()
        normalized = parsed._replace(scheme=scheme, netloc=netloc)
        return urlunparse(normalized)  
    
    def insertLink(self, link_):
        cursor = self.conn.cursor()
        if not self.isAdded(link_):
            cursor.execute('INSERT INTO URLList VALUES (?, ?);', (None, link_))
    
    def isFiltered(self, word):
        if any(char.isdigit() for char in word):
            return True
        if len(word) < 3:
            return True

        parsed = self.morph.parse(word)[0]
        bad_pos = {'PREP', 'CONJ', 'PRCL', 'INTJ'}
        return parsed.tag.POS in bad_pos

    def monitoring(self):
        list_of_tables = ['URLList', 'wordList', 'wordLocation', 'linkBetweenURL', 'linkWord']
        each_rows = []
        for every_table in list_of_tables:
            cursor = self.conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM {every_table};')
            result = cursor.fetchone()[0]
            print("There are\u001b[31m", result, "\u001b[0mrows in", every_table)
            each_rows.append(result)
        return each_rows

    # Непосредственно сам метод сбора данных
    def crawl(self, urlList, maxDepth = 1):

        urlList = [url_.rstrip('/') for url_ in urlList]

        for start_urls in urlList:
            if self.normalizeURL(start_urls, start_urls):
                self.insertLink(start_urls)
        
        number_of_indexed_link = 0
        self.df.loc[0] = [0, 0, 0, 0, 0, 0]
        new_links = []
        for _ in range(0, maxDepth):
            for url_ in urlList: 
                print('indexing\033[32m', url_, '\033[0m')
                try:
                    # timeout в секундах
                    html_doc = requests.get(url_, timeout = 5)
                    html_doc.encoding = 'utf-8'
                    soup = BeautifulSoup(html_doc.text, 'html.parser')

                    # Индексация и получение списка чистых ссылок со страниц
                    new_links += self.addToIndex(soup, url_)
                    number_of_indexed_link += 1
                    self.df.loc[number_of_indexed_link] = [number_of_indexed_link] + self.monitoring()
                    
                except ReadTimeout:
                    print(f"\033[31mTimeout exceeded for {url_}, skipping...\033[0m")
                    continue  # Переходим к следующему URL
                except Exception as e:
                    print(f"\033[31mError processing {url_}: {str(e)}, skipping...\033[0m")
                    continue  # Переходим к следующему URL

            urlList = [element for element in new_links]
        self.conn.commit()
        self.graphs()

    # Построение графиков
    def graphs(self):
        self.df.plot(x = 'Number of indexed links', y = 'URLlist rows',
                     title = 'Число строк в таблице URLlist', grid = True, xlim=(0, None), ylim=(0, None))
        plt.savefig('graph1.png')
        plt.close()

        self.df.plot(x = 'Number of indexed links', y = 'wordList rows',
                     title = 'Число строк в таблице wordList', grid = True, xlim=(0, None), ylim=(0, None))
        plt.savefig('graph2.png')
        plt.close()

        self.df.plot(x = 'Number of indexed links', y = 'wordLocation rows',
                     title = 'Число строк в таблице wordLocation', grid = True, xlim=(0, None), ylim=(0, None))
        plt.savefig('graph3.png')
        plt.close()

        self.df.plot(x = 'Number of indexed links', y = 'linkBetweenURL rows',
                     title = 'Число строк в таблице linkBetweenURL', grid = True, xlim=(0, None), ylim=(0, None))
        plt.savefig('graph4.png')
        plt.close()

        self.df.plot(x = 'Number of indexed links', y = 'linkWord rows',
                     title = 'Число строк в таблице linkWord', grid = True, xlim=(0, None), ylim=(0, None))
        plt.savefig('graph5.png')
        plt.close()
        

if __name__ == '__main__':

    crawler = Crawler('DB.db')

    links = ['https://www.gazeta.ru/', 'https://ria.ru']

    crawler.crawl(links, 2)