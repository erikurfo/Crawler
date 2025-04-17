import sqlite3
import pymorphy2

class Searcher:
    def __init__(self, dbFileName):
        self.con = sqlite3.connect(dbFileName)
        self.morph = pymorphy2.MorphAnalyzer()

    def __del__(self):
        self.con.close()

    def lemmatize(self, word):
        return self.morph.parse(word)[0].normal_form

    def getWordsIds(self, queryString):
        queryWords = queryString.lower().split(" ")
        lemmas = [self.lemmatize(word) for word in queryWords]
        rowidList = []
        for lemma in lemmas:
            res = self.con.execute("SELECT rowid FROM wordList WHERE word=?", (lemma,)).fetchone()
            if res:
                rowidList.append(res[0])
            else:
                raise Exception(f"Слово '{lemma}' не найдено в индексе.")
        return rowidList

    def getMatchRows(self, queryString):
        wordsList = queryString.lower().split(" ")
        lemmas = [self.lemmatize(w) for w in wordsList]
        wordIds = self.getWordsIds(queryString)

        fieldList = ["w0.fk_URLId"]
        tableList = ["wordLocation w0"]
        clauseList = [f"w0.fk_wordId={wordIds[0]}"]
        for i in range(1, len(lemmas)):
            tableList.append(f"wordLocation w{i}")
            clauseList.append(f"w{i}.fk_wordId={wordIds[i]}")
            clauseList.append(f"w0.fk_URLId=w{i}.fk_URLId")
            fieldList.append(f"w{i}.location")

        sql = f"SELECT {', '.join(fieldList)} FROM {' , '.join(tableList)} WHERE {' AND '.join(clauseList)}"
        cur = self.con.execute(sql)
        return [row for row in cur], wordIds

    def normalizeScores(self, scores, smallIsBetter=False):
        if not scores:
            return {}

        vsmall = 0.00001
        minscore = min(scores.values())
        maxscore = max(scores.values())
        range_ = maxscore - minscore if maxscore != minscore else 1.0
        result = {}
        for key, value in scores.items():
            if smallIsBetter:
                result[key] = float(minscore) / max(vsmall, value)
            else:
                result[key] = (value - minscore) / range_
        return result

    def frequencyScore(self, rowsLoc):
        if not rowsLoc:
            return {}

        counts = {}
        for row in rowsLoc:
            counts[row[0]] = counts.get(row[0], 0) + 1
        return self.normalizeScores(counts, smallIsBetter=False)

    def calculatePageRank(self, iterations=20):
        self.con.execute("DROP TABLE IF EXISTS pagerank")
        self.con.execute("CREATE TABLE pagerank (urlid INTEGER PRIMARY KEY, score REAL)")
        self.con.execute("INSERT INTO pagerank SELECT rowid, 1.0 FROM URLList")
        self.con.commit()

        for i in range(iterations):
            print(f"PageRank итерация {i+1}")
            for (urlid,) in self.con.execute("SELECT rowid FROM URLList"):
                pr = 0.15
                for (linker,) in self.con.execute("SELECT fk_FromURL_Id FROM linkBetweenURL WHERE fk_ToURLId=?", (urlid,)):
                    linkingpr = self.con.execute("SELECT score FROM pagerank WHERE urlid=?", (linker,)).fetchone()[0]
                    linkingcount = self.con.execute("SELECT COUNT(*) FROM linkBetweenURL WHERE fk_FromURL_Id=?", (linker,)).fetchone()[0]
                    if linkingcount != 0:
                        pr += 0.85 * (linkingpr / linkingcount)
                self.con.execute("UPDATE pagerank SET score=? WHERE urlid=?", (pr, urlid))
            self.con.commit()

    def pagerankScore(self, rowsLoc):
        if not rowsLoc:
            return {}

        scores = {}
        for row in rowsLoc:
            pr = self.con.execute("SELECT score FROM pagerank WHERE urlid=?", (row[0],)).fetchone()
            if pr:
                scores[row[0]] = pr[0]
        return self.normalizeScores(scores, smallIsBetter=False)

    def geturlname(self, urlid):
        res = self.con.execute("SELECT URL FROM URLList WHERE rowid=?", (urlid,)).fetchone()
        return res[0] if res else ""

    def getSortedList(self, queryString):
        try:
            rowsLoc, _ = self.getMatchRows(queryString)
        except Exception as e:
            print("Ошибка:", e)
            return

        if not rowsLoc:
            print("По запросу ничего не найдено.")
            return

        m1 = self.frequencyScore(rowsLoc)
        m2 = self.pagerankScore(rowsLoc)

        scores = {}
        for urlid in m1:
            scores[urlid] = (m1.get(urlid, 0) + m2.get(urlid, 0)) / 2.0

        sortedScores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        for urlid, score in sortedScores[:10]:
            print(f"{score:.3f} | {urlid} | {self.geturlname(urlid)}")


if __name__ == "__main__":
    db_filename = "DB.db"

    searcher = Searcher(db_filename)

    # Вычисляем PageRank
    # searcher.calculatePageRank()

    query = input("Введите поисковый запрос (через пробел): ")
    print("\nРезультаты ранжирования по запросу:", query)
    print("="*50)
    searcher.getSortedList(query)
    print("="*50)