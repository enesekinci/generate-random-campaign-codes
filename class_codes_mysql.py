import datetime
import random
import mysql.connector

startTime = datetime.datetime.now()

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="python_codes"
)


class Codes:

    long = 7
    chars = ['A', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P',
             'R', 'S', 'T', 'U', 'V', 'Y', 'Z', 'X', 'W', '1', '2', '3', '4', '5', '6', '7']
    prefix = 'KL'
    campaignCode = 'A'
    startTime = datetime.datetime.now()
    unit = 1000000
    codes = set()
    bulk_insert = []
    database_codes = set()
    code = ''
    status = 0

    def getCodes(self):

        for x in range(self.unit):

            self.code = self.prefix+self.campaignCode + \
                (self.generateCode())
            self.status = 1 if self.code in self.codes else 0

            while self.status == 1:
                self.code = self.prefix+self.campaignCode+(self.generateCode())
                self.status = 1 if self.code in self.codes else 0

            self.codes.add(self.code)
            self.database_codes.add((self.code, datetime.datetime.now()))

            if (len(self.codes) % 100000 == 0):
                self.bulk_insert.append(self.database_codes)
                self.database_codes = set()

        return [self.codes, self.database_codes, self.bulk_insert]

    def generateCode(self):
        return ''.join(random.sample(self.chars, self.long))


result = Codes().getCodes()
codes = result[0]
database_codes = result[1]
bulk_insert = result[2]

sql = "INSERT INTO codes (code, datetime) VALUES (%s, %s)"
count = 1

for row in bulk_insert:
    cursor = db.cursor()
    cursor.executemany(sql, row)
    db.commit()
    print("insert page:"+str(count))
    count += 1

cursor.close()

finish = datetime.datetime.now()
print(str(finish-startTime))
