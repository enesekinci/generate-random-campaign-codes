import datetime
import random
import pymongo

startTime = datetime.datetime.now()

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["python_codes"]
table = db['codes']


class Codes:

    long = 7
    chars = ['A', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P',
             'R', 'S', 'T', 'U', 'V', 'Y', 'Z', 'X', 'W', '1', '2', '3', '4', '5', '6', '7']
    prefix = 'KL'
    campaignCode = 'A'
    startTime = datetime.datetime.now()
    unit = 2f5000000
    codes = set()
    bulk_insert = []
    database_codes = []
    code = ''
    status = 1

    def getCodes(self):

        for x in range(self.unit):

            self.code = self.prefix+self.campaignCode + \
                (self.generateCode())
            self.status = 1 if self.code in self.codes else 0

            while self.status == 1:
                self.code = self.prefix+self.campaignCode+(self.generateCode())
                self.status = 1 if self.code in self.codes else 0

            self.codes.add(self.code)

            self.database_codes.append(
                {"code": self.code, "datetime": datetime.datetime.now()})

            if (len(self.codes) % 250000 == 0):
                self.bulk_insert.append(self.database_codes)
                self.database_codes = []

        return [self.codes, self.database_codes, self.bulk_insert]

    def generateCode(self):
        return ''.join(random.sample(self.chars, self.long))


result = Codes().getCodes()
codes = result[0]
database_codes = result[1]
bulk_insert = result[2]

count = 1
for row in bulk_insert:
    x = table.insert_many(row)
    print("insert page:"+str(count))
    count += 1

finish = datetime.datetime.now()
print(str(finish-startTime))
