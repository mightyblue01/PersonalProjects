import os
import cx_Oracle
import csv
import datetime
import sys

PYTHONIOENCODING="UTF-8"
PYTHONIONENCODING="UTF-8"

class OracleConnector():
    connection = None
    tempFile = '/tmp/temp-file.csv'

    def __init__(self, connection_string, temp_file):
        self.connection = cx_Oracle.connect(connection_string, encoding='UTF-8', nencoding='UTF-8')

        print("initializing connection")
        print(self.connection)
        cursor = self.connection.cursor()
        cursor.execute("SELECT 'Hello World!' FROM dual")
        res = cursor.fetchall()
        cursor.close()
        print(res)

    def records(self, path):
        with open(path, delimiter='|') as f:
            contents = f.read()
            return (record for record in contents.split('|~!'))

    def handleLineTerminator(self, fileName):
        with open(fileName, "r") as myfile:
            data = myfile.read().replace('|~!', '\r')

        with open(self.tempFile, 'w') as filehandle:
            filehandle.write(data)

    def handleLineTerminator_wip(self, fileName):
        # f= open(fileName,mode='r',encoding='utf8', newline='\~!')
        # csv.reader(self.records(fileName))
        # tempFile='/tmp/temp-file.csv'
        # tempFile='/dbfs/mnt/input/con4.csv'
        with open(fileName, "r") as myfile:
            data = myfile.read().replace('|~!', '\r')

        return data

        # return iter(data.split('\r'))

    def splitList(self, l, n):
        n = max(1, n)
        return [l[i:i + n] for i in range(0, len(l), n)]

    def pushBulkDataToDB(self, inputFileName, dbTableName):
        print("push bulk data")
        # if csvfile.endswith(".csv") or csvfile.endswith(".CSV"):
        cursor = self.connection.cursor()
        print(dbTableName)
        # handle line delimiter

        # changed#reader = csv.reader(open(inputFileName), delimiter=',')
        # reader = csv.reader(open(inputFileName,encoding='utf-8'), delimiter='|',quotechar='"',lineterminator='\r',escapechar='\\')
        # -- reader = csv.reader(open(inputFileName,encoding='utf-8'), delimiter='|',quotechar='"',lineterminator='\|~!',escapechar='\\')

        # csv.register_dialect('myD', delimiter = '|', lineterminator = '|~!')
        self.handleLineTerminator(inputFileName)
        reader = csv.reader(open(self.tempFile, encoding='utf-8'), delimiter='|', quotechar='"', lineterminator='\|\r',
                            escapechar='\\')
        # reader = csv.reader(returnData.splitlines(),delimiter='|',quotechar='"',lineterminator='\|\r',escapechar='\\')
        # reader = csv.reader(returnData,delimiter='|',lineterminator='\|\r')

        recordsList = []
        column_list = ''
        value_list = ''
        first_row = next(reader)
        print(first_row)
        # column_string = ','.join(first_row).translate('"')
        column_string = ','.join(first_row).translate('"')
        print(column_string)
        # insert_string='insert into ' + schema + '.' + dbTableName + ' (' + column_string + ') values ('
        insert_string = 'insert into ' + dbTableName + ' (' + column_string + ') values ('
        val_list = []
        for i in range(1, len(first_row) + 1):
            val_list.append(':' + str(i))
        value_string = ','.join(val_list)
        # value_string='|'.join(val_list)
        insert_string += value_string + ')'
        # -- print(insert_string)
        for row in reader:
            for index, col in enumerate(row):
                # print("enum ...")
                col_tr = col
                print(col_tr)
                # if col_tr:
                #  if col_tr[0] != '"' :
                #    try:
                #      col_tr=datetime.datetime.strptime(col_tr,'%d-%b-%y')
                #    except ValueError:
                #      continue
                row[index] = col_tr
                # print("adding index %s"%index)
            recordsList.append(row)
        cursor.prepare(insert_string)
        print(len(recordsList))

        #### split in chunks ####
        resizedList = self.splitList(recordsList, 50000)
        for i in range(len(resizedList)):
            cursor.executemany(None, resizedList[i])
            print('Inserted: ' + str(cursor.rowcount) + ' rows.')
        #########################

        print('Inserted: ' + str(cursor.rowcount) + ' rows.')
        self.connection.commit()

        os.remove(self.tempFile)
        cursor.close()

    def executeQuery(self, queryString):
        print("execute query %s" % queryString)
        cursor = self.connection.cursor()
        cursor.execute(queryString)
        res = cursor.fetchall()
        self.connection.commit()
        print(res)
        cursor.close()

    def closeConnection(self):
        print("Closing the connection")
        self.connection.close()

#Now test
print("TEst")
dbStagingTable = "bw47"
connectionString = "user/password@//x.x.x.x:1521/XE"

inFileName = "/dbfs/mnt/input/input_31.csv"

outFileName = "/dbfs/mnt/input/outfile"
stagingConnection = OracleConnector(connectionString, None)


stagingConnection.pushBulkDataToDB(inFileName, dbStagingTable)

stagingConnection.closeConnection()
