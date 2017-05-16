import json
from pymongo import MongoClient
import os
import re
import unicodecsv as csv
import time
from dateutil import parser
import cgi
import codecs
import pyprind

def initDB():
    print("Initializing Database.")
    global client,db,backfillCollection,transactionCollection
    
    client = MongoClient('localhost',27017)
    db = client['rwr_extractor']
    
    #collections = ['transactions','entities']
    collections = ['transactions']
    
    backfillCollection = db['backfill']
    transactionCollection = db['transactions']
    entityCollection = db['entities']
    
    for collection in [x for x in collections if x in db.collection_names()]:
        if db[collection].count() != 0:
            db[collection].delete_many({})

def processIntelTrakExport(fileName):
    raw_file = open(fileName, 'rb')
    input_file = list(csv.DictReader(raw_file, encoding='utf-8'))
    transactions = [];
    progbar = pyprind.ProgBar(len(input_file), title=str("Processing " + str(fileName) + "."))
    for row in input_file:
        for k,v in row.items():
            if k not in [u"Title",u"Description",u"Citations",u"Date",u"End Date",u"Start Date",u"Created At",u"Updated At"]:
                nested = False
                obj = False
                subvalues = v.split("|")
                if len(subvalues) > 1:
                    cleanVals = []
                    for value in subvalues:
                        if value.find(":") > 0:
                            cleanVals.append(dict({value.split(":")[0]:value.split(":")[1]}))
                        else:
                            cleanVals.append(value)
                    row[k] = cleanVals
                elif v.find(":") > 0:
                    row[k] = dict({v.split(":")[0]:v.split(":")[1]})
                else:
                    #if type(v) is not unicode:
                    #    v = unicode(v, 'utf-8')
                    row[k] = json.dumps(v)
            elif k == "Citations":
                citations = v.split("|")
                row[k] = []
                for cite in citations:
                    row[k].append(cite)
                    #cleanCite = {}
                    #matches = re.findall(r'^(?:\"|\“)(.+?)(?:\"|\”)(\s?.*\s?\.)(\s.*)$', cite.replace("Image: ","").strip())
                    #if len(matches) > 0:
                    #    for match in matches:
                    #        if len(match) == 3:
                    #            cleanCite['title'] = match[-3].strip()
                    #            cleanCite['publication'] = match[-2].strip()
                    #            cleanCite['date'] = match[-1].strip()
                    #    row[k].append(cleanCite)
                    #else:
                    #    row[k].append(cite)
            elif k == 'Description':
                    row[k] = json.dumps(v.replace('\"','').replace("\'",''))
            elif k == 'Website':
                    row[k] = v.replace(":","\:")
            else:
                if k in ["Date","End Date","Start Date","Created At","Updated At"]:
                    if v:
                        row[k] = parser.parse(v).isoformat()
                    else: row[k] = None
                else:
                    row[k] = json.dumps(v)
        transactions.append(row)
        progbar.update()
    return transactions

def importDocs(data, coll):
    coll.insert_many(data)

initDB()
#files = filter(re.compile("^([A-Z])").match,os.listdir("./raw_docx"))
#progbar = pyprind.ProgBar(len(files), title=str("Importing " + str(len(files)) + " documents from ./raw_docx."))
#for idx,doc in enumerate(files):
    #lexis_docs = readDocument("./raw_docx/" + doc)
    #importDocs(lexis_docs,backfillCollection)
    #progbar.update(item_id = files[idx])
transactions = processIntelTrakExport("./Inteltrak Export/transactions.csv")
#entities = processIntelTrakExport("./Inteltrak Export/entities.csv")
with open('./transactions.json', 'w') as fp:
    json.dump(transactions, fp)
#print(transactions, entities)
importDocs(transactions,db['transactions'])
#importDocs(entities, db['entities'])