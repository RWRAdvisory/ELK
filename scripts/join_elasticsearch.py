import csv
import re
from pymongo import MongoClient
from unidecode import unidecode
from elasticsearch import Elasticsearch
from elasticsearch_dsl.query import MultiMatch, Match
from elasticsearch_dsl import MultiSearch, Search
from pymongo.operations import UpdateOne
from collections import defaultdict
import json


client = Elasticsearch('192.168.1.132', timeout=30,  max_retries=10, retry_on_timeout=True)

sample_title = "Russian Helicopters made first contract for civil products supply to Pakistan"

queries_all = []
def multiSearch(ms, sample_title, date, secondary_phrase, transaction):
    ms = ms.add(getCitation(sample_title, date, secondary_phrase))
    return ms

def getCitation(title, date, secondary_phrase):
    #Basic Search Interface
    s = Search()
    if not title: title = 'EMPTYSEARCHQUERYPLACEHOLDER'
    q = MultiMatch(query=title, type="phrase", fields=['header'])
    q = Match(header={'query': '*' + title + '*', "type": "phrase", 'slop': 1.5})
    s = s.query(q)
    if date:
        q3 = MultiMatch(query=date, fields=['sug_pub_date', 'LOAD-DATE', 'header'])
        s = s.query(q3)
    q4 = Match(header={'query': sample_title})
    queries_all.append(s)
    #q2 = Match(text={"query": secondary_phrase})
    #.highlight('text', fragment_size=500)
    return s

match_cite = re.compile(r"\"?(.*)\"(.*?\.)?(.*?\.)?(.*?\.)")
match_title = re.compile(r"\"?(.*)\"")
match_date = re.compile(r".*?([A-z]+ [0-9]{1,2}, [0-9]{4}).*?")

def parseResults(ms, x):
    backfill = ms.execute()
    print(len(backfill), len(x))
    for i, response in enumerate(backfill):
        if response:
            text_file = open("Output.html", "a")
            for hit in response:
                if hit:
                    print(i, len(x), hit)
                    print_str = '<h1>{trans_title}</h1><br /><h2>{title}</h2><br /><br />'.format(title=hit.sug_title, trans_title=x[i]['Title'])
                    #for fragment in hit.meta.highlight.text:
                    #    print_str = print_str + '<br /><br /><hr><p>{fragment}'.format(fragment=fragment.replace('<em>', '<b>').replace('</em>', '</b>'))
                    text_file.write(print_str)
            text_file.close()

all_fingerprints = []
def parseResult(msear, transaction_array):
    transaction_dict = defaultdict(lambda: [])
    trans_id_lookup = {}
    backfill = msear.execute()
    queries = msear.to_dict()
    for i, response in enumerate(backfill):
        curr_transaction = transaction_array[i]
        citation_array = [hit.fingerprint for hit in response] if response else None
        transaction_dict[curr_transaction['Id']].append(citation_array)
        trans_id_lookup[curr_transaction['Id']] = curr_transaction
    for trans_id, citations in transaction_dict.items():
        trans_id_lookup[trans_id]['ES_Fingerprints'] = citations
        all_fingerprints.extend([citation for citation_array in citations if citation_array for citation in citation_array])
    # Find all transactions which don't have any valid documents in the search results.
    missing_transactions = [transaction for transaction, citations in transaction_dict.items() if len([citation for citation in citations if citation]) == 0]
    #print([trans_id_lookup[transaction_id]['Citations'].split("|") for transaction_id in missing_transactions])
    print("Parsed %i transactions. %i don't have results" % (len(set([x['Id'] for x in transaction_array])), len(missing_transactions)))
    return trans_id_lookup.values()


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def initDb():
    print("Initializing Database.")
    mongocl = MongoClient('localhost', 27017)
    db = mongocl['rwr_extractor']
    return (client, db)

mongocl, db  = initDb()
transactions = [transaction for transaction in db['transactions'].find()]
for x in batch(transactions, 1000):
    curr_transactions = []
    ms = MultiSearch(index='backfill').using(client)
    for row in x:
        citations = row['Citations']
        for cite in citations:
            clean_cite = unidecode(cite).strip()
            m_title = match_title.match(clean_cite)
            m_date = match_date.match(clean_cite)
            matches = match_cite.match(clean_cite)
            title = m_title.group(1).rstrip('.') if m_title else None
            date = m_date.group(1).rstrip('.') if m_date else None
            #    title = matches.group(1).rstrip('.')
            #    date = matches.group(4).rstrip('.')
                #if m_date and date.strip() != m_date.group(1).strip():
                #    print(m_date.group(1), date)
            #    if not m_title:
            #        print(clean_cite)
            ms = multiSearch(ms, title, date, 'india', row)
            curr_transactions.append(row)
    matched_transactions_raw = parseResult(ms, curr_transactions)
    #matchedTransactions = db['transactions_matched'].insert_many(matched_transactions_raw)
with open('./fingerprints.json', 'w') as fp:
    print(len(all_fingerprints))
    json.dump(all_fingerprints, fp)