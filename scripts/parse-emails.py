import logging

import lexisparse
import os, shutil
from tika import parser
import uuid
import json
from imaplib import IMAP4_SSL
import email
import re
import mailbox
from email.parser import Parser
from multiprocessing import Pool

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logger.info("Program started")

'''
Scan the backfill folder for unprocessed documents
'''
def scanFolder(path="./Backfill/"):
    dirs = [file for file in os.listdir(path) if not file[0] in ['~','.']]
    return dirs

'''
Scan the IMAP destination for unprocessed documents
'''

def testEmail():
    for t in [file for file in os.listdir('./Emails-Old/')]:
        with open('./Emails-Old/' + t) as fp:
            print(parseLexis(parser.from_buffer(get_text(Parser().parsestr(fp.read())))['content']))


def processMessage(message_path):
    with open('./Emails/' + message_path) as mp:
        message = email.message_from_file(mp)
        message_content = get_text(message)
        if message_content:
            parsed_message = parser.from_buffer(get_text(message))
            if parsed_message:
                try:
                    lexisDocuments = parseLexis(parsed_message['content'])
                except:
                    logger.debug("Broke Alert: %s" % (message['message-id'].split('SMTP')[0].replace('<', '')))
                    pass
                else:
                    writeResults(lexisDocuments, './Emails/%s' % (message['message-id'].split('SMTP')[0].replace('<', '')), 'Email')
                    moveFolder('./Emails/' + message_path, dest_path='./Processed/Email/%s' % (message['message-id'].split('SMTP')[0].replace('<', '')))


def scanEmailParallel(mbox='./test.mbox'):
    all_messages = scanFolder('./Emails')
    pool = Pool()                         # Create a multiprocessing Pool
    pool.map(processMessage, all_messages)  # proces data_inputs iterable with pool

def scanBackfill(path="./Backfill/"):
    backfill = scanFolder(path)
    for file in backfill:
        print(os.path.join(path, file))
        lexisdocuments = parseFile(path + file)
        if len(lexisdocuments) > 0:
            moveFolder(path + file)
            writeResults(lexisdocuments, path+file, 'Backfill')
            logger.info("Finished processing %s" % path + file)
        else:
            logger.info("Failed to process %s" % path + file)

def fetchEmail():
    with IMAP4_SSL('imap.gmail.com') as M:
        M.login('welp', 'welp')
        M.select()
        typ, data = M.search(None, 'ALL')
        for num in data[0].split():
            typ, data = M.fetch(num, '(RFC822)')
            raw_email = data[0][1]
            email_message = email.message_from_bytes(raw_email)
            write_email(email_message)
            #print('Message %s\n%s\n' % (num, data[0][1]))
        M.close()
        M.logout()

def write_email(email_instance):
    message_id_match = re.compile('<(.*)(?:SMTPIN_ADDED_BROKEN@.*)>')
    message_id = re.match(message_id_match, (email_instance['message-id'])).group(1)
    fp = open('./Emails-Old/{message_id}.email'.format(message_id=message_id.group(1)), 'w')
    fp.write(email_instance.as_string())
    fp.close()
    logger.debug("Parsed Alert: %s" % (message_id.group(1)))

def get_text(msg):
    if msg.is_multipart():
        return get_text(msg.get_payload(0))
    else:
        return msg.get_payload(None, True)

def get_first_text_block(email_message_instance):
    maintype = email_message_instance.get_content_maintype()
    if maintype == 'multipart':
        for part in email_message_instance.get_payload():
            if part.get_content_maintype() == 'text':
                return part.get_payload()
    elif maintype == 'text':
        return email_message_instance.get_payload()

'''
Parse an individual word file
'''
def parseFile(path):
    fileContents = extractText(path)
    if fileContents:
        lexisDocuments = parseLexis(fileContents["content"])
    else: lexisDocuments = []
    logger.debug("Parsed %s and found %s documents." % (path, len(lexisDocuments)))
    return lexisDocuments

'''
Post-process, just move them away to different directory for now
'''
def moveFolder(path, dest_path="./Processed/"):
    shutil.move(path, dest_path)
    logger.debug("%s moved to %s" % (path, dest_path))

'''
Extract text from the word file.
'''
def extractText(path):
    parsed = parser.from_file(path)
    return parsed

def writeResults(lexisDocuments, path, type=None):
    if type:
        dest_path = './Parsed/%s/%s.json' % (type, uuid.uuid1())
    else: dest_path = './Parsed/%s.json' % uuid.uuid1()
    with open(dest_path, 'a') as outfile:
        for document in lexisDocuments:
            document['source_path'] = path
            document['import_type'] = type
            json.dump(document, outfile)
            outfile.write('\n')
    logger.debug('Wrote %s documents from %s to %s' % (len(lexisDocuments), path, dest_path))

'''
Clean meta-data
'''

def parseLexis(fullstr):
    lexis_docs = lexisparse.splitdocs(fullstr,
                                      bottommarker=["LOAD-DATE", "PUBLICATION-TYPE"],
                                      colnames=["LOAD-DATE", "PUBLICATION-TYPE", "JOURNAL-CODE", "DOCUMENT-TYPE",
                                                "DISTRIBUTION", "BYLINE", "SECTION", "LENGTH", "LANGUAGE", "HIGHLIGHT"])
    return lexis_docs

'''
Push to Elastic Search
'''
def pushToES():
    pass


'''
Scan folder and push all the identified files to the lexis parser.
'''

def main(path="./Backfill/"):
    scanEmailParallel(mbox='./All.mbox')
    #scanBackfill()


main()
#scanEmail()
#testEmail()