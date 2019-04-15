import urllib.request
import psycopg2
import psycopg2.extras
import gzip
import shutil
import os
import re
import logging
import math
from lib_bgp_data import Database
from configparser import ConfigParser
from datetime import datetime

LOG_LOCATION = r"/home/jab09044/HistoryLog/"

def main():
    #Logging config
    t = datetime.now()
    logging.basicConfig(level=logging.INFO, filename=LOG_LOCATION + t.strftime("%d_%m_%Y"))
    logging.info("Start Time: " + t.strftime("%c"))

    filename = "ris_whoisdump.IPv4"
    gz_filename = filename + ".gz"

    #Download and unzip prefix_origin_summary
    urllib.request.urlretrieve ("http://ris.ripe.net/dumps/riswhoisdump.IPv4.gz",gz_filename)
    with gzip.open(gz_filename, "rb") as f_gz:
        with open (filename, "wb") as f_out:
            shutil.copyfileobj(f_gz, f_out)
    os.remove(gz_filename)

    #Parse and upload to DB    
    #db = Database(cursor_factory=psycopg2.extras.NamedTupleCursor)
    cparser = ConfigParser()
    cparser.read("/etc/bgp/bgp.conf")
    #Establish DB connection, lib_bgp_data doesn't work
    try:
        conn = psycopg2.connect(host = cparser['bgp']['host'],
                                database = cparser['bgp']['database'],
                                user = cparser['bgp']['user'],
                                password = cparser['bgp']['password'])
    except:
        logging.info("Login failed at " + t.strftime("%H:%M:%S"))
    cur = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

    with open(filename) as fp:
        sql_select_table = """SELECT * FROM prefix_origin_history;"""
        sql_select = """SELECT AGE(first_seen), history
                        FROM prefix_origin_history
                        WHERE prefix_origin = (%s);"""
        sql_insert = """INSERT INTO prefix_origin_history
                        VALUES ((%s),DEFAULT, decode(%s, 'hex'));"""
        sql_update = """UPDATE prefix_origin_history
                        SET history = (%s), last_updated = (%s)
                        WHERE prefix_origin = (%s);"""
        
        temp = fp.read().splitlines() # split input file into a list of strings by line
        
        # log length of input
        logging.info("Input length: " + str(len(temp)))

        i = 0 # number of loops
        j = 0 # number of new records
        for line in temp:
            # ignores ~20 line comment block
            if(not line or line[0]=='%'):
                continue
            # Elements are tab separated
            entry = line.split('\t')
            # Ignore AS-Sets
            if (re.search('\{{1,2}|\}{1,2}', entry[0])!=None):
                continue
            # Split by ',' does nothing, entry[0] is a ~6 digit string
            origins = entry[0].split(',')

            for origin in origins:
                # Merges prefix and origin asn
                prefix_origin = entry[1] + "-" + origin
                data = (prefix_origin, )
                # gets back and age and a history for prefix_origin, can we get a row #?
                cur.execute(sql_select,data)
                record = cur.fetchone()
                if(not record):
                    j+=1
                    data = (prefix_origin, '01')
                    cur.execute(sql_insert,data)
                if(record):
                    pass
                    # Get history as bytearray from current row
                    #history = bytearray(record.history)
                    # Convert bytearray to an int
                    #histInt = int.from_bytes(history, byteorder='big', signed=False)
                    # Bitwise, shift left and add 
                    #histInt = (histInt<<1 | 0x1)
                    # Convert back to bytearray
                    #histBytes = histInt.to_bytes(math.ceil(histInt.bit_length()/8), byteorder='big', signed=False)
                    
                    # Generate tuple to pass to sql command
                    #data = (histBytes, datetime.now(), prefix_origin)
                    #cur.execute(sql_update, data)
            i+=1
            conn.commit()

    logging.info("Lines processed: " + str(i))
    logging.info("New records processed: " + str(j))
    #Close DB connection
    cur.close()
    conn.close()
    os.remove(filename)

def evenHex(num):
    temp = '%num' % (num,)
    return ('0' * (len(temp) %2)) + temp

if __name__=="__main__":
    main()
