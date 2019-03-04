import urllib.request
import psycopg2
import psycopg2.extras
import gzip
import shutil
import os
import re
from lib_bgp_data import Database
from configparser import ConfigParser

def main():
    filename = "ris_whoisdump.IPv4"
    gz_filename = filename + ".gz"

    #Download and unzip prefix_origin_summary
    urllib.request.urlretrieve ("http://ris.ripe.net/dumps/riswhoisdump.IPv4.gz",gz_filename)
    with gzip.open(gz_filename, "rb") as f_gz:
        with open (filename, "wb") as f_out:
            shutil.copyfileobj(f_gz, f_out)
    os.remove(gz_filename)

    #Parse and upload to DB
#    db = Database(cursor_factory=psycopg2.extras.NamedTupleCursor)
    cparser = ConfigParser()
    cparser.read("/etc/bgp/bgp.conf")
    #Establish DB connection, lib_bgp_data doesn't work
    conn = psycopg2.connect(host = cparser['bgp']['host'],
                            database = cparser['bgp']['database'],
                            user = cparser['bgp']['user'],
                            password = cparser['bgp']['password'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

    with open(filename) as fp:
        sql_select_table = """SELECT * FROM prefix_origin_history;"""
        sql_select = """SELECT AGE(first_seen), history 
                        FROM prefix_origin_history 
                        WHERE prefix_origin = (%s);"""
        sql_insert = """INSERT INTO prefix_origin_history 
                        VALUES ((%s),DEFAULT, decode(%s, 'hex'));"""
        sql_update = """UPDATE prefix_origin_history 
                        SET history = (%s) 
                        WHERE prefix_origin = (%s);"""
        temp = fp.read().splitlines()
        i = 0
        for line in temp:
            if(not line or line[0]=='%'):
                continue
            entry = line.split('\t')
            #Remove curly braces
            re.sub('{{ | }}', '', entry[0])
            origins = entry[0].split(',')

            for origin in origins:
                prefix_origin = entry[1] + "-" + origin
                data = (prefix_origin,)
                cur.execute(sql_select,data)
                record = cur.fetchone()
                if(not record):
                    data = (prefix_origin, '01')
                    cur.execute(sql_insert,data)
                if(record):
                    history = bytearray(record.history)
                    age = record.age.days
                    #history is an array of bytes
                    if(age/8 > len(history)):
                        history.append(0)
                    bit_to_flip = age % 8
                    history[-1] = history[-1] | bit_to_flip
                    data = (history,prefix_origin)
                    cur.execute(sql_update,data) 
    #                days_old =
    #                data = 
    #                db.execute(sql_update,data) 
            i+=1
            conn.commit()
    #Close DB connection
    cur.close()
    conn.close()
    os.remove(filename)

def evenHex(num):
    temp = '%num' % (num,)
    return ('0' * (len(temp) %2)) + temp

if __name__=="__main__":
    main()
