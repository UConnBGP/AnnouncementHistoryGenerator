import psycopg2
import psycopg2.extras
import logging
from datetime import datetime
from configparser import ConfigParser
from lib_bgp_data import Database

LOG_LOCATION = r"/home/jab09044/HistoryLog/"

def main():
    t = datetime.now()
    logging.basicConfig(level=logging.INFO, filename=LOG_LOCATION + t.strftime("%d_%m_%Y"))
    logging.info("Start Time: " + t.strftime("%c"))

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

    # Generate Table
    sql_table = """CREATE TABLE IF NOT EXISTS prefix_origin_history (
                    first_seen date DEFAULT current_date,
                    history bigint DEFAULT 1,
                    LIKE validity INCLUDING prefix, asn AS origin
                    PRIMARY KEY (origin, prefix)
                );"""

    # Case 1: Add new Entry 
    sql_new_record = """INSERT INTO prefix_origin_history
                        (SELECT v.asn, v.prefix, current_timestamp, 1 
                        AS history FROM prefix_origin_history AS h
                        RIGHT JOIN validity v
                        ON v.prefix != h.prefix AND v.asn != h.origin)"""
    
    # Case 2: Update record with 1 for seen prefix-origin
    sql_up1_record = """UPDATE prefix_origin_history 
                        SET history = subquery.history*2+1
                        FROM 
                        (SELECT hs.origin, hs.prefix, hs.history 
                        AS history FROM prefix_origin_history AS hs
                        INNER JOIN validity v
                        ON v.prefix = hs.prefix AND v.asn = hs.origin) 
                        AS subquery"""

    # Case 3: Update record with 0 for unseen prefix-origin
    sql_up0_record = """UPDATE prefix_origin_history 
                        SET h.history = subquery.history*2
                        FROM 
                        (SELECT v.origin, v.prefix, hs.history 
                        AS history FROM prefix_origin_history AS hs
                        LEFT JOIN validity v
                        ON v.prefix != hs.prefix AND v.asn != hs.origin) 
                        AS subquery"""
    
    cur.execute(sql_table)
    logging.info("Table creation completed.")
    cur.execute(sql_new_record)
    logging.info("New entries added.")
    cur.execute(sql_up1_record)
    logging.info("Seen pairs updated.")
    cur.execute(sql_up0_record)
    logging.info("Unseen pairs updated.")


    logging.info("End Time: " + datetime.now().strftime("%c"))

if __name__=="__main__":
    main()
