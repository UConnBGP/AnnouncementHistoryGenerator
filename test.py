import psycopg2
import psycopg2.extras
from configparser import ConfigParser
from lib_bgp_data import Database

def main():
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
                    origin bigint,
                    prefix cidr,
                    first_seen date DEFAULT current_date,
                    history bigint,
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
                        SET h.history = subquery.history*2+1
                        FROM 
                        (SELECT v.origin, v.prefix, hs.history 
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
    cur.execute(sql_new_record)
    cur.execute(sql_up1_record)
    cur.execute(sql_up0_record)

if __name__=="__main__":
    main()
