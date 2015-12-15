# Copyright 2015 Cisco Systems, Inc.
# All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import json
import psycopg2
import time

pg_con = psycopg2.connect("dbname=test user=test")
pg_cur = pg_con.cursor()


def postgresql_create_table():
    pg_cur.execute("CREATE TABLE test (PREFIX INTEGER PRIMARY KEY, data json);")
    pg_con.commit()


def postgresql_drop_table():
    pg_cur.execute("DROP TABLE IF EXISTS test;")
    pg_con.commit()


def postgresql_stats():
    print "** postgresql stats"
    pg_cur.execute("select pg_relation_size('test'),pg_total_relation_size('test')")
    pg_relation_size, pg_total_relation_size = pg_cur.fetchone()
    pg_cur.execute("select count(*) from test")
    pg_row_count = pg_cur.fetchone()
    print "count %d" % pg_row_count
    print "table storage size %d" % pg_relation_size
    print "index size %d" % (pg_total_relation_size - pg_relation_size)


def postgresql_load_prefix():
    t = time.time()
    for prefix in open('rib.json').readlines():
        prefix = json.loads(prefix)
        pg_cur.execute('INSERT INTO test (PREFIX,data) VALUES (%s,%s);', (prefix['PREFIX'], prefix))
    pg_con.commit()
    print "postgresql insert time %s" % (time.time() - t)


if __name__ == "__main__":
    postgresql_create_table()
    postgresql_load_prefix()
    postgresql_stats()
    postgresql_drop_table()


# postgresql insert time 61.1340229511
# ** postgresql stats
# count 500000
# table storage size 172482560
# index size 14794752
