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
import time
import pymongo

mg_db = pymongo.MongoClient(host='127.0.0.1').test
mg_col = mg_db.test_collection


def mongodb_drop_table():
    mg_db.drop_collection('test_collection')


def test_json_eval_load_time():

    # for json load time
    t = time.time()
    for prefix in open('rib.json').readlines():
        prefix = json.loads(prefix)
        pass
    print 'json load time: %s' % (time.time() - t)

    # for eval load time

    t = time.time()
    for prefix in open('rib.json').readlines():
        prefix = eval(prefix)
        pass
    print 'eval load time %s' % (time.time() - t)


def insert_mongo():
    # insert to mongo time
    t = time.time()
    for prefix in open('rib.json').readlines():
        prefix = json.loads(prefix)
        mg_col.insert(prefix)
    print 'mongo insert time %s ' % (time.time() - t)
    mongodb_stats()
    mongodb_drop_table()


def mongodb_stats():
    print "** mongodb stats"
    print "count %d" % mg_col.database.command('collstats', 'test_collection')['count']
    print "size %d" % mg_col.database.command('collstats', 'test_collection')['size']
    print "storage size %d" % mg_col.database.command('collstats', 'test_collection')['storageSize']
    print "index size %d" % mg_col.database.command('collstats', 'test_collection')['totalIndexSize']

if __name__ == "__main__":

    test_json_eval_load_time()
    insert_mongo()

# json load time: 5.47320604324
# eval load time 22.8725829124
# mongo insert time 173.118676901
# ** mongodb stats
# count 500000
# size 247998720
# storage size 335900672
# index size 16237536
