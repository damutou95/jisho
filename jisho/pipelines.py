# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import hashlib
import time
import logging
from pymongo.errors import DuplicateKeyError

class JishoPipeline(object):
    def process_item(self, item, spider):
        client = pymongo.MongoClient(host='127.0.0.1', port=27017)
        db = client['jisho']
        col = db['sentences']
        hash = hashlib.md5()
        hash.update((item['english'] + item['japanese']).encode('utf-8'))
        hashCode = hash.hexdigest()
        data = {'_id': hashCode, 'english': item['english'], 'japanese': item['japanese'], 'time': time.strftime('%Y.%m.%d %H:%M:%S', time.localtime())}
        try:
            col.insert(data)
            logging.info('#' * 20 + '成功插入一条数据！')
        except DuplicateKeyError:
            logging.info('#' * 20 + '成功过滤重复请求！')

        return item
