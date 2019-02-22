# -*- coding: utf-8 -*-
import scrapy
import re
import pymysql
import pymongo
from scrapy import Request
from jisho.items import JishoItem


class FanyiSpider(scrapy.Spider):
    name = 'fanyi'
    allowed_domains = ['sss']
    db = pymysql.Connect(host='127.0.0.1', user='root', password='18351962092', db='frequentwords')
    cursor = db.cursor()
    sql = 'select * from en_full limit 0,500000'
    cursor.execute(sql)
    results = cursor.fetchall()
    start_urls = [f'https://jisho.org/search/{row[0]}%23sentences?page=1' for row in results]
    #start_urls = ['https://jisho.org/search/you%23sentences?page=1']
    headers = {
        'User-Agent':  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
    }
    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse, headers=self.headers, dont_filter=True, meta={'tag': 0})

    def parse(self, response):
        try:
            sentenceCount = int(re.findall('class="result_count"> — (\d+?) found<\/span><\/h4>', response.text)[0])
        except IndexError:
            #没有这个词的句子就把页数当作零
            sentenceCount = 0
        pageNum = sentenceCount//20 if sentenceCount % 20 == 0 else sentenceCount//20 + 1
        for i in range(pageNum):
            url = response.url.replace('page=1', f'page={str(i+1)}')
            host = '127.0.0.1'
            user = 'root'
            passwd = '18351962092'
            dbname = 'jishoUrl'
            tablename = 'url'
            db = pymysql.connect(host, user, passwd, dbname)
            cursor = db.cursor()
            sql = f"select * from {tablename}"
            cursor.execute(sql)
            results = cursor.fetchall()
            db.commit()
            cursor.close()
            db.close()
            urls = []
            for row in results:
                urls.append(row[0])
            # 如果链接内容已经爬取过，那么不爬
            if url not in urls:
                yield Request(url=url, callback=self.parsePlus, headers=self.headers, dont_filter=True, meta={'tag': 0})

    def parsePlus(self, response):
        selectors = response.xpath('//div[@class="sentence_content"]')
        for selector in selectors:
            item = JishoItem()
            item['english'] = selector.xpath('.//span[@class="english"]/text()').extract_first()
            selectorJapan = selector.xpath('.//ul[@class="japanese_sentence japanese japanese_gothic clearfix"]//span[@class="unlinked"]/text()').extract()
            # for i in selectorJapan:
            item['japanese'] = ''.join(selectorJapan)
            print(item['japanese'] + response.url)
            print(item['english'] + response.url)
            yield item


