# -*- encoding: utf-8 -*-
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from elasticsearch import Elasticsearch
from scrapy import Request
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class TestdemoPipeline:
    def process_item(self, item, spider):
        return item


class MongoDBPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        cls.cle = crawler.settings.get('MONGODB_CONNECTION_STRING')
        cls.name = crawler.settings.get('MONGODB_NAME')
        cls.collection = crawler.settings.get('MONGODB_COLLECTION')
        return cls()

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.cle)
        self.db = self.client[self.name]

    def process_item(self, item, spider):
        # 这里用到了 $set 运算符，该运算符作用是将字段的值替换为指定的值，upsert 为 True 表示插入
        # 在爬取数据希望没有重复时使用update_one,爬虫中断再此爬取也不用担心遇到重复数据
        self.db[self.collection].update_one({'name': item['name']},
                                            {'$set': dict(item)}, upsert=True)
        return item

    def close_spider(self, spider):
        self.client.close()


class EsPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        cls.con = crawler.settings.get('ES_URL')
        cls.index = crawler.settings.get('ES_INDEX')
        return cls()

    def open_spider(self, spider):
        self.conn = Elasticsearch([self.con])
        if not self.conn.indices.exists(self.index):
            self.conn.indices.create(index=self.index)

    def process_item(self, item, spider):
        self.conn.index(index=self.index, doc_type='movie', body=dict(item), id=hash(item['name']))
        return item

    def close_spider(self, spider):
        self.conn.transport.close()


class ImagePipeline(ImagesPipeline):
    def file_path(self, request, response=None, info=None):
        movie = request.meta['movie']
        type = request.meta['type']
        name = request.meta['name']
        file_name = f'{movie}/{type}/{name}.jpg'
        return file_name

    def item_completed(self, results, item, info):
        image_paths = [x for ok, x in results if ok]
        if not image_paths:
            raise DropItem('Image Downloaded Failed')
        return item

    def get_media_requests(self, item, info):
        for director in item['directors']:
            director_name = director['director_name']
            director_image = director['director_image']
            yield Request(director_image, meta={
                'name': director_name,
                'type': 'director',
                'movie': item['name']
            })
        for actor in item['actors']:
            actor_name = actor['actor_name']
            actor_image = actor['actor_image']
            yield Request(actor_image, meta={
                'name': actor_name,
                'type': 'actor',
                'movie': item['name']
            })
