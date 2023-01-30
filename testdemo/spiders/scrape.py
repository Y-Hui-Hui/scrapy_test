# -*- encoding: utf-8 -*-
import scrapy
import pprint
from scrapy import Request
from testdemo.items import TestdemoItem


class ScrapeSpider(scrapy.Spider):
    name = 'scrape'
    allowed_domains = ['ssr1.scrape.center']
    base_url = 'http://ssr1.scrape.center/'
    max_page = 10

    def start_requests(self):
        for i in range(2, self.max_page + 1):
            url = f'{self.base_url}page/{i}'
            yield Request(url, callback=self.parse_index)

    def parse_index(self, response):
        for item in response.css('.item'):
            href = item.css('.name::attr(href)').get()
            url = response.urljoin(href)
            yield Request(url, callback=self.parse_detail)

    def parse_detail(self, response):
        item = TestdemoItem()
        item['name'] = response.xpath('//h2[@class="m-b-sm"]/text()').get()
        item['categories'] = response.xpath('string(//div[@class="categories"]//span)').getall()
        item['score'] = response.css('.score::text').get().strip()
        item['drama'] = response.css('.drama p::text').get().strip()
        item['directors'] = []
        directors = response.xpath('//div[contains(@class,"directors")]//div[contains(@class,"director")]')
        for director in directors:
            director_image = director.xpath('.//img[@class="image"]/@src').get()
            director_name = director.xpath('.//p/text()').get()

            item['directors'].append({
                'director_name': director_name,
                'director_image': director_image
            })
        item['actors'] = []
        actors = response.css('.actors .actor')
        for actor in actors:
            actor_image = actor.css('.actor .image::attr(src)').get()
            actor_name = actor.css('.actor .name::text').get()

            item['actors'].append({
                'actor_name': actor_name,
                'actor_image': actor_image
            })

        # pprint.pprint(item)
        yield item