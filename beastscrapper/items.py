# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BeastscrapperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # define the fields for your item here like:
    # name = scrapy.Field()
    product = scrapy.Field()
    price = scrapy.Field()
    description = scrapy.Field()
    reviews = scrapy.Field()
    num_reviews = scrapy.Field()
    url = scrapy.Field()
