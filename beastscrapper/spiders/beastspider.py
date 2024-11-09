import scrapy
from beastscrapper.items import BeastscrapperItem


class BeastspiderSpider(scrapy.Spider):
    name = "beastspider"
    allowed_domains = ["mrbeast.store"]
    start_urls = ["https://mrbeast.store/collections/all"]

    def parse(self, response):
        products = response.css("ul#product-grid li.grid__item")

        for product in products:
            relative_url = product.css("div.card-wrapper.product-card-wrapper.underline-links-hover a::attr(href)").get()
            product_url = "https://mrbeast.store" + relative_url

            yield response.follow(product_url, callback=self.parse_product)

        next_page = response.css("link[rel=next]::attr(href)").get()

        if next_page is not None:
            next_url = "https://mrbeast.store" + next_page
            yield response.follow(next_url, callback=self.parse)

    def parse_product(self, response):
        product_item = BeastscrapperItem()

        product_item["product"] = response.css("div.product__title h1::text").get()
        product_item["price"] = response.css("div.price__container div.price__regular span.price-item.price-item--regular::text").get()
        product_item["description"] = response.css("div.product__description.rte.quick-add-hidden p span::text").get()
        product_item["reviews"] = response.css("div.jdgm-prev-badge::attr(data-average-rating)").get()
        product_item["num_reviews"] = response.css("div.jdgm-prev-badge::attr(data-number-of-reviews)").get()
        product_item["url"] = response.css("link[rel=canonical]::attr(href)").get()

        yield product_item
