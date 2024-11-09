# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item prices with a single interface
from itemadapter import ItemAdapter
import psycopg2

class BeastscrapperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Strip all whitespaces from string
        field_names = adapter.field_names()
        for field_name in field_names:
            value = adapter.get(field_name)
            if isinstance(value, str):
                adapter[field_name] = value.strip()

        # Propercase Product Name
        proper_case = adapter.get("product")
        if proper_case:
            adapter["product"] = proper_case.title()

        # Convert price to int
        price_key = adapter.get("price")
        if price_key:
            value = price_key.strip()
            value = value.replace(",", "").replace(".", "").replace("Rp", "").replace("IDR", "")
            adapter["price"] = int(value)

        # Convert reviews and num_reviews to float
        int_values = ["reviews", "num_reviews"]
        for int_value in int_values:
            value = adapter.get(int_value)
            if value:
                adapter[int_value] = float(value)

        return item

class SaveToPostgreSQLPipeline:
    def __init__(self):
        self.connect = psycopg2.connect(
            host="localhost",
            dbname="WebScrapping",
            user="postgres",
            password="ivanbenedictus",
            port=5432
        )
        self.cursor = self.connect.cursor()

        # Create a Table
        create_table = """
        CREATE TABLE IF NOT EXISTS beastspider(
            product VARCHAR(255) PRIMARY KEY, 
            price INTEGER,
            description VARCHAR(255),
            reviews FLOAT,
            num_reviews FLOAT,
            url VARCHAR(255)
        )
        """
        self.cursor.execute(create_table)
        self.connect.commit()

    def process_item(self, item, spider):
        data_name = """
        INSERT INTO beastspider(
            product,
            price,
            description,
            reviews,
            num_reviews,
            url
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (product) DO NOTHING
        """
        data_values = (
            item.get("product"),
            item.get("price"),
            item.get("description"),
            item.get("reviews"),
            item.get("num_reviews"),
            item.get("url")
        )

        try:
            self.cursor.execute(data_name, data_values)
            self.connect.commit()
        except psycopg2.Error as e:
            self.connect.rollback()  # Rollback the transaction to clear the error state
            spider.logger.error(f"Database error: {e}")
            raise e

        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()