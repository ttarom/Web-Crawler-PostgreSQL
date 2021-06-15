from unittest import TestCase
from src.cron import page_count, load_data_sql, get_dataframe


# in tests we trust
# https://realpython.com/python-testing/
# ^^^^^ TEST YOUR CODE !


class Test(TestCase):
    def page_count(self):
        actual = page_count()
        self.assertTrue(actual > 0, 'page_count returned 0 pages')

    def get_dataframe(self):
        actual = get_dataframe(1)
        self.assertTrue(len(actual) > 0, 'dataframe returned an empty table')

    def load_data_sql(self):
        actual = load_data_sql(1)
        self.assertTrue(actual, 'Failed inserting record into table')
