===========
itemloaders
===========

.. image:: https://img.shields.io/pypi/v/itemloaders.svg
   :target: https://pypi.python.org/pypi/itemloaders
   :alt: PyPI Version

.. image:: https://img.shields.io/pypi/pyversions/itemloaders.svg
   :target: https://pypi.python.org/pypi/itemloaders
   :alt: Supported Python Versions

.. image:: https://github.com/scrapy/itemloaders/actions/workflows/tests-ubuntu.yml/badge.svg
   :target: https://github.com/scrapy/itemloaders/actions/workflows/tests-ubuntu.yml
   :alt: CI Status

.. image:: https://codecov.io/github/scrapy/itemloaders/coverage.svg?branch=master
   :target: https://codecov.io/gh/scrapy/itemloaders
   :alt: Coverage report

.. image:: https://readthedocs.org/projects/itemloaders/badge/?version=latest
   :target: https://itemloaders.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status


``itemloaders`` is a library that helps you collect data from HTML and XML sources.

It comes in handy to extract data from web pages, as it supports
data extraction using CSS and XPath Selectors.

It's specially useful when you need to standardize the data from many sources.
For example, it allows you to have all your casting and parsing rules in a
single place.

Here is an example to get you started::

    from itemloaders import ItemLoader
    from parsel import Selector

    html_data = '''
    <!DOCTYPE html>
    <html>
        <head>
            <title>Some random product page</title>
        </head>
        <body>
            <div class="product_name">Some random product page</div>
            <p id="price">$ 100.12</p>
        </body>
    </html>
    '''
    loader = ItemLoader(selector=Selector(html_data))
    loader.add_xpath('name', '//div[@class="product_name"]/text()')
    loader.add_xpath('name', '//div[@class="product_title"]/text()')
    loader.add_css('price', '#price::text')
    loader.add_value('last_updated', 'today') # you can also use literal values
    item = loader.load_item()
    item
    # {'name': ['Some random product page'], 'price': ['$ 100.12'], 'last_updated': ['today']}

For more information, check out the `documentation <https://itemloaders.readthedocs.io/en/latest/>`_.

Contributing
============

All contributions are welcome!

* If you want to review some code, check open
  `Pull Requests here <https://github.com/scrapy/itemloaders/pulls>`_

* If you want to submit a code change

   * File an `issue here <https://github.com/scrapy/itemloaders/issues>`_, if there isn't one yet
   * Fork this repository
   * Create a branch to work on your changes
   * Run `pre-commit install` to install pre-commit hooks
   * Push your local branch and submit a Pull Request
