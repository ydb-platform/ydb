======
Parsel
======

.. image:: https://github.com/scrapy/parsel/actions/workflows/tests-ubuntu.yml/badge.svg
   :target: https://github.com/scrapy/parsel/actions/workflows/tests-ubuntu.yml
   :alt: Tests

.. image:: https://img.shields.io/pypi/pyversions/parsel.svg
   :target: https://github.com/scrapy/parsel/actions/workflows/tests.yml
   :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/v/parsel.svg
   :target: https://pypi.python.org/pypi/parsel
   :alt: PyPI Version

.. image:: https://img.shields.io/codecov/c/github/scrapy/parsel/master.svg
   :target: https://codecov.io/github/scrapy/parsel?branch=master
   :alt: Coverage report


Parsel is a BSD-licensed Python_ library to extract data from HTML_, JSON_, and
XML_ documents.

It supports:

-   CSS_ and XPath_ expressions for HTML and XML documents

-   JMESPath_ expressions for JSON documents

-   `Regular expressions`_

Find the Parsel online documentation at https://parsel.readthedocs.org.

Example (`open online demo`_):

.. code-block:: pycon

    >>> from parsel import Selector
    >>> text = """
    ... <html>
    ...     <body>
    ...         <h1>Hello, Parsel!</h1>
    ...         <ul>
    ...             <li><a href="http://example.com">Link 1</a></li>
    ...             <li><a href="http://scrapy.org">Link 2</a></li>
    ...         </ul>
    ...         <script type="application/json">{"a": ["b", "c"]}</script>
    ...     </body>
    ... </html>"""
    >>> selector = Selector(text=text)
    >>> selector.css("h1::text").get()
    'Hello, Parsel!'
    >>> selector.xpath("//h1/text()").re(r"\w+")
    ['Hello', 'Parsel']
    >>> for li in selector.css("ul > li"):
    ...     print(li.xpath(".//@href").get())
    ...
    http://example.com
    http://scrapy.org
    >>> selector.css("script::text").jmespath("a").get()
    'b'
    >>> selector.css("script::text").jmespath("a").getall()
    ['b', 'c']

.. _CSS: https://en.wikipedia.org/wiki/Cascading_Style_Sheets
.. _HTML: https://en.wikipedia.org/wiki/HTML
.. _JMESPath: https://jmespath.org/
.. _JSON: https://en.wikipedia.org/wiki/JSON
.. _open online demo: https://colab.research.google.com/drive/149VFa6Px3wg7S3SEnUqk--TyBrKplxCN#forceEdit=true&sandboxMode=true
.. _Python: https://www.python.org/
.. _regular expressions: https://docs.python.org/library/re.html
.. _XML: https://en.wikipedia.org/wiki/XML
.. _XPath: https://en.wikipedia.org/wiki/XPath
