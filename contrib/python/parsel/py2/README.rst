======
Parsel
======

.. image:: https://img.shields.io/travis/scrapy/parsel/master.svg
   :target: https://travis-ci.org/scrapy/parsel
   :alt: Build Status

.. image:: https://img.shields.io/pypi/v/parsel.svg
   :target: https://pypi.python.org/pypi/parsel
   :alt: PyPI Version

.. image:: https://img.shields.io/codecov/c/github/scrapy/parsel/master.svg
   :target: http://codecov.io/github/scrapy/parsel?branch=master
   :alt: Coverage report


Parsel is a BSD-licensed Python_ library to extract and remove data from HTML_
and XML_ using XPath_ and CSS_ selectors, optionally combined with
`regular expressions`_.

Find the Parsel online documentation at https://parsel.readthedocs.org.

Example (`open online demo`_):

.. code-block:: python

    >>> from parsel import Selector
    >>> selector = Selector(text=u"""<html>
            <body>
                <h1>Hello, Parsel!</h1>
                <ul>
                    <li><a href="http://example.com">Link 1</a></li>
                    <li><a href="http://scrapy.org">Link 2</a></li>
                </ul>
            </body>
            </html>""")
    >>> selector.css('h1::text').get()
    'Hello, Parsel!'
    >>> selector.xpath('//h1/text()').re(r'\w+')
    ['Hello', 'Parsel']
    >>> for li in selector.css('ul > li'):
    ...     print(li.xpath('.//@href').get())
    http://example.com
    http://scrapy.org


.. _CSS: https://en.wikipedia.org/wiki/Cascading_Style_Sheets
.. _HTML: https://en.wikipedia.org/wiki/HTML
.. _open online demo: https://colab.research.google.com/drive/149VFa6Px3wg7S3SEnUqk--TyBrKplxCN#forceEdit=true&sandboxMode=true
.. _Python: https://www.python.org/
.. _regular expressions: https://docs.python.org/library/re.html
.. _XML: https://en.wikipedia.org/wiki/XML
.. _XPath: https://en.wikipedia.org/wiki/XPath
