Wikipedia
=========

.. image:: https://travis-ci.org/goldsmith/Wikipedia.png?branch=master
  :target: https://travis-ci.org/goldsmith/Wikipedia
.. image:: https://pypip.in/d/wikipedia/badge.png
  :target: https://crate.io/packages/wikipedia
.. image:: https://pypip.in/v/wikipedia/badge.png
  :target: https://crate.io/packages/wikipedia
.. image:: https://pypip.in/license/wikipedia/badge.png
    :target: https://pypi.python.org/pypi/wikipedia/
    :alt: License

**Wikipedia** is a Python library that makes it easy to access and parse
data from Wikipedia.

Search Wikipedia, get article summaries, get data like links and images
from a page, and more. Wikipedia wraps the `MediaWiki
API <https://www.mediawiki.org/wiki/API>`__ so you can focus on using
Wikipedia data, not getting it.

.. code:: python

  >>> import wikipedia
  >>> print wikipedia.summary("Wikipedia")
  # Wikipedia (/ˌwɪkɨˈpiːdiə/ or /ˌwɪkiˈpiːdiə/ WIK-i-PEE-dee-ə) is a collaboratively edited, multilingual, free Internet encyclopedia supported by the non-profit Wikimedia Foundation...

  >>> wikipedia.search("Barack")
  # [u'Barak (given name)', u'Barack Obama', u'Barack (brandy)', u'Presidency of Barack Obama', u'Family of Barack Obama', u'First inauguration of Barack Obama', u'Barack Obama presidential campaign, 2008', u'Barack Obama, Sr.', u'Barack Obama citizenship conspiracy theories', u'Presidential transition of Barack Obama']

  >>> ny = wikipedia.page("New York")
  >>> ny.title
  # u'New York'
  >>> ny.url
  # u'http://en.wikipedia.org/wiki/New_York'
  >>> ny.content
  # u'New York is a state in the Northeastern region of the United States. New York is the 27th-most exten'...
  >>> ny.links[0]
  # u'1790 United States Census'

  >>> wikipedia.set_lang("fr")
  >>> wikipedia.summary("Facebook", sentences=1)
  # Facebook est un service de réseautage social en ligne sur Internet permettant d'y publier des informations (photographies, liens, textes, etc.) en contrôlant leur visibilité par différentes catégories de personnes.

Note: this library was designed for ease of use and simplicity, not for advanced use. If you plan on doing serious scraping or automated requests, please use `Pywikipediabot <http://www.mediawiki.org/wiki/Manual:Pywikipediabot>`__ (or one of the other more advanced `Python MediaWiki API wrappers <http://en.wikipedia.org/wiki/Wikipedia:Creating_a_bot#Python>`__), which has a larger API, rate limiting, and other features so we can be considerate of the MediaWiki infrastructure.

Installation
------------

To install Wikipedia, simply run:

::

  $ pip install wikipedia

Wikipedia is compatible with Python 2.6+ (2.7+ to run unittest discover) and Python 3.3+.

Documentation
-------------

Read the docs at https://wikipedia.readthedocs.org/en/latest/.

-  `Quickstart <https://wikipedia.readthedocs.org/en/latest/quickstart.html>`__
-  `Full API <https://wikipedia.readthedocs.org/en/latest/code.html>`__

To run tests, clone the `respository on GitHub <https://github.com/goldsmith/Wikipedia>`__, then run:

::

  $ pip install -r requirements.txt
  $ bash runtests  # will run tests for python and python3
  $ python -m unittest discover tests/ '*test.py'  # manual style

in the root project directory.

To build the documentation yourself, after installing requirements.txt, run:

::

  $ pip install sphinx
  $ cd docs/
  $ make html

License
-------

MIT licensed. See the `LICENSE
file <https://github.com/goldsmith/Wikipedia/blob/master/LICENSE>`__ for
full details.

Credits
-------

-  `wiki-api <https://github.com/richardasaurus/wiki-api>`__ by
   @richardasaurus for inspiration
-  @nmoroze and @themichaelyang for feedback and suggestions
-  The `Wikimedia
   Foundation <http://wikimediafoundation.org/wiki/Home>`__ for giving
   the world free access to data



.. image:: https://d2weczhvl823v0.cloudfront.net/goldsmith/wikipedia/trend.png
   :alt: Bitdeli badge
   :target: https://bitdeli.com/free

