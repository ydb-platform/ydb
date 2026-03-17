=======
Protego
=======

.. image:: https://img.shields.io/pypi/pyversions/protego.svg
   :target: https://pypi.python.org/pypi/protego
   :alt: Supported Python Versions

.. image:: https://github.com/scrapy/protego/actions/workflows/tests-ubuntu.yml/badge.svg
   :target: https://github.com/scrapy/protego/actions/workflows/tests-ubuntu.yml
   :alt: CI

Protego is a pure-Python ``robots.txt`` parser with support for modern
conventions.


Install
=======

To install Protego, simply use pip:

.. code-block:: none

    pip install protego


Usage
=====

.. code-block:: pycon

   >>> from protego import Protego
   >>> robotstxt = """
   ... User-agent: *
   ... Disallow: /
   ... Allow: /about
   ... Allow: /account
   ... Disallow: /account/contact$
   ... Disallow: /account/*/profile
   ... Crawl-delay: 4
   ... Request-rate: 10/1m                 # 10 requests every 1 minute
   ...
   ... Sitemap: http://example.com/sitemap-index.xml
   ... Host: http://example.co.in
   ... """
   >>> rp = Protego.parse(robotstxt)
   >>> rp.can_fetch("http://example.com/profiles", "mybot")
   False
   >>> rp.can_fetch("http://example.com/about", "mybot")
   True
   >>> rp.can_fetch("http://example.com/account", "mybot")
   True
   >>> rp.can_fetch("http://example.com/account/myuser/profile", "mybot")
   False
   >>> rp.can_fetch("http://example.com/account/contact", "mybot")
   False
   >>> rp.crawl_delay("mybot")
   4.0
   >>> rp.request_rate("mybot")
   RequestRate(requests=10, seconds=60, start_time=None, end_time=None)
   >>> list(rp.sitemaps)
   ['http://example.com/sitemap-index.xml']
   >>> rp.preferred_host
   'http://example.co.in'


Using Protego with Requests_:

.. code-block:: pycon

   >>> from protego import Protego
   >>> import requests
   >>> r = requests.get("https://google.com/robots.txt")
   >>> rp = Protego.parse(r.text)
   >>> rp.can_fetch("https://google.com/search", "mybot")
   False
   >>> rp.can_fetch("https://google.com/search/about", "mybot")
   True
   >>> list(rp.sitemaps)
   ['https://www.google.com/sitemap.xml']

.. _Requests: https://3.python-requests.org/


Comparison
==========

The following table compares Protego to the most popular ``robots.txt`` parsers
implemented in Python or featuring Python bindings:

+----------------------------+---------+-----------------+--------+---------------------------+
|                            | Protego | RobotFileParser | Reppy  | Robotexclusionrulesparser |
+============================+=========+=================+========+===========================+
| Implementation language    | Python  | Python          | C++    | Python                    |
+----------------------------+---------+-----------------+--------+---------------------------+
| Reference specification    | Google_ | `Martijn Koster’s 1996 draft`_                       |
+----------------------------+---------+-----------------+--------+---------------------------+
| `Wildcard support`_        | ✓       |                 | ✓      | ✓                         |
+----------------------------+---------+-----------------+--------+---------------------------+
| `Length-based precedence`_ | ✓       |                 | ✓      |                           |
+----------------------------+---------+-----------------+--------+---------------------------+
| Performance_               |         | +40%            | +1300% | -25%                      |
+----------------------------+---------+-----------------+--------+---------------------------+

.. _Google: https://developers.google.com/search/reference/robots_txt
.. _Length-based precedence: https://developers.google.com/search/reference/robots_txt#order-of-precedence-for-group-member-lines
.. _Martijn Koster’s 1996 draft: https://www.robotstxt.org/norobots-rfc.txt
.. _Performance: https://anubhavp28.github.io/gsoc-weekly-checkin-12/
.. _Wildcard support: https://developers.google.com/search/reference/robots_txt#url-matching-based-on-path-values


API Reference
=============

Class ``protego.Protego``:

Properties
----------

*   ``sitemaps`` {``list_iterator``} A list of sitemaps specified in
    ``robots.txt``.

*   ``preferred_host`` {string} Preferred host specified in ``robots.txt``.


Methods
-------

*   ``parse(robotstxt_body)`` Parse ``robots.txt`` and return a new instance of
    ``protego.Protego``.

*   ``can_fetch(url, user_agent)`` Return True if the user agent can fetch the
    URL, otherwise return ``False``.

*   ``crawl_delay(user_agent)`` Return the crawl delay specified for the user
    agent as a float. If nothing is specified, return ``None``.

*   ``request_rate(user_agent)`` Return the request rate specified for the user
    agent as a named tuple ``RequestRate(requests, seconds, start_time,
    end_time)``. If nothing is specified, return ``None``.

*   ``visit_time(user_agent)`` Return the visit time specified for the user
    agent as a named tuple ``VisitTime(start_time, end_time)``.
    If nothing is specified, return ``None``.
