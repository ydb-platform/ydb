uap-python
==========

A python implementation of the UA Parser (https://github.com/ua-parser,
formerly https://github.com/tobie/ua-parser)

Build Status
------------

.. image:: https://github.com/ua-parser/uap-python/actions/workflows/ci.yml/badge.svg
   :alt: CI on the master branch


Installing
----------

Install via pip
~~~~~~~~~~~~~~~

Just run:

.. code-block:: sh

    $ pip install ua-parser

Manual install
~~~~~~~~~~~~~~

In the top-level directory run:

.. code-block:: sh

    $ python setup.py install

Change Log
---------------
Because this repo is mostly a python wrapper for the User Agent String Parser repo (https://github.com/ua-parser/uap-core), the changes made to this repo are best described by the update diffs in that project. Please see the diffs for this submodule (https://github.com/ua-parser/uap-core/releases) for a list of what has changed between versions of this package.

Getting Started
---------------

Retrieve data on a user-agent string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> from ua_parser import user_agent_parser
    >>> import pprint
    >>> pp = pprint.PrettyPrinter(indent=4)
    >>> ua_string = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.104 Safari/537.36'
    >>> parsed_string = user_agent_parser.Parse(ua_string)
    >>> pp.pprint(parsed_string)
    {   'device': {'brand': 'Apple', 'family': 'Mac', 'model': 'Mac'},
        'os': {   'family': 'Mac OS X',
                  'major': '10',
                  'minor': '9',
                  'patch': '4',
                  'patch_minor': None},
        'string': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.104 '
                  'Safari/537.36',
        'user_agent': {   'family': 'Chrome',
                          'major': '41',
                          'minor': '0',
                          'patch': '2272'}}

Extract browser data from user-agent string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> from ua_parser import user_agent_parser
    >>> import pprint
    >>> pp = pprint.PrettyPrinter(indent=4)
    >>> ua_string = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.104 Safari/537.36'
    >>> parsed_string = user_agent_parser.ParseUserAgent(ua_string)
    >>> pp.pprint(parsed_string)
    {'family': 'Chrome', 'major': '41', 'minor': '0', 'patch': '2272'}

..

    ⚠️Before 0.15, the convenience parsers (``ParseUserAgent``,
    ``ParseOs``, and ``ParseDevice``) were not cached, which could
    result in degraded performances when parsing large amounts of
    identical user-agents (which might occur for real-world datasets).

    For these versions (up to 0.10 included), prefer using ``Parse``
    and extracting the sub-component you need from the resulting
    dictionary.

Extract OS information from user-agent string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> from ua_parser import user_agent_parser
    >>> import pprint
    >>> pp = pprint.PrettyPrinter(indent=4)
    >>> ua_string = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.104 Safari/537.36'
    >>> parsed_string = user_agent_parser.ParseOS(ua_string)
    >>> pp.pprint(parsed_string)
    {   'family': 'Mac OS X',
        'major': '10',
        'minor': '9',
        'patch': '4',
        'patch_minor': None}

Extract Device information from user-agent string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> from ua_parser import user_agent_parser
    >>> import pprint
    >>> pp = pprint.PrettyPrinter(indent=4)
    >>> ua_string = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.104 Safari/537.36'
    >>> parsed_string = user_agent_parser.ParseDevice(ua_string)
    >>> pp.pprint(parsed_string)
    {'brand': 'Apple', 'family': 'Mac', 'model': 'Mac'}

Copyright
---------

Copyright 2008 Google Inc. See ua\_parser/LICENSE for more information
