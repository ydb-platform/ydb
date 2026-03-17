.. raw:: html

   <img align="right" width="auto" height="auto" src="https://www.elastic.co/static-res/images/elastic-logo-200.png">


Elasticsearch Python Client
===========================

.. image:: https://img.shields.io/pypi/v/elasticsearch
   :target: https://pypi.org/project/elasticsearch

.. image:: https://img.shields.io/conda/vn/conda-forge/elasticsearch?color=blue
   :target: https://anaconda.org/conda-forge/elasticsearch

.. image:: https://pepy.tech/badge/elasticsearch
   :target: https://pepy.tech/project/elasticsearch?versions=*

.. image:: https://clients-ci.elastic.co/job/elastic+elasticsearch-py+master/badge/icon
   :target: https://clients-ci.elastic.co/job/elastic+elasticsearch-py+master

.. image:: https://readthedocs.org/projects/elasticsearch-py/badge/?version=latest&style=flat
   :target: https://elasticsearch-py.readthedocs.io

*The official Python client for Elasticsearch.*


Features
--------

* Translating basic Python data types to and from JSON
* Configurable automatic discovery of cluster nodes
* Persistent connections
* Load balancing (with pluggable selection strategy) across available nodes
* Failed connection penalization (time based - failed connections won't be
  retried until a timeout is reached)
* Support for TLS and HTTP authentication
* Thread safety across requests
* Pluggable architecture
* Helper functions for idiomatically using APIs together


Installation
------------

Install the ``elasticsearch`` package with `pip
<https://pypi.org/project/elasticsearch>`_::

    $ python -m pip install elasticsearch

If your application uses async/await in Python you can install with
the ``async`` extra::

    $ python -m pip install elasticsearch[async]

Read more about `how to use asyncio with this project <https://elasticsearch-py.readthedocs.io/en/master/async.html>`_.


Compatibility
-------------

Language clients are forward compatible; meaning that clients support communicating
with greater or equal minor versions of Elasticsearch. Elasticsearch language clients
are only backwards compatible with default distributions and without guarantees made.

If you have a need to have multiple versions installed at the same time older
versions are also released as ``elasticsearch2`` and ``elasticsearch5``.


Documentation
-------------

Documentation for the client is `available on elastic.co`_ and `Read the Docs`_.

.. _available on elastic.co: https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html
.. _Read the Docs: https://elasticsearch-py.readthedocs.io

Quick Start
-----------

.. code-block:: python

    # Import the client from the 'elasticsearch' module
    >>> from elasticsearch import Elasticsearch
    
    # Instantiate a client instance
    >>> client = Elasticsearch("http://localhost:9200")
    
    # Call an API, in this example `info()`
    >>> resp = client.info()

    # View the result
    >>> resp
    {
      "name" : "instance-name",
      "cluster_name" : "cluster-name",
      "cluster_uuid" : "cluster-uuid",
      "version" : {
        "number" : "7.14.0",
        ...
      },
      "tagline" : "You know, for Search"
    }


You can read more about `configuring the client`_ in the documentation.

.. _configuring the client: https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html


License
-------

Copyright 2021 Elasticsearch B.V. Licensed under the Apache License, Version 2.0.
