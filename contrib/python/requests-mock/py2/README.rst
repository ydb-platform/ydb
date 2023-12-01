===============================
requests-mock
===============================

.. image:: https://badge.fury.io/py/requests-mock.png
    :target: https://pypi.org/project/requests-mock/

Intro
=====

`requests-mock` provides a building block to stub out the HTTP `requests`_ portions of your testing code.
You should checkout the `docs`_ for more information.

The Basics
==========

Everything in `requests`_ eventually goes through an adapter to do the transport work.
`requests-mock` creates a custom `adapter` that allows you to predefine responses when certain URIs are called.

There are then a number of methods provided to get the adapter used.

A simple example:

.. code:: python

    >>> import requests
    >>> import requests_mock

    >>> session = requests.Session()
    >>> adapter = requests_mock.Adapter()
    >>> session.mount('mock://', adapter)

    >>> adapter.register_uri('GET', 'mock://test.com', text='data')
    >>> resp = session.get('mock://test.com')
    >>> resp.status_code, resp.text
    (200, 'data')

Obviously having all URLs be `mock://` prefixed isn't going to be useful,
so you can use `requests_mock.Mocker` to get the adapter into place.

As a context manager:

.. code:: python

    >>> with requests_mock.Mocker() as m:
    ...     m.get('http://test.com', text='data')
    ...     requests.get('http://test.com').text
    ...
    'data'

Or as a decorator:

.. code:: python

    >>> @requests_mock.Mocker()
    ... def test_func(m):
    ...     m.get('http://test.com', text='data')
    ...     return requests.get('http://test.com').text
    ...
    >>> test_func()
    'data'

Or as a pytest fixture:

.. code:: python

    >>> def test_simple(requests_mock):
    ...    requests_mock.get('http://test.com', text='data')
    ...    assert 'data' == requests.get('http://test.com').text

For more information checkout the `docs`_.

Reporting Bugs
==============

Development and bug tracking is performed on `GitHub`_.

Questions
=========

There is a tag dedicated to `requests-mock` on `StackOverflow`_ where you can ask usage questions.

License
=======

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

.. _requests: https://requests.readthedocs.io
.. _docs: https://requests-mock.readthedocs.io/
.. _GitHub: https://github.com/jamielennox/requests-mock
.. _StackOverflow: https://stackoverflow.com/questions/tagged/requests-mock
