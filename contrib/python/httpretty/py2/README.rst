HTTPretty 0.9.7
===============

HTTP Client mocking tool for Python. Provides a full fake TCP socket module. Inspired by `FakeWeb <https://github.com/chrisk/fakeweb>`_

- `Github Repository <https://github.com/gabrielfalcao/HTTPretty>`_
- `Documentation <https://httpretty.readthedocs.io/en/latest/>`_
- `PyPI Package <https://pypi.org/project/httpretty/>`_


**Python Support:**

- **2.7.13**
- **3.6.5**

.. image:: https://github.com/gabrielfalcao/HTTPretty/raw/master/docs/source/_static/logo.svg?sanitize=true

.. image:: https://readthedocs.org/projects/httpretty/badge/?version=latest
   :target: http://httpretty.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status
.. image:: https://travis-ci.org/gabrielfalcao/HTTPretty.svg?branch=master
    :target: https://travis-ci.org/gabrielfalcao/HTTPretty
.. |PyPI python versions| image:: https://img.shields.io/pypi/pyversions/HTTPretty.svg
   :target: https://pypi.python.org/pypi/HTTPretty
.. |Join the chat at https://gitter.im/gabrielfalcao/HTTPretty| image:: https://badges.gitter.im/gabrielfalcao/HTTPretty.svg
   :target: https://gitter.im/gabrielfalcao/HTTPretty?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge


Install
-------

.. code:: bash

   pip install httpretty



Common Use Cases
================

- Test-driven development of API integrations
- Fake responses of external APIs
- Record and playback HTTP requests


Simple Example
--------------

.. code:: python

    import sure
    import httpretty
    import requests


    @httpretty.activate
    def test_httpbin():
        httpretty.register_uri(
            httpretty.GET,
            "https://httpbin.org/ip",
            body='{"origin": "127.0.0.1"}'
        )

        response = requests.get('https://httpbin.org/ip')
        response.json().should.equal({'origin': '127.0.0.1'})

        httpretty.latest_requests().should.have.length_of(1)
        httpretty.last_request().should.equal(httpretty.latest_requests()[0])
        httpretty.last_request().body.should.equal('{"origin": "127.0.0.1"}')


checking multiple responses
---------------------------

 .. code:: python

    @httpretty.activate
    def test_post_bodies():
        url = 'http://httpbin.org/post'
        httpretty.register_uri(httpretty.POST, url, status=200)
        httpretty.register_uri(httpretty.POST, url, status=400)
         requests.post(url, data={'foo': 'bar'})
        requests.post(url, data={'zoo': 'zoo'})
         assert 'foo=bar' in httpretty.latest_requests()[0].body
        assert 'zoo=bar' in httpretty.latest_requests()[1].body


License
=======

::

    <HTTPretty - HTTP client mock for Python>
    Copyright (C) <2011-2018>  Gabriel Falcão <gabriel@nacaolivre.org>

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following
    conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    OTHER DEALINGS IN THE SOFTWARE.

Main contributors
=================

HTTPretty has received `many contributions <https://github.com/gabrielfalcao/HTTPretty/graphs/contributors>`_
but some folks made remarkable contributions and deserve extra credit:

-  Andrew Gross ~> `@andrewgross <https://github.com/andrewgross>`_
-  Hugh Saunders ~> `@hughsaunders <https://github.com/hughsaunders>`_
-  James Rowe ~> `@JNRowe <https://github.com/JNRowe>`_
-  Matt Luongo ~> `@mhluongo <https://github.com/mhluongo>`_
-  Steve Pulec ~> `@spulec <https://github.com/spulec>`_
-  Miro Hrončok ~> `@hroncok <https://github.com/hroncok>`_
