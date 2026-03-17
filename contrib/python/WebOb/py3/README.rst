WebOb
=====

.. image:: https://travis-ci.org/Pylons/webob.png?branch=master
        :target: https://travis-ci.org/Pylons/webob

.. image:: https://readthedocs.org/projects/webob/badge/?version=stable
        :target: https://docs.pylonsproject.org/projects/webob/en/stable/
        :alt: Documentation Status

WebOb provides objects for HTTP requests and responses.  Specifically
it does this by wrapping the `WSGI <http://wsgi.readthedocs.io/en/latest/>`_ request
environment and response status/headers/app_iter(body).

The request and response objects provide many conveniences for parsing
HTTP request and forming HTTP responses.  Both objects are read/write:
as a result, WebOb is also a nice way to create HTTP requests and
parse HTTP responses.

Support and Documentation
-------------------------

See the `WebOb Documentation website <https://docs.pylonsproject.org/projects/webob/en/stable/>`_ to view
documentation, report bugs, and obtain support.

License
-------

WebOb is offered under the `MIT-license
<https://docs.pylonsproject.org/projects/webob/en/stable/license.html>`_.

Authors
-------

WebOb was authored by Ian Bicking and is currently maintained by the `Pylons
Project <https://pylonsproject.org/>`_ and a team of contributors.