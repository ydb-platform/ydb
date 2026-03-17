requests-ntlm
=============

.. image:: https://github.com/requests/requests-ntlm/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/requests/requests-ntlm/actions/workflows/ci.yml

This package allows for HTTP NTLM authentication using the requests library.

Usage
-----

``HttpNtlmAuth`` extends requests ``AuthBase``, so usage is simple:

.. code:: python

    import requests
    from requests_ntlm import HttpNtlmAuth

    requests.get("http://ntlm_protected_site.com",auth=HttpNtlmAuth('domain\\username','password'))

``HttpNtlmAuth`` can be used in conjunction with a ``Session`` in order to
make use of connection pooling. Since NTLM authenticates connections,
this is more efficient. Otherwise, each request will go through a new
NTLM challenge-response.

.. code:: python

    import requests
    from requests_ntlm import HttpNtlmAuth

    session = requests.Session()
    session.auth = HttpNtlmAuth('domain\\username','password')
    session.get('http://ntlm_protected_site.com')

Installation
------------

    pip install requests_ntlm

Requirements
------------

- requests_
- pyspnego_

.. _requests: https://github.com/kennethreitz/requests/
.. _pyspnego: https://github.com/jborean93/pyspnego/

Authors
-------

- `Ben Toews`_

.. _Ben Toews: https://github.com/mastahyeti

- `Ian Cordasco`_

.. _Ian Cordasco: https://github.com/sigmavirus24

- `Cory Benfield`_

.. _Cory Benfield: https://github.com/Lukasa
