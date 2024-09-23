Requests-OAuthlib |build-status| |coverage-status| |docs|
=========================================================

This project provides first-class OAuth library support for `Requests <https://requests.readthedocs.io>`_.

The OAuth 1 workflow
--------------------

OAuth 1 can seem overly complicated and it sure has its quirks. Luckily,
requests_oauthlib hides most of these and let you focus at the task at hand.

Accessing protected resources using requests_oauthlib is as simple as:

.. code-block:: pycon

    >>> from requests_oauthlib import OAuth1Session
    >>> twitter = OAuth1Session('client_key',
                                client_secret='client_secret',
                                resource_owner_key='resource_owner_key',
                                resource_owner_secret='resource_owner_secret')
    >>> url = 'https://api.twitter.com/1/account/settings.json'
    >>> r = twitter.get(url)

Before accessing resources you will need to obtain a few credentials from your
provider (e.g. Twitter) and authorization from the user for whom you wish to
retrieve resources for. You can read all about this in the full
`OAuth 1 workflow guide on RTD <https://requests-oauthlib.readthedocs.io/en/latest/oauth1_workflow.html>`_.

The OAuth 2 workflow
--------------------

OAuth 2 is generally simpler than OAuth 1 but comes in more flavours. The most
common being the Authorization Code Grant, also known as the WebApplication
flow.

Fetching a protected resource after obtaining an access token can be extremely
simple. However, before accessing resources you will need to obtain a few
credentials from your provider (e.g. Google) and authorization from the user
for whom you wish to retrieve resources for. You can read all about this in the
full `OAuth 2 workflow guide on RTD <https://requests-oauthlib.readthedocs.io/en/latest/oauth2_workflow.html>`_.

Installation
-------------

To install requests and requests_oauthlib you can use pip:

.. code-block:: bash

    pip install requests requests-oauthlib

.. |build-status| image:: https://github.com/requests/requests-oauthlib/actions/workflows/run-tests.yml/badge.svg
   :target: https://github.com/requests/requests-oauthlib/actions
.. |coverage-status| image:: https://img.shields.io/coveralls/requests/requests-oauthlib.svg
   :target: https://coveralls.io/r/requests/requests-oauthlib
.. |docs| image:: https://readthedocs.org/projects/requests-oauthlib/badge/
   :alt: Documentation Status
   :scale: 100%
   :target: https://requests-oauthlib.readthedocs.io/
