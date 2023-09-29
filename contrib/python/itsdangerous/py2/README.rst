itsdangerous
============

... so better sign this

Various helpers to pass data to untrusted environments and to get it
back safe and sound. Data is cryptographically signed to ensure that a
token has not been tampered with.

It's possible to customize how data is serialized. Data is compressed as
needed. A timestamp can be added and verified automatically while
loading a token.


Installing
----------

Install and update using `pip`_:

.. code-block:: text

    pip install -U itsdangerous

.. _pip: https://pip.pypa.io/en/stable/quickstart/


A Simple Example
----------------

Here's how you could generate a token for transmitting a user's id and
name between web requests.

.. code-block:: python

    from itsdangerous import URLSafeSerializer
    auth_s = URLSafeSerializer("secret key", "auth")
    token = auth_s.dumps({"id": 5, "name": "itsdangerous"})

    print(token)
    # eyJpZCI6NSwibmFtZSI6Iml0c2Rhbmdlcm91cyJ9.6YP6T0BaO67XP--9UzTrmurXSmg

    data = auth_s.loads(token)
    print(data["name"])
    # itsdangerous


Donate
------

The Pallets organization develops and supports itsdangerous and other
popular packages. In order to grow the community of contributors and
users, and allow the maintainers to devote more time to the projects,
`please donate today`_.

.. _please donate today: https://palletsprojects.com/donate


Links
-----

*   Website: https://palletsprojects.com/p/itsdangerous/
*   Documentation: https://itsdangerous.palletsprojects.com/
*   License: `BSD <https://github.com/pallets/itsdangerous/blob/master/LICENSE.rst>`_
*   Releases: https://pypi.org/project/itsdangerous/
*   Code: https://github.com/pallets/itsdangerous
*   Issue tracker: https://github.com/pallets/itsdangerous/issues
*   Test status: https://travis-ci.org/pallets/itsdangerous
*   Test coverage: https://codecov.io/gh/pallets/itsdangerous
