ItsDangerous
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

.. _pip: https://pip.pypa.io/en/stable/getting-started/


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

The Pallets organization develops and supports ItsDangerous and other
popular packages. In order to grow the community of contributors and
users, and allow the maintainers to devote more time to the projects,
`please donate today`_.

.. _please donate today: https://palletsprojects.com/donate


Links
-----

-   Documentation: https://itsdangerous.palletsprojects.com/
-   Changes: https://itsdangerous.palletsprojects.com/changes/
-   PyPI Releases: https://pypi.org/project/ItsDangerous/
-   Source Code: https://github.com/pallets/itsdangerous/
-   Issue Tracker: https://github.com/pallets/itsdangerous/issues/
-   Website: https://palletsprojects.com/p/itsdangerous/
-   Twitter: https://twitter.com/PalletsTeam
-   Chat: https://discord.gg/pallets
