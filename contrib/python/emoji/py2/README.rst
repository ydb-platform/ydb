Emoji
=====

Emoji for Python.  This project was inspired by `kyokomi <https://github.com/kyokomi/emoji>`__.


Example
-------

The entire set of Emoji codes as defined by the `unicode consortium <http://www.unicode.org/Public/emoji/1.0/full-emoji-list.html>`__
is supported in addition to a bunch of `aliases <http://www.emoji-cheat-sheet.com/>`__.  By
default, only the official list is enabled but doing ``emoji.emojize(language='alias')`` enables
both the full list and aliases.

.. code-block:: python

    >> import emoji
    >> print(emoji.emojize('Python is :thumbs_up:'))
    Python is 👍
    >> print(emoji.emojize('Python is :thumbsup:', language='alias'))
    Python is 👍
    >> print(emoji.demojize('Python is 👍'))
    Python is :thumbs_up:
    >>> print(emoji.emojize("Python is fun :red_heart:"))
    Python is fun ❤
    >>> print(emoji.emojize("Python is fun :red_heart:", variant="emoji_type"))
    Python is fun ❤️ #red heart, not black heart
    >>> print(emoji.is_emoji("👍"))
    True

..

By default, the language is English (``language='en'``) but  also supported languages are:

Spanish (``'es'``), Portuguese (``'pt'``), Italian (``'it'``), French (``'fr'``), German (``'de'``)


.. code-block:: python

    >> print(emoji.emojize('Python es :pulgar_hacia_arriba:', language='es'))
    Python es 👍
    >> print(emoji.demojize('Python es 👍', language='es'))
    Python es :pulgar_hacia_arriba:
    >>> print(emoji.emojize("Python é :polegar_para_cima:", language='pt'))
    Python é 👍
    >>> print(emoji.demojize("Python é 👍", language='pt'))
    Python é :polegar_para_cima:️

..

Installation
------------

Via pip:

.. code-block:: console

    $ pip install emoji --upgrade

From master branch:

.. code-block:: console

    $ git clone https://github.com/carpedm20/emoji.git
    $ cd emoji
    $ python setup.py install


Developing
----------

.. code-block:: console

    $ git clone https://github.com/carpedm20/emoji.git
    $ cd emoji
    $ pip install -e .\[dev\]
    $ pytest

The ``utils/get-codes-from-unicode-consortium.py`` may help when updating
``unicode_codes.py`` but is not guaranteed to work.  Generally speaking it
scrapes a table on the Unicode Consortium's website with
`BeautifulSoup <http://www.crummy.com/software/BeautifulSoup/>`_ and prints the
contents to ``stdout`` in a more useful format.


Links
-----

**Overview of all emoji:**

`https://carpedm20.github.io/emoji/ <https://carpedm20.github.io/emoji/>`__

(auto-generated list of the emoji that are supported by the current version of this package)

**For English:**

`Emoji Cheat Sheet <http://www.emoji-cheat-sheet.com/>`__

`Official unicode list <http://www.unicode.org/Public/emoji/1.0/full-emoji-list.html>`__

**For Spanish:**

`Unicode list <https://emojiterra.com/es/puntos-de-codigo/>`__

**For Portuguese:**

`Unicode list <https://emojiterra.com/pt/pontos-de-codigo/>`__

**For Italian:**

`Unicode list <https://emojiterra.com/it/punti-di-codice/>`__

**For French:**

`Unicode list <https://emojiterra.com/fr/points-de-code/>`__

**For German:**

`Unicode list <https://emojiterra.com/de/codepoints/>`__


Authors
-------

Taehoon Kim / `@carpedm20 <http://carpedm20.github.io/about/>`__

Kevin Wurster / `@geowurster <http://twitter.com/geowurster/>`__

Maintainer
----------
Tahir Jalilov / `@TahirJalilov <https://github.com/TahirJalilov>`__
