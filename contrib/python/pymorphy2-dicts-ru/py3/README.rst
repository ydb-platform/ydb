pymorphy2-dicts-ru
==========================================================

Russian dictionaries for `pymorphy2`_.

.. _pymorphy2: https://github.com/kmike/pymorphy2

Installation
------------

Install::

    $ pip install pymorphy2-dicts-ru

Usage
-----

To use these dictionaries with pymorphy2 create MorphAnalyzer
with ``lang='ru'`` parameter:

>>> import pymorphy2
>>> morph = pymorphy2.MorphAnalyzer(lang='ru')

To get a path to the installed dictionary data use
``pymorphy2_dicts_ru.get_path()`` method.

Development
-----------

The main repo is https://github.com/kmike/pymorphy2-dicts/. The repository
doesn't contain the data itself: only package template and update
scripts are stored in VCS.

License for Python code in this package is MIT.
The data is licensed under
`Creative Commons Attribution-Share Alike <http://creativecommons.org/licenses/by-sa/3.0/>`_.
