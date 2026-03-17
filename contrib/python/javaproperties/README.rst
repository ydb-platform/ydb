|repostatus| |ci-status| |coverage| |pyversions| |license|

.. |repostatus| image:: https://www.repostatus.org/badges/latest/active.svg
    :target: https://www.repostatus.org/#active
    :alt: Project Status: Active - The project has reached a stable, usable
          state and is being actively developed.

.. |ci-status| image:: https://github.com/jwodder/javaproperties/actions/workflows/test.yml/badge.svg
    :target: https://github.com/jwodder/javaproperties/actions/workflows/test.yml
    :alt: CI Status

.. |coverage| image:: https://codecov.io/gh/jwodder/javaproperties/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/jwodder/javaproperties

.. |pyversions| image:: https://img.shields.io/pypi/pyversions/javaproperties.svg
    :target: https://pypi.org/project/javaproperties

.. |license| image:: https://img.shields.io/github/license/jwodder/javaproperties.svg?maxAge=2592000
    :target: https://opensource.org/licenses/MIT
    :alt: MIT License

`GitHub <https://github.com/jwodder/javaproperties>`_
| `PyPI <https://pypi.org/project/javaproperties>`_
| `Documentation <https://javaproperties.readthedocs.io>`_
| `Issues <https://github.com/jwodder/javaproperties/issues>`_
| `Changelog <https://github.com/jwodder/javaproperties/blob/master/CHANGELOG.md>`_

``javaproperties`` provides support for reading & writing |properties|_ (both
the simple line-oriented format and XML) with a simple API based on the
``json`` module — though, for recovering Java addicts, it also includes a
``Properties`` class intended to match the behavior of |propclass|_ as much as
is Pythonically possible.

Previous versions of ``javaproperties`` included command-line programs for
basic manipulation of ``.properties`` files.  As of version 0.4.0, these
programs have been split off into a separate package, |clipkg|_.


Installation
============
``javaproperties`` requires Python 3.8 or higher.  Just use `pip
<https://pip.pypa.io>`_ for Python 3 (You have pip, right?) to install it::

    python3 -m pip install javaproperties


Examples
========

Dump some keys & values (output order not guaranteed):

>>> properties = {"key": "value", "host:port": "127.0.0.1:80", "snowman": "☃", "goat": "🐐"}
>>> print(javaproperties.dumps(properties))
#Mon Sep 26 14:57:44 EDT 2016
key=value
goat=\ud83d\udc10
host\:port=127.0.0.1\:80
snowman=\u2603

Load some keys & values:

>>> javaproperties.loads('''
... #Mon Sep 26 14:57:44 EDT 2016
... key = value
... goat: \\ud83d\\udc10
... host\\:port=127.0.0.1:80
... #foo = bar
... snowman   ☃
... ''')
{'goat': '🐐', 'host:port': '127.0.0.1:80', 'key': 'value', 'snowman': '☃'}

Dump some properties to a file and read them back in again:

>>> with open('example.properties', 'w', encoding='latin-1') as fp:
...     javaproperties.dump(properties, fp)
...
>>> with open('example.properties', 'r', encoding='latin-1') as fp:
...     javaproperties.load(fp)
...
{'goat': '🐐', 'host:port': '127.0.0.1:80', 'key': 'value', 'snowman': '☃'}

Sort the properties you're dumping:

>>> print(javaproperties.dumps(properties, sort_keys=True))
#Mon Sep 26 14:57:44 EDT 2016
goat=\ud83d\udc10
host\:port=127.0.0.1\:80
key=value
snowman=\u2603

Turn off the timestamp:

>>> print(javaproperties.dumps(properties, timestamp=None))
key=value
goat=\ud83d\udc10
host\:port=127.0.0.1\:80
snowman=\u2603

Use your own timestamp (automatically converted to local time):

>>> print(javaproperties.dumps(properties, timestamp=1234567890))
#Fri Feb 13 18:31:30 EST 2009
key=value
goat=\ud83d\udc10
host\:port=127.0.0.1\:80
snowman=\u2603

Dump as XML:

>>> print(javaproperties.dumps_xml(properties))
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
<entry key="key">value</entry>
<entry key="goat">🐐</entry>
<entry key="host:port">127.0.0.1:80</entry>
<entry key="snowman">☃</entry>
</properties>

New in v0.6.0: Dump Unicode characters as-is instead of escaping them:

>>> print(javaproperties.dumps(properties, ensure_ascii=False))
#Tue Feb 25 19:13:27 EST 2020
key=value
goat=🐐
host\:port=127.0.0.1\:80
snowman=☃

`And more! <https://javaproperties.readthedocs.io>`_


.. |properties| replace:: Java ``.properties`` files
.. _properties: https://en.wikipedia.org/wiki/.properties

.. |propclass| replace:: Java 8's ``java.util.Properties``
.. _propclass: https://docs.oracle.com/javase/8/docs/api/java/util/Properties.html

.. |clipkg| replace:: ``javaproperties-cli``
.. _clipkg: https://github.com/jwodder/javaproperties-cli
