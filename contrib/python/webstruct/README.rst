Webstruct
=========

.. image:: https://img.shields.io/pypi/v/webstruct.svg
   :target: https://pypi.python.org/pypi/webstruct
   :alt: PyPI Version

.. image:: https://travis-ci.org/scrapinghub/webstruct.svg?branch=master
   :target: https://travis-ci.org/scrapinghub/webstruct
   :alt: Build Status

.. image:: https://codecov.io/gh/scrapinghub/webstruct/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/scrapinghub/webstruct
   :alt: Code Coverage

.. image:: https://readthedocs.org/projects/webstruct/badge/?version=latest
   :target: http://webstruct.readthedocs.io/en/latest/
   :alt: Documentation


Webstruct is a library for creating statistical NER_ systems that work
on HTML data, i.e. a library for building tools that extract named
entities (addresses, organization names, open hours, etc) from webpages.

Unlike most NER systems, webstruct works on HTML data, not only
on text data. This allows to define features that use HTML structure,
and also to embed annotation results back into HTML.

Read the docs_ for more info.

License is MIT.

.. _docs: http://webstruct.readthedocs.io/en/latest/
.. _NER: http://en.wikipedia.org/wiki/Named-entity_recognition

Contributing
------------

* Source code: https://github.com/scrapinghub/webstruct
* Bug tracker: https://github.com/scrapinghub/webstruct/issues

To run tests, make sure tox_ is installed, then run
``tox`` from the source root.

.. _tox: https://tox.readthedocs.io/en/latest/
