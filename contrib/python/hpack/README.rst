========================================
hpack: HTTP/2 Header Encoding for Python
========================================

.. image:: https://github.com/python-hyper/hpack/workflows/CI/badge.svg
    :target: https://github.com/python-hyper/hpack/actions
    :alt: Build Status
.. image:: https://codecov.io/gh/python-hyper/hpack/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/python-hyper/hpack
    :alt: Code Coverage
.. image:: https://readthedocs.org/projects/hpack/badge/?version=latest
    :target: https://hpack.readthedocs.io/en/latest/
    :alt: Documentation Status
.. image:: https://img.shields.io/badge/chat-join_now-brightgreen.svg
    :target: https://gitter.im/python-hyper/community
    :alt: Chat community

.. image:: https://raw.github.com/python-hyper/documentation/master/source/logo/hyper-black-bg-white.png

This module contains a pure-Python HTTP/2 header encoding (HPACK) logic for use
in Python programs that implement HTTP/2.

Documentation
=============

Documentation is available at https://hpack.readthedocs.io .

Quickstart:

.. code-block:: python

    from hpack import Encoder, Decoder

    headers = [
        (':method', 'GET'),
        (':path', '/jimiscool/'),
        ('X-Some-Header', 'some_value'),
    ]

    e = Encoder()
    encoded_bytes = e.encode(headers)

    d = Decoder()
    decoded_headers = d.decode(encoded_bytes)


Contributing
============

``hpack`` welcomes contributions from anyone! Unlike many other projects we are
happy to accept cosmetic contributions and small contributions, in addition to
large feature requests and changes.

Before you contribute (either by opening an issue or filing a pull request),
please `read the contribution guidelines`_.

.. _read the contribution guidelines: http://hyper.readthedocs.org/en/development/contributing.html

License
=======

``hpack`` is made available under the MIT License. For more details, see the
``LICENSE`` file in the repository.

Authors
=======

``hpack`` is maintained by Cory Benfield, with contributions from others. For
more details about the contributors, please see ``CONTRIBUTORS.rst``.
