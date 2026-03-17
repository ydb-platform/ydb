.. -*-restructuredtext-*-

.. image:: https://github.com/ubernostrum/webcolors/workflows/CI/badge.svg
   :alt: CI status image
   :target: https://github.com/ubernostrum/webcolors/actions?query=workflow%3ACI

``webcolors`` is a module for working with and converting between the
various HTML/CSS color formats.

Support is included for normalizing and converting between the
following formats (RGB colorspace only; conversion to/from HSL can be
handled by the ``colorsys`` module in the Python standard library):

* Specification-defined color names

* Six-digit hexadecimal

* Three-digit hexadecimal

* Integer ``rgb()`` triplet

* Percentage ``rgb()`` triplet

For example:

.. code-block:: python

    >>> import webcolors
    >>> webcolors.hex_to_name("#daa520")
    'goldenrod'

Implementations are also provided for the HTML5 color parsing and
serialization algorithms. For example, parsing the infamous
"chucknorris" string into an ``rgb()`` triplet:

.. code-block:: python

    >>> import webcolors
    >>> webcolors.html5_parse_legacy_color("chucknorris")
    HTML5SimpleColor(red=192, green=0, blue=0)

Full documentation is `available online <https://webcolors.readthedocs.io/>`_.
