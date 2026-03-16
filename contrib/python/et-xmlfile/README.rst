.. image:: https://foss.heptapod.net/openpyxl/et_xmlfile/badges/branch/default/coverage.svg
    :target: https://coveralls.io/bitbucket/openpyxl/et_xmlfile?branch=default
    :alt: coverage status

et_xmfile
=========

XML can use lots of memory, and et_xmlfile is a low memory library for creating large XML files
And, although the standard library already includes an incremental parser, `iterparse` it has no equivalent when writing XML. Once an element has been added to the tree, it is written to
the file or stream and the memory is then cleared.

This module is based upon the `xmlfile module from lxml <http://lxml.de/api.html#incremental-xml-generation>`_ with the aim of allowing code to be developed that will work with both libraries.
It was developed initially for the openpyxl project, but is now a standalone module.

The code was written by Elias Rabel as part of the `Python Düsseldorf <http://pyddf.de>`_ openpyxl sprint in September 2014.

Proper support for incremental writing was provided by Daniel Hillier in 2024

Note on performance
-------------------

The code was not developed with performance in mind, but turned out to be faster than the existing SAX-based implementation but is generally slower than lxml's xmlfile.
There is one area where an optimisation for lxml may negatively affect the performance of et_xmfile and that is when using the `.element()` method on the xmlfile context manager. It is, therefore, recommended simply to create Elements write these directly, as in the sample code.
