# ODFPY

This is a collection of utility programs written in Python to manipulate
OpenDocument 1.2 files.

How to proceed: Each application has its own directory. In there, look
at the manual pages. The Python-based tools need the odf library. Just
make a symbolic link like this: ln -s ../odf odf
... or type: make

For your own use of the odf library, see api-for-odfpy.odt

## INSTALLATION

First you get the package.

    $ git clone https://github.com/eea/odfpy.git

Then you can build and install the library for Python2 and Python3:

```
$ python setup.py build
$ python3 setup.py build
$ su
# python setup.py install
# python3 setup.py install
```
The library is incompatible with PyXML.

## RUNNING TESTS

Install `tox` via `pip` when running the tests for the first time:

```
$ pip install tox
```

Run the tests for all supported python versions:

```
$ tox
```

## REDISTRIBUTION LICENSE

This project, with the exception of the OpenDocument schemas, are
Copyright (C) 2006-2014, Daniel Carrera, Alex Hudson, Søren Roug,
Thomas Zander, Roman Fordinal, Michael Howitz and Georges Khaznadar.

It is distributed under both GNU General Public License v.2 or (at
your option) any later version or APACHE License v.2.
See GPL-LICENSE-2.txt and APACHE-LICENSE-2.0.txt.

The OpenDocument RelaxNG Schemas are Copyright © OASIS Open 2005. See
the schema files for their copyright notice.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 

## TODO / IDEAS

* html2odf
  Alex Hudson has been contracted to produce a command-line html2odf 
  converter. It should include support for images, tables, CSS, etc.
  He will provide a C# version first, and later a C version.

* odf2pdf
  A valuable tool, but one that is hard to do. PDF is an immensely
  popular format, but it's tricky to make PDFs. With an odf2pdf tool
  available, many developers would use ODF purely for the purpose of
  generating a PDF later. The latest idea is to hire KOffice 
  developers and get them to trim down KOffice into a converter.

* pdf2odf
  This conversion is less likely to produce good results, but it 
  might be worth a shot. Poppler is a pdf library that can convert 
  PDF into XML. Maybe we can convert that XML to ODF.
  http://webcvs.freedesktop.org/poppler/poppler/

* odfclean
  A command-line program that removes unused automatic styles, 
  metadata and track-changes. Some companies might like to send all
  out-going files through odfclean to remove any information they
  don't want others to see.

* odf2xliff
  Create XLIFF extraction and merge application. XLIFF is a OASIS file
  for translations. You extract the text strings, send them to the translator
  and then import them. It allows you to work on the document in the
  meantime and only retranslate the changed parts.

* odfdiff
  A program that can generate a diff between two ODF files. Useful for 
  SVN commit messages. This is very difficult to do. But see:
  http://www.manageability.org/blog/stuff/open-source-xml-diff-in-java/view
  http://freshmeat.net/projects/xmldiff/

* odfsign
   Sign and verify the signature(s) of an ODF document.
