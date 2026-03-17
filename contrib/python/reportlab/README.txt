=====================================
README
=====================================

(C) Copyright ReportLab Europe Ltd. 2000-2023.
See ``LICENSE.txt`` for license details.

This is the ReportLab PDF Toolkit. It allows rapid creation
of rich PDF documents, and also creation of charts in a variety
of bitmap and vector formats.

This library is also the foundation for our commercial product
Report Markup Language (RML), available in the ReportLab PLUS
package (see https://docs.reportlab.com/rmlfornewbies/). 
RML offers many more features, a template-based style
of document development familiar to all web developers, and
higher development productivity.  Please consider trying out
RML for your project, as the license sales support our open
source development.

Contents of this file:

1. Licensing

2. Installation

   2.1 Source Distribution or Subversion

   2.2 Manual Installation without C Compiler (e.g. Windows)

   2.3 Windows packages

   2.4 Mac OSX installation

   2.5 Ubuntu and other Debian-based Systems

3. Prerequisites / Dependencies

4. Documentation

5. Acknowledgements and Thanks


1. Licensing
============
BSD license.  See ``LICENSE.txt`` for details.


2. Installation
===============

In most cases, ``pip install reportlab`` will do the job.  
Full details follow below for each platform if you want to build from source.


2.1 General prerequisites
--------------------------
ReportLab requires Python 3.7 or higher.  

From version 4.0 onwards we are "c-less" - maintaining extensions are now 
Somebody Else's Problem.

To include images in PDFs (other than JPG, which PDF natively supports), we depend on `pillow`.
To render bitmaps with correct text metrics, we depend on three other python packages:`pyCairo` and `freetype-py`.

These should be included in the wheels, or you can build from source


2.2. Where to get the code
------------------------------------------
Latest builds are available from ReportLab's
open source download area::

    https://www.reportlab.com/pypi/

Main releases are also available from the Python Package Index:

    http://pypi.python.org/

The code is currently hosted in Mercurial at
https://hg.reportlab.com/hg-public/.

You can obtain the latest code from our Mercurial repository with::

    hg clone https://hg.reportlab.com/hg-public/reportlab

A mirror only repository is available for git users at

	https://github.com/MrBitBucket/reportlab-mirror

Please do not use this for issue reporting etc; use the mail list at

    https://pairlist2.pair.net/mailman/listinfo/reportlab-users

Users of our commercial libraries, and/or anyone who registers on our site,
can also access our commercial area which has exactly the same packages,
paired with the matching commercial ones (rlextra); it is important to keep
both in sync.



3. Documentation
================
The latest documentation is available at https://docs.reportlab.com/

For over 20 years we generated our own manuals using the library.
In a 'built' distribution, they may already be present in the
docs/ directory.  If not, execute ``python genAll.py`` in
that directory, and it will create three PDF manuals::

    reportlab-userguide.pdf
    reportlab-reference.pdf
    reportlab-graphics-reference.pdf

These are also available in daily build form from the documentation
page on our web site. The manuals are very useful 'how-to' examples
if you are aiming to create long documents.  However, they will gradually
become out of date.

4. Test suite
=============
Tests are in the ``tests/`` directory.  They can be executed by cd'ing into the
directory and executing ``python runAll.py``.

The tests will simply try to 'import reportlab'.  Be warned that if you are not in a virtual environment, and you already have a copy of reportlab installed (which happens by default in Ubuntu 12.04 desktop), it may try to run the installed reportlab and cause permission errors as it can't generate PDF files without sudo rights.

The tests mostly produce output files with the same name as the test, but extension
.pdf.  It is worth reviewing the list of test scripts as they provide valuable 'how
to' information.


6. Demos
========
A small number of demo programs are included in ``demos/`` in the source distribution, 
none of which are particularly exciting, but which may have some intructional value.  
These were the first programs we wrote back in 2000.

The *odyssey* demo serves as our benchmark suite.  If you download the full Odyssey text,
you can generate a PDF of Homer's Odyssey with either (a) no wrapping, (b) simple paragraphs
or (c) paragraphs with enough artificial markup (bold/italic on certain words) to exercise
the parser.


7. Acknowledgements and Thanks
==============================
``lib/normalDate.py`` originally by Jeff Bauer.

Many, many contributors have helped out between 2000 and 2016.
We try to keep a list in the first chapter of the User Guide; if you
have contributed and are not listed there, please let us know.
