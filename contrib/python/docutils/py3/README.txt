.. include:: docs/header0.txt

=========================
 README: Docutils 0.21.2
=========================

:Author: David Goodger
:Contact: goodger@python.org
:Date: $Date: 2024-04-23 20:54:26 +0200 (Di, 23. Apr 2024) $
:Web site: https://docutils.sourceforge.io/
:Copyright: This document has been placed in the public domain.

.. contents::


Quick-Start
===========

This is for those who want to get up & running quickly.

1. Docutils requires **Python**, available from
   https://www.python.org/.
   See Dependencies_ below for details.

2. Install the latest stable release from PyPi with pip_::

       pip install docutils

   For alternatives and details, see section `Installation`_ below.

3. Use the `front-end scripts`_ to convert reStructuredText documents.
   Try for example::

       docutils FAQ.txt FAQ.html

   See Usage_ below for details.


Purpose
=======

The purpose of the Docutils project is to provide a set of tools for
processing plaintext documentation into useful formats, such as HTML,
LaTeX, troff (man pages), OpenOffice, and native XML.  Support for the
following sources has been implemented:

* Standalone files.

* `PEPs (Python Enhancement Proposals)`_.

Support for the following sources is planned or provided by
`third party tools`_:

* Inline documentation from Python modules and packages, extracted
  with namespace context.

* Email (RFC-822 headers, quoted excerpts, signatures, MIME parts).

* Wikis, with global reference lookups of "wiki links".

* Compound documents, such as multiple chapter files merged into a
  book.

* And others as discovered.

.. _PEPs (Python Enhancement Proposals):
   https://peps.python.org/pep-0012
.. _third party tools: docs/user/links.html#related-applications


Dependencies
============

To run the code, Python_ must be installed.
(Python is pre-installed with most Linux distributions.)

* Since version 0.21, Docutils requires Python 3.9 or later.
* Docutils versions 0.19 to 0.20.1 require Python 3.7 or later.
* Docutils versions 0.16 to 0.18 require Python 2.7 or 3.5+.

.. _Python: https://www.python.org/.


Recommendations
---------------

Docutils uses the following packages for enhanced functionality, if they
are installed:

* The recommended installer is pip_, setuptools_ works, too.

* The `Python Imaging Library`_ (PIL) is used for some image
  manipulation operations.

* The `Pygments`_ package provides syntax highlight of "code" directives
  and roles.

* The `myst`_, `pycmark`_, or `recommonmark`_ parsers can be used to
  parse input in "Markdown" (CommonMark_) format.

The `Docutils Link List <docs/user/links.html>`__ records projects that
users of Docutils and reStructuredText may find useful.

.. _pip: https://pypi.org/project/pip/
.. _setuptools: https://pypi.org/project/setuptools/
.. _Python Imaging Library: http://www.pythonware.com/products/pil/
.. _Pygments: https://pypi.org/project/Pygments/
.. _myst: https://pypi.org/project/myst-docutils/
.. _pycmark: https://pypi.org/project/pycmark/
.. _recommonmark: https://github.com/rtfd/recommonmark
.. _CommonMark: https://spec.commonmark.org/0.30/


Installation
============

The `Python Packaging User Guide`_ gives details how to
`use pip for installing`_.

* The simplest way is to install the latest *stable release* from PyPi::

      pip install docutils

* To install a *pre-relase*, append the option ``--pre``.

* To install a `development version`_ *from source*:

  1. Open a shell

  2. Go to the directory containing the file ``setup.py``.

  3. Install the package with **one** of the following commands::

         pip install -e .  # editable install
         pip install .     # regular install

     or do a `"manual" install`_.

  4. Optional steps:

     * `Running the test suite`_
     * `Converting the documentation`_

  See also the OS-specific installation instructions below and
  the `Docutils version repository`_ documentation.

* To install for a *specific Python version*, use this version in the
  setup call, e.g. ::

       python3.11 -m pip install docutils

  If the python executable isn't on your path, you'll have to specify the
  complete path, such as ``/usr/local/bin/python3.11``.

  To install for different Python versions, repeat step 3 for every
  required version. The last installed version will be used for the
  ``docutils`` command line application.

.. _Python Packaging User Guide: https://packaging.python.org/en/latest/
.. _use pip for installing:
    https://packaging.python.org/en/latest/tutorials/installing-packages/
    #use-pip-for-installing
.. _"editable" install:
    https://pip.pypa.io/en/stable/topics/local-project-installs/
    #editable-installs
.. _"manual" install: docs/dev/repository.html#manual-install


GNU/Linux, BSDs, Unix, Mac OS X, etc.
-------------------------------------

* Use ``su`` or ``sudo`` for a system-wide
  installation as ``root``, e.g.::

      sudo pip install docutils


Windows
-------

* The Python FAQ explains `how to run a Python program under Windows`__.

  __ https://docs.python.org/3/faq/windows.html
     #how-do-i-run-a-python-program-under-windows

* Usually, pip_ is automatically installed if you are using Python
  downloaded from https://python.org. If not, see the
  `pip documentation <https://pip.pypa.io/en/stable/installation/>`__.

* The command window should recognise the word ``py`` as an instruction to
  start the interpreter, e.g.

       py -m pip install docutils

  If this does not work, you may have to specify the full path to the
  Python executable.


Usage
=====

Start the "docutils" command line application with::

    docutils [options] [<source> [<destination>]]

The default action is to convert a reStructuredText_ document to HTML5,
for example::

    docutils test.rst test.html

Read the ``--help`` option output for details on options and arguments and
`Docutils Front-End Tools`_ for the full documentation of the various tools.

For programmatic use of the `docutils` Python package, read the
`API Reference Material`_ and the source code.
Remaining questions may be answered in the `Docutils Project
Documentation`_ or the Docutils-users_ mailing list.

Contributions are welcome!

.. _reStructuredText: https://docutils.sourceforge.io/rst.html
.. _front-end scripts:
.. _Docutils Front-End Tools: docs/user/tools.html
.. _API Reference Material: /docs/index.html
                            #api-reference-material-for-client-developers
.. _Docutils Project Documentation: /docs/index.html


Project Files & Directories
===========================

* README.txt: You're reading it.

* COPYING.txt: Public Domain Dedication and copyright details for
  non-public-domain files (most are PD).

* FAQ.txt: Frequently Asked Questions (with answers!).

* RELEASE-NOTES.txt: Summary of the major changes in recent releases.

* HISTORY.txt: A detailed change log, for the current and all previous
  project releases.

* BUGS.txt: Known bugs, and how to report a bug.

* THANKS.txt: List of contributors.

* setup.py: Installation script.  See "Installation" below.

* docutils: The project source directory, installed as a Python
  package.

* docs: The project documentation directory.  Read ``docs/index.txt``
  for an overview.

* docs/user: The project user documentation directory.  Contains the
  following documents, among others:

  - docs/user/tools.txt: Docutils Front-End Tools
  - docs/user/latex.txt: Docutils LaTeX Writer
  - docs/user/rst/quickstart.txt: A ReStructuredText Primer
  - docs/user/rst/quickref.html: Quick reStructuredText (HTML only)

* docs/ref: The project reference directory.
  ``docs/ref/rst/restructuredtext.txt`` is the reStructuredText
  reference.

* licenses: Directory containing copies of license files for
  non-public-domain files.

* tools: Directory for Docutils front-end tools.  See
  ``docs/user/tools.txt`` for documentation.

* test: Unit tests.  Not required to use the software, but very useful
  if you're planning to modify it.  See `Running the Test Suite`_
  below.


Development version
===================

While we are trying to follow a "release early & often" policy,
features are added frequently.
We recommend using a current snapshot or a working copy of the repository.

Repository check-out:
  To keep up to date on the latest developments,
  use a `working copy`__ of the `Docutils version repository`_.

Snapshots:
  To get a repository _`snapshot`, go to
  https://sourceforge.net/p/docutils/code/HEAD/tree/trunk/docutils/
  and click the download snapshot button.

  Unpack in a temporary directory,
  **not** directly in Python's ``site-packages``.

Continue with the `Installation`_ instructions below.

__ docs/dev/repository.html#checking-out-the-repository
.. _Docutils version repository: docs/dev/repository.html
.. _sandbox: https://docutils.sourceforge.io/sandbox/README.html


Converting the documentation
============================

After unpacking and installing the Docutils package, the following
shell commands will generate HTML for all included documentation::

    cd <archive_directory_path>
    tools/buildhtml.py .

On Windows systems, type::

    cd <archive_directory_path>
    py tools\buildhtml.py ..

The final directory name of the ``<archive_directory_path>`` is
"docutils" for snapshots.  For official releases, the directory may be
called "docutils-X.Y.Z", where "X.Y.Z" is the release version.

Some files may generate system messages (warnings and errors).  The
``docs/user/rst/demo.txt`` file (under the archive directory) contains
five intentional errors.  (They test the error reporting mechanism!)


Running the Test Suite
======================

The test suite is documented in `Docutils Testing`_ (docs/dev/testing.txt).

To run the entire test suite, open a shell and use the following
commands::

    cd <archive_directory_path>/test
    ./alltests.py

Under Windows, type::

    cd <archive_directory_path>\test
    python alltests.py


You should see a long line of periods, one for each test, and then a
summary like this::

    Ran 1744 tests in 5.859s

    OK (skipped=1)
    Elapsed time: 6.235 seconds

The number of tests will grow over time, and the times reported will
depend on the computer running the tests.
Some test are skipped, if optional dependencies (`recommendations`_)
are missing.
The difference between the two times represents the time required to set
up the tests (import modules, create data structures, etc.).

A copy of the test output is written to the file ``alltests.out``.

If any of the tests fail, please `open a bug report`_ or `send an email`_
(see `Bugs <BUGS.html>`_).
Please include all relevant output, information about your operating
system, Python version, and Docutils version.  To see the Docutils
version, look at the test output or use ::

    docutils --version

.. _Docutils Testing: https://docutils.sourceforge.io/docs/dev/testing.html
.. _open a bug report:
   https://sourceforge.net/p/docutils/bugs/
.. _send an email: mailto:docutils-users@lists.sourceforge.net
   ?subject=Test%20suite%20failure
.. _web interface: https://sourceforge.net/p/docutils/mailman/


Getting Help
============

All documentation can be reached from the `Project Documentation
Overview`_.

The SourceForge `project page`_ has links to the tracker, mailing
lists, and code repository.

If you have further questions or need assistance with Docutils or
reStructuredText, please post a message to the Docutils-users_ mailing
list.

.. _Project Documentation Overview: docs/index.html
.. _project page: https://sourceforge.net/p/docutils
.. _Docutils-users: docs/user/mailing-lists.html#docutils-users


..
   Local Variables:
   mode: indented-text
   indent-tabs-mode: nil
   sentence-end-double-space: t
   fill-column: 70
   End:
