XHTML2PDF
=========

.. image:: https://img.shields.io/pypi/v/xhtml2pdf?label=PyPI&logo=PyPI&logoColor=white&color=blue
    :target: https://pypi.python.org/pypi/xhtml2pdf
    :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/xhtml2pdf?label=Python&logo=Python&logoColor=white
    :target: https://www.python.org/downloads
    :alt: Python versions

.. image:: https://img.shields.io/coveralls/github/xhtml2pdf/xhtml2pdf?label=Coveralls&logo=Coveralls&logoColor=white
    :target: https://coveralls.io/github/xhtml2pdf/xhtml2pdf
    :alt: Coveralls

.. image:: https://img.shields.io/readthedocs/xhtml2pdf?label=Read%20the%20Docs&logo=read%20the%20docs&logoColor=white
   :target: http://xhtml2pdf.readthedocs.io/en/latest/?badge=latest
   :alt: Read the Docs

|

Release Notes can be found here: `Release Notes <https://xhtml2pdf.readthedocs.io/en/latest/release-notes.html>`__
As with all open-source software, its use in production depends on many factors, so be aware that you may find issues in some cases.

**Big thanks** to everyone who has worked on this project so far and to those who help maintain it.

About
=====

xhtml2pdf is a HTML to PDF converter using Python, the ReportLab Toolkit, html5lib and pypdf. It supports HTML5 and CSS 2.1 (and some of CSS 3). It is completely written in pure Python, so it is platform independent.

The main benefit of this tool is that a user with web skills like HTML and CSS is able to generate PDF templates very quickly without learning new technologies.

Please consider support this project using `Patreon <https://www.patreon.com/xhtml2pdf>`__ or Bitcoins: ``bc1qmr0skzwx5scyvh2ql28f7gfh6l65ua250qv227``



Documentation
==============

The documentation of xhtml2pdf is available at `Read the Docs <http://xhtml2pdf.readthedocs.io>`__.

And we could use your help improving it! A good place to start is ``doc/source/usage.rst``.


Installation
============

This is a typical Python library and can be installed using pip::

    pip install xhtml2pdf


Requirements
============

Only Python 3.8+ is tested and guaranteed to work.

All mandatory requirements are listed in the ``pyproject.toml`` file and are installed automatically using the ``pip install xhtml2pdf`` method.

As PDF library we depend on reportlab, which needs a rendering backend to generate bitmaps and vector graphic formats.
For more information about this, have a look at the `reportlab docs <https://docs.reportlab.com/install/open_source_installation/>`__.

The recommended choice is the `cairo graphics library <https://cairographics.org/>`__ which has to be installed system-wide e.g. via the OS package manager
in combination with the ``PyCairo`` extra dependency:

    pip install xhtml2pdf[pycairo]

Alternatively, the legacy ``RenderPM`` can be used by installing:

    pip install xhtml2pdf[renderpm]


Alternatives
============

You can try `WeasyPrint <http://weasyprint.org>`__. The codebase is pretty, it has different features and it does a lot of what xhtml2pdf does.


Call for testing
================

This project is heavily dependent on getting its test coverage up! Furthermore, parts of the codebase could do well with cleanups and refactoring.

If you benefit from xhtml2pdf, perhaps look at the `test coverage <https://coveralls.io/github/xhtml2pdf/xhtml2pdf>`__ and identify parts that are yet untouched.


Development environment
=======================

#. If you don't have it, install ``pip``, the python package installer::

    sudo easy_install pip

   For more information about ``pip`` refer to http://www.pip-installer.org

#. We will recommend using ``venv`` for development.

#. Create a virtual environment for the project. This can be inside the project directory, but cannot be under version control::

    python -m venv .venv

#. Activate your virtual environment::

    source .venv/bin/activate

   Later to deactivate it use::

    deactivate

#. The next step will be to install/upgrade dependencies from the ``pyproject.toml`` file::

    pip install -e .[test,docs,build]

#. Run tests to check your configuration::

    tox

   You should have a log with the following success status::

    congratulations :) (75.67 seconds)


Python integration
==================

Some simple demos of how to integrate xhtml2pdf into a Python program may be found here: ``test/simple.py``


Running tests
=============

Two different test suites are available to assert that xhtml2pdf works reliably:

#. Unit tests. The unit testing framework is currently minimal, but is being
   improved on a regular basis (contributions welcome). They should run in the
   expected way for Python's unittest module, i.e.::

        tox

#. Functional tests. Thanks to mawe42's super cool work, a full functional
   test suite is available at ``testrender/``.

You can run them using make

.. code:: bash

        make test       # run tests
        make test-ref   # generate reference data for testrender
        make test-all   # Run all test using tox

Contact
=======

This project is community-led! Feel free to open up issues on GitHub about new ideas to improve xhtml2pdf.


History
=======

These are the major milestones and the maintainers of the project:

* 2000-2007 Dirk Holtwick (commercial project of spirito.de)
* 2007-2010 Dirk Holtwick (project named "pisa", project released as GPL)
* 2010-2012 Dirk Holtwick (project named "xhtml2pdf", changed license to Apache)
* 2012-2015 Chris Glass (@chrisglass)
* 2015-2016 Benjamin Bach (@benjaoming)
* 2016-2018 Sam Spencer (@LegoStormtroopr)
* 2018-Current Luis Zarate (@luisza)

For more history, see the `Release Notes <https://xhtml2pdf.readthedocs.io/en/latest/release-notes.html>`__.

License
=======

Copyright 2010 Dirk Holtwick, holtwick.it

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
