The `debian` Python modules work with Debian-related data formats,
providing a means to read data from files involved in Debian packaging,
and the distribution of Debian packages. The ability to create or edit
the files is also available for some formats.

Currently supported are:

  * Debtags information (`debian.debtags` module)
  * debian/changelog (`debian.changelog` module)
  * Packages files, pdiffs (`debian.debian_support` module)
  * Control files of single or multiple RFC822-style paragraphs, e.g.
    debian/control, .changes, .dsc, Packages, Sources, Release, etc.
    (`debian.deb822` module)
  * Raw .deb and .ar files, with (read-only) access to contained
    files and meta-information (`debian.debfile` module)

`API documentation`_, can be found online and throughout the code. There
are examples both within the code and in the examples_ directory.

.. _API documentation: https://python-debian-team.pages.debian.net/python-debian/html/

.. _examples: https://salsa.debian.org/python-debian-team/python-debian/tree/master/examples

Note that some modules can use `python-apt`_ to speed up processing.

.. _python-apt: https://packages.debian.org/unstable/python3-apt


Contributions to `python-debian` are most welcome, including expansion of the
module's capabilities. If you have a module that is for manipulation or
interrogation of Debian specific data then consider adding it to this package.
Please discuss your ideas on the `mailing list`_,
make merge requests via the `salsa repository`_,
and see the Contributing section of the documentation.

.. _mailing list: mailto:pkg-python-debian-maint@lists.alioth.debian.org

.. _salsa repository: https://salsa.debian.org/python-debian-team/python-debian
