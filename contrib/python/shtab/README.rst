|Logo|

shtab
=====

|PyPI-Downloads| |Tests| |Coverage| |PyPI| |Conda|

- What: Automatically generate shell tab completion scripts for Python CLI apps
- Why: Speed & correctness. Alternatives like
  `argcomplete <https://pypi.org/project/argcomplete>`_ and
  `pyzshcomplete <https://pypi.org/project/pyzshcomplete>`_ are slow and have
  side-effects
- How: ``shtab`` processes an ``argparse.ArgumentParser`` object to generate a
  tab completion script for your shell

Features
--------

- Outputs tab completion scripts for

  - ``bash``
  - ``zsh``
  - ``tcsh``

- Supports

  - `argparse <https://docs.python.org/library/argparse>`_
  - `docopt <https://pypi.org/project/docopt>`_ (via `argopt <https://pypi.org/project/argopt>`_)

- Supports arguments, options and subparsers
- Supports choices (e.g. ``--say={hello,goodbye}``)
- Supports file and directory path completion
- Supports custom path completion (e.g. ``--file={*.txt}``)

------------------------------------------

.. contents:: Table of Contents
   :backlinks: top

Installation
------------

Choose one of:

- ``pip install shtab``, or
- ``conda install -c conda-forge shtab``

See `operating system-specific instructions in the docs <https://docs.iterative.ai/shtab/#installation>`_.

Usage
-----

There are two ways of using ``shtab``:

- `CLI Usage <https://docs.iterative.ai/shtab/use/#cli-usage>`_: ``shtab``'s own CLI interface for external applications

  - may not require any code modifications whatsoever
  - end-users execute ``shtab your_cli_app.your_parser_object``

- `Library Usage <https://docs.iterative.ai/shtab/use/#library-usage>`_: as a library integrated into your CLI application

  - adds a couple of lines to your application
  - argument mode: end-users execute ``your_cli_app --print-completion {bash,zsh,tcsh}``
  - subparser mode: end-users execute ``your_cli_app completion {bash,zsh,tcsh}``

Examples
--------

See `the docs for usage examples <https://docs.iterative.ai/shtab/use/#main.py>`_.

FAQs
----

Not working? Check out `frequently asked questions <https://docs.iterative.ai/shtab/#faqs>`_.

Alternatives
------------

- `argcomplete <https://pypi.org/project/argcomplete>`_

  - executes the underlying script *every* time ``<TAB>`` is pressed (slow and
    has side-effects)

- `pyzshcomplete <https://pypi.org/project/pyzshcomplete>`_

  - executes the underlying script *every* time ``<TAB>`` is pressed (slow and
    has side-effects)
  - only provides ``zsh`` completion

- `click <https://pypi.org/project/click>`_

  - different framework completely replacing the builtin ``argparse``
  - solves multiple problems (rather than POSIX-style "do one thing well")

Contributions
-------------

Please do open `issues <https://github.com/iterative/shtab/issues>`_ & `pull requests <https://github.com/iterative/shtab/pulls>`_! Some ideas:

- support ``fish`` (`#174 <https://github.com/iterative/shtab/pull/174>`_)
- support ``powershell``

See
`CONTRIBUTING.md <https://github.com/iterative/shtab/tree/main/CONTRIBUTING.md>`_
for more guidance.

|Hits|

.. |Logo| image:: https://github.com/iterative/shtab/raw/main/meta/logo.png
.. |Tests| image:: https://img.shields.io/github/actions/workflow/status/iterative/shtab/test.yml?logo=github&label=tests
   :target: https://github.com/iterative/shtab/actions
   :alt: Tests
.. |Coverage| image:: https://codecov.io/gh/iterative/shtab/branch/main/graph/badge.svg
   :target: https://codecov.io/gh/iterative/shtab
   :alt: Coverage
.. |Conda| image:: https://img.shields.io/conda/v/conda-forge/shtab.svg?label=conda&logo=conda-forge
   :target: https://anaconda.org/conda-forge/shtab
   :alt: conda-forge
.. |PyPI| image:: https://img.shields.io/pypi/v/shtab.svg?label=pip&logo=PyPI&logoColor=white
   :target: https://pypi.org/project/shtab
   :alt: PyPI
.. |PyPI-Downloads| image:: https://img.shields.io/pypi/dm/shtab.svg?label=pypi%20downloads&logo=PyPI&logoColor=white
   :target: https://pepy.tech/project/shtab
   :alt: Downloads
.. |Hits| image:: https://cgi.cdcl.ml/hits?q=shtab&style=social&r=https://github.com/iterative/shtab&a=hidden
   :target: https://cgi.cdcl.ml/hits?q=shtab&a=plot&r=https://github.com/iterative/shtab&style=social
   :alt: Hits
