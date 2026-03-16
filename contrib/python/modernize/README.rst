::

    Python           _              _        
       _ __  ___  __| |___ _ _ _ _ (_)______ 
      | '  \/ _ \/ _` / -_) '_| ' \| |_ / -_)
      |_|_|_\___/\__,_\___|_| |_||_|_/__\___|

This library is a very thin wrapper around `fissix
<https://github.com/jreese/fissix>`_, a fork of lib2to3, to utilize it
to make Python 2 code more modern with the intention of eventually
porting it over to Python 3.

The ``python -m modernize`` command works like
``python -m fissix``, see `fissix <https://github.com/jreese/fissix>`_.
Here's how you'd rewrite a
single file::

    python -m modernize -w example.py

It does not guarantee, but it attempts to spit out a codebase compatible
with Python 2.6+ or Python 3. The code that it generates has a runtime
dependency on `six <https://pypi.python.org/pypi/six>`_, unless the
``--no-six`` option is used. Version 1.9.0 or later of ``six`` is
recommended. Some of the fixers output code that is not compatible with
Python 2.5 or lower.

**Documentation:** `modernize.readthedocs.io
<https://modernize.readthedocs.io/>`_.

See the ``LICENSE`` file for the license of ``modernize``.
Using this tool does not affect licensing of the modernized code.

.. image:: https://readthedocs.org/projects/modernize/badge/
    :target: https://readthedocs.org/projects/modernize/?badge=latest
    :alt: Documentation Status

.. image:: https://api.travis-ci.org/pycqa/modernize.svg?branch=master
    :target: https://travis-ci.org/pycqa/modernize

.. image:: https://coveralls.io/repos/pycqa/modernize/badge.png?branch=master
    :target: https://coveralls.io/r/pycqa/modernize?branch=master
