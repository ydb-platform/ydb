flake8-docstrings
=================

A simple module that adds an extension for the fantastic pydocstyle_ tool to
flake8_.

Simply install this extension::

    pip install flake8-docstrings

and run flake8.

You can set the pydocstyle convention_ at the command line using::

    $ flake8 --docstring-convention numpy ...

Or, adding ``docstring-convention=numpy`` to your flake8 configuration file.
The available set of conventions depends on the version of pydocstyle installed.
The default is ``pep257``, pydocstyle v2.0.0 added ``numpy`` (for the numpydoc
standard), while pydocstyle v4.0.0 added ``google``.

In order to choose a custom list of error codes, use the special value
``docstring-convention=all``, then choose the codes you want checked using
flake8_'s built-in ``--ignore``/``--select`` functionality.

Report any issues on our `bug tracker`_.

.. _pydocstyle: https://github.com/pycqa/pydocstyle
.. _flake8: https://github.com/pycqa/flake8
.. _convention: http://www.pydocstyle.org/en/latest/error_codes.html#default-conventions
.. _bug tracker: https://github.com/pycqa/flake8-docstrings/issues
