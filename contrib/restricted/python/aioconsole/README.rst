aioconsole
==========

|docs-badge| |cov-badge| |ci-badge| |version-badge| |pyversion-badge|


Asynchronous console and interfaces for asyncio

aioconsole_ provides:

* asynchronous equivalents to `input`_, `print`_, `exec`_ and `code.interact`_
* an interactive loop running the asynchronous python console
* a way to customize and run command line interface using `argparse`_
* `stream`_ support to serve interfaces instead of using standard streams
* the ``apython`` script to access asyncio code at runtime without modifying the sources

⚠️ Limitations
--------------

Better alternative to ``aioconsole.ainput()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A common use case for aioconsole is the async alternative to the builtin `input`_ function.
However, aioconsole_ was written in 2015 and since then the powerful prompt-toolkit_ library has gained better asyncio support.
The recommended way to `prompt in an asyncio application <https://python-prompt-toolkit.readthedocs.io/en/master/pages/asking_for_input.html#prompt-in-an-asyncio-application>`_ is now to use the `prompt-toolkit`_ library:

.. code:: python3

   from prompt_toolkit import PromptSession
   from prompt_toolkit.patch_stdout import patch_stdout

   async def my_coroutine():
      session = PromptSession()
      while True:
         with patch_stdout():
               result = await session.prompt_async("Say something: ")
         print(f"You said: {result}")


Better python consoles with async support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The python console exposed by `aioconsole`_ is quite limited compared to modern consoles such as `IPython`_ or `ptpython`_. Luckily, those projects gained greater asyncio support over the years. In particular, the following use cases overlap with `aioconsole`_ capabilities:

- `Embedding a ptpython console in an asyncio program <https://github.com/prompt-toolkit/ptpython/blob/master/examples/asyncio-python-embed.py>`_
- `Using the await syntax in an IPython console <https://ipython.readthedocs.io/en/stable/whatsnew/version7.html#autowait-asynchronous-repl>`_


Requirements
------------

*  Python \>= 3.8


Installation
------------

aioconsole_ is available on PyPI_ and GitHub_.
Both of the following commands install the ``aioconsole`` package
and the ``apython`` script.

.. code:: console

    $ pip3 install aioconsole   # from PyPI
    $ python3 setup.py install  # or from the sources
    $ apython -h
    usage: apython [-h] [--serve [HOST:] PORT] [--no-readline]
                   [--banner BANNER] [--locals LOCALS]
                   [-m MODULE | FILE] ...

    Run the given python file or module with a modified asyncio policy replacing
    the default event loop with an interactive loop. If no argument is given, it
    simply runs an asynchronous python console.

    positional arguments:
      FILE                  python file to run
      ARGS                  extra arguments

    optional arguments:
      -h, --help            show this help message and exit
      --serve [HOST:] PORT, -s [HOST:] PORT
                            serve a console on the given interface instead
      --no-readline         force readline disabling
      --banner BANNER       provide a custom banner
      --locals LOCALS       provide custom locals as a dictionary
      -m MODULE             run a python module



Simple usage
------------

The following example demonstrates the use of ``await`` inside the console:

.. code:: console

    $ apython
    Python 3.5.0 (default, Sep 7 2015, 14:12:03)
    [GCC 4.8.4] on linux
    Type "help", "copyright", "credits" or "license" for more information.
    ---
    This console is running in an asyncio event loop.
    It allows you to wait for coroutines using the 'await' syntax.
    Try: await asyncio.sleep(1, result=3, loop=loop)
    ---

.. code:: python3

    >>> await asyncio.sleep(1, result=3)
    # Wait one second...
    3
    >>>


Documentation
-------------

Find more examples in the documentation_ and the `example directory`_.


Contact
-------

Vincent Michel: vxgmichel@gmail.com

.. _aioconsole: https://pypi.python.org/pypi/aioconsole
.. _GitHub: https://github.com/vxgmichel/aioconsole
.. _input: https://docs.python.org/3/library/functions.html#input
.. _print: https://docs.python.org/3/library/functions.html#print
.. _exec: https://docs.python.org/3/library/functions.html#exec
.. _code.interact: https://docs.python.org/3/library/code.html#code.interact
.. _argparse: https://docs.python.org/dev/library/argparse.html
.. _stream: https://docs.python.org/3/library/asyncio-stream.html
.. _example directory: https://github.com/vxgmichel/aioconsole/blob/main/example
.. _documentation: http://aioconsole.readthedocs.io/
.. _PyPI: aioconsole_
.. _IPython: https://ipython.readthedocs.io
.. _ptpython: https://github.com/prompt-toolkit/ptpython
.. _prompt-toolkit: https://python-prompt-toolkit.readthedocs.io

.. |docs-badge| image:: https://readthedocs.org/projects/aioconsole/badge/?version=latest
   :target: http://aioconsole.readthedocs.io/
   :alt:

.. |ci-badge| image:: https://github.com/vxgmichel/aioconsole/workflows/CI/badge.svg
   :target: https://github.com/vxgmichel/aioconsole/actions?query=branch%3Amain
   :alt:

.. |cov-badge| image:: https://codecov.io/gh/vxgmichel/aioconsole/branch/main/graph/badge.svg
   :target: https://codecov.io/gh/vxgmichel/aioconsole
   :alt:

.. |version-badge| image:: https://img.shields.io/pypi/v/aioconsole.svg
   :target: https://pypi.python.org/pypi/aioconsole
   :alt:

.. |pyversion-badge| image:: https://img.shields.io/pypi/pyversions/aioconsole.svg
   :target: https://pypi.python.org/pypi/aioconsole
   :alt:
