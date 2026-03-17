multiprocess
============
better multiprocessing and multithreading in Python

About Multiprocess
------------------
``multiprocess`` is a fork of ``multiprocessing``. ``multiprocess`` extends ``multiprocessing`` to provide enhanced serialization, using ``dill``. ``multiprocess`` leverages ``multiprocessing`` to support the spawning of processes using the API of the Python standard library's ``threading`` module. ``multiprocessing`` has been distributed as part of the standard library since Python 2.6.

``multiprocess`` is part of ``pathos``,  a Python framework for heterogeneous computing.
``multiprocess`` is in active development, so any user feedback, bug reports, comments,
or suggestions are highly appreciated.  A list of issues is located at https://github.com/uqfoundation/multiprocess/issues, with a legacy list maintained at https://uqfoundation.github.io/project/pathos/query.


Major Features
--------------
``multiprocess`` enables:

* objects to be transferred between processes using pipes or multi-producer/multi-consumer queues
* objects to be shared between processes using a server process or (for simple data) shared memory

``multiprocess`` provides:

* equivalents of all the synchronization primitives in ``threading``
* a ``Pool`` class to facilitate submitting tasks to worker processes
* enhanced serialization, using ``dill``


Current Release
[![Downloads](https://static.pepy.tech/personalized-badge/multiprocess?period=total&units=international_system&left_color=grey&right_color=blue&left_text=pypi%20downloads)](https://pepy.tech/project/multiprocess)
[![Conda Downloads](https://img.shields.io/conda/dn/conda-forge/multiprocess?color=blue&label=conda%20downloads)](https://anaconda.org/conda-forge/multiprocess)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-get%20help-black.svg)](https://stackoverflow.com/questions/tagged/multiprocess)
---------------
The latest released version of ``multiprocess`` is available from:
    https://pypi.org/project/multiprocess

``multiprocess`` is distributed under a 3-clause BSD license, and is a fork of ``multiprocessing``.


Development Version
[![Support](https://img.shields.io/badge/support-the%20UQ%20Foundation-purple.svg?style=flat&colorA=grey&colorB=purple)](http://www.uqfoundation.org/pages/donate.html)
[![Documentation Status](https://readthedocs.org/projects/multiprocess/badge/?version=latest)](https://multiprocess.readthedocs.io/en/latest/?badge=latest)
[![Build Status](https://app.travis-ci.com/uqfoundation/multiprocess.svg?label=build&logo=travis&branch=master)](https://app.travis-ci.com/github/uqfoundation/multiprocess)
[![codecov](https://codecov.io/gh/uqfoundation/multiprocess/branch/master/graph/badge.svg?token=X9xDo1q1Ol)](https://codecov.io/gh/uqfoundation/multiprocess)
-------------------
You can get the latest development version with all the shiny new features at:
    https://github.com/uqfoundation

If you have a new contribution, please submit a pull request.


Installation
------------
``multiprocess`` can be installed with ``pip``::

    $ pip install multiprocess

For Python 2, a C compiler is required to build the included extension module from source. Python 3 and binary installs do not require a C compiler.


Requirements
------------
``multiprocess`` requires:

* ``python`` (or ``pypy``), **>=3.9**
* ``setuptools``, **>=42**
* ``dill``, **>=0.4.1**


Basic Usage
-----------
The ``multiprocess.Process`` class follows the API of ``threading.Thread``.
For example ::

    from multiprocess import Process, Queue

    def f(q):
        q.put('hello world')

    if __name__ == '__main__':
        q = Queue()
        p = Process(target=f, args=[q])
        p.start()
        print (q.get())
        p.join()

Synchronization primitives like locks, semaphores and conditions are
available, for example ::

    >>> from multiprocess import Condition
    >>> c = Condition()
    >>> print (c)
    <Condition(<RLock(None, 0)>), 0>
    >>> c.acquire()
    True
    >>> print (c)
    <Condition(<RLock(MainProcess, 1)>), 0>

One can also use a manager to create shared objects either in shared
memory or in a server process, for example ::

    >>> from multiprocess import Manager
    >>> manager = Manager()
    >>> l = manager.list(range(10))
    >>> l.reverse()
    >>> print (l)
    [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
    >>> print (repr(l))
    <Proxy[list] object at 0x00E1B3B0>

Tasks can be offloaded to a pool of worker processes in various ways,
for example ::

    >>> from multiprocess import Pool
    >>> def f(x): return x*x
    ...
    >>> p = Pool(4)
    >>> result = p.map_async(f, range(10))
    >>> print (result.get(timeout=1))
    [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

When ``dill`` is installed, serialization is extended to most objects,
for example ::

    >>> from multiprocess import Pool
    >>> p = Pool(4)
    >>> print (p.map(lambda x: (lambda y:y**2)(x) + x, xrange(10)))
    [0, 2, 6, 12, 20, 30, 42, 56, 72, 90]


More Information
----------------
Probably the best way to get started is to look at the documentation at
http://multiprocess.rtfd.io. Also see ``multiprocess.tests`` for scripts that
demonstrate how ``multiprocess`` can be used to leverge multiple processes
to execute Python in parallel. You can run the test suite with
``python -m multiprocess.tests``. As ``multiprocess`` conforms to the
``multiprocessing`` interface, the examples and documentation found at
http://docs.python.org/library/multiprocessing.html also apply to
``multiprocess`` if one will ``import multiprocessing as multiprocess``.
See https://github.com/uqfoundation/multiprocess/tree/master/py3.12/examples
for a set of examples that demonstrate some basic use cases and benchmarking
for running Python code in parallel. Please feel free to submit a ticket on
github, or ask a question on stackoverflow (**@Mike McKerns**). If you would
like to share how you use ``multiprocess`` in your work, please send an email
(to **mmckerns at uqfoundation dot org**).


Citation
--------
If you use ``multiprocess`` to do research that leads to publication, we ask that you
acknowledge use of ``multiprocess`` by citing the following in your publication::

    M.M. McKerns, L. Strand, T. Sullivan, A. Fang, M.A.G. Aivazis,
    "Building a framework for predictive science", Proceedings of
    the 10th Python in Science Conference, 2011;
    http://arxiv.org/pdf/1202.1056

    Michael McKerns and Michael Aivazis,
    "pathos: a framework for heterogeneous computing", 2010- ;
    https://uqfoundation.github.io/project/pathos

Please see https://uqfoundation.github.io/project/pathos or
http://arxiv.org/pdf/1202.1056 for further information.

