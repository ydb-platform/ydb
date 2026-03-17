"""
Line Profiler
=============

The line_profiler module for doing line-by-line profiling of functions

+---------------+--------------------------------------------+
| Github        | https://github.com/pyutils/line_profiler   |
+---------------+--------------------------------------------+
| Pypi          | https://pypi.org/project/line_profiler     |
+---------------+--------------------------------------------+
| ReadTheDocs   | https://kernprof.readthedocs.io/en/latest/ |
+---------------+--------------------------------------------+


Installation
============

Releases of :py:mod:`line_profiler` and :py:mod:`kernprof` can be installed
using pip

.. code:: bash

    pip install line_profiler


The package also provides extras for optional dependencies, which can be
installed via:


.. code:: bash

    pip install line_profiler[all]


Line Profiler Basic Usage
=========================

To demonstrate line profiling, we first need to generate a Python script to
profile. Write the following code to a file called ``demo_primes.py``.

.. code:: python

    from line_profiler import profile


    @profile
    def is_prime(n):
        '''
        Check if the number "n" is prime, with n > 1.

        Returns a boolean, True if n is prime.
        '''
        max_val = n ** 0.5
        stop = int(max_val + 1)
        for i in range(2, stop):
            if n % i == 0:
                return False
        return True


    @profile
    def find_primes(size):
        primes = []
        for n in range(size):
            flag = is_prime(n)
            if flag:
                primes.append(n)
        return primes


    @profile
    def main():
        print('start calculating')
        primes = find_primes(100000)
        print(f'done calculating. Found {len(primes)} primes.')


    if __name__ == '__main__':
        main()


In this script we explicitly import the ``profile`` function from
``line_profiler``, and then we decorate function of interest with ``@profile``.

By default nothing is profiled when running the script.

.. code:: bash

    python demo_primes.py


The output will be

.. code::

    start calculating
    done calculating. Found 9594 primes.


The quickest way to enable profiling is to set the environment variable
``LINE_PROFILE=1`` and running your script as normal.

.... todo: add a link that points to docs showing all the different ways to enable profiling.


.. code:: bash

    LINE_PROFILE=1 python demo_primes.py

This will output 3 files: profile_output.txt, profile_output_<timestamp>.txt,
and profile_output.lprof and stdout will look something like:


.. code::

    start calculating
    done calculating. Found 9594 primes.
    Timer unit: 1e-09 s

      0.65 seconds - demo_primes.py:4 - is_prime
      1.47 seconds - demo_primes.py:19 - find_primes
      1.51 seconds - demo_primes.py:29 - main
    Wrote profile results to profile_output.txt
    Wrote profile results to profile_output_2023-08-12T193302.txt
    Wrote profile results to profile_output.lprof
    To view details run:
    python -m line_profiler -rtmz profile_output.lprof


The details contained in the output txt files or by running the script provided
in the output will show detailed line-by-line timing information for each
decorated function.


.. code::

    Timer unit: 1e-06 s

    Total time: 0.731624 s
    File: ./demo_primes.py
    Function: is_prime at line 4

    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
         4                                           @profile
         5                                           def is_prime(n):
         6                                               '''
         7                                               Check if the number "n" is prime, with n > 1.
         8
         9                                               Returns a boolean, True if n is prime.
        10                                               '''
        11    100000      14178.0      0.1      1.9      max_val = n ** 0.5
        12    100000      22830.7      0.2      3.1      stop = int(max_val + 1)
        13   2755287     313514.1      0.1     42.9      for i in range(2, stop):
        14   2745693     368716.6      0.1     50.4          if n % i == 0:
        15     90406      11462.9      0.1      1.6              return False
        16      9594        922.0      0.1      0.1      return True


    Total time: 1.56771 s
    File: ./demo_primes.py
    Function: find_primes at line 19

    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
        19                                           @profile
        20                                           def find_primes(size):
        21         1          0.2      0.2      0.0      primes = []
        22    100001      10280.4      0.1      0.7      for n in range(size):
        23    100000    1544196.6     15.4     98.5          flag = is_prime(n)
        24    100000      11375.4      0.1      0.7          if flag:
        25      9594       1853.2      0.2      0.1              primes.append(n)
        26         1          0.1      0.1      0.0      return primes


    Total time: 1.60483 s
    File: ./demo_primes.py
    Function: main at line 29

    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
        29                                           @profile
        30                                           def main():
        31         1         14.0     14.0      0.0      print('start calculating')
        32         1    1604795.1    2e+06    100.0      primes = find_primes(100000)
        33         1         20.6     20.6      0.0      print(f'done calculating. Found {len(primes)} primes.')


See Also:

    * autoprofiling usage in: :py:mod:`line_profiler.autoprofile`

Limitations
===========

Line profiling does have limitations, and it is important to be aware of them.
Profiling multi-threaded, multi-processing, and asynchronous code may produce
unexpected or no results. All profiling also adds some amount of overhead to
the runtime, which may influence which parts of the code become bottlenecks.

Line profiler only measures the time between the start and end of a Python
call, so for benchmarking GPU code (e.g. with torch), which have asynchronous
or delayed behavior, it will only show the time to sync blocking calls in the
main thread.

Other profilers have different limitations and different trade-offs. It's good
to be aware of the right tool for the job. Here is a short list of other
profiling tools:


* `Scalene <https://github.com/plasma-umass/scalene>`_: A CPU+GPU+memory sampling based profiler.

* `PyInstrument  <https://github.com/joerick/pyinstrument>`_: A call stack profiler.

* `Yappi <https://github.com/sumerc/yappi>`_: A tracing profiler that is multithreading, asyncio and gevent aware.

* `profile / cProfile <https://docs.python.org/3/library/profile.html>`_: The builtin profile module.

* `timeit <https://docs.python.org/3/library/timeit.html>`_: The builtin timeit module for profiling single statements.

* `timerit <https://github.com/Erotemic/timerit>`_: A multi-statements alternative to the builtin ``timeit`` module.

* `torch.profiler <https://pytorch.org/docs/stable/profiler.html>`_ tools for profiling torch code.


.. .. todo: give more details on exact limitations.

"""
# Note: there are better ways to generate primes
# https://github.com/Sylhare/nprime

__submodules__ = [
    'line_profiler',
    'ipython_extension',
]

__autogen__ = """
mkinit ./line_profiler/__init__.py --relative
mkinit ./line_profiler/__init__.py --relative -w
"""


# from .line_profiler import __version__

# NOTE: This needs to be in sync with ../kernprof.py and line_profiler.py
__version__ = '4.2.0'

from .line_profiler import (LineProfiler,
                            load_ipython_extension, load_stats, main,
                            show_func, show_text,)


from .explicit_profiler import profile


__all__ = ['LineProfiler', 'line_profiler',
           'load_ipython_extension', 'load_stats', 'main', 'show_func',
           'show_text', '__version__', 'profile']
