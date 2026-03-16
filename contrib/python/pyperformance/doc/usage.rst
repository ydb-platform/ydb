+++++
Usage
+++++

Installation
============

Command to install pyperformance::

    python3 -m pip install pyperformance

The command installs a new ``pyperformance`` program.

If needed, ``pyperf`` and ``six`` dependencies are installed automatically.

pyperformance works on Python 3.6 and newer, but it may work on Python 3.4 and
3.5.

At runtime, Python development files (header files) may be needed to install
some dependencies like ``dulwich_log`` or ``psutil``, to build their C
extension. Commands on Fedora to install dependencies:

* Python 3: ``sudo dnf install python3-devel``
* PyPy: ``sudo dnf install pypy-devel``

Windows notes
-------------

On Windows, to allow pyperformance to build dependencies from source
like ``greenlet``, ``dulwich`` or ``psutil``, if you want to use a
``python.exe`` built from source, you should not use the ``python.exe``
directly. Instead, you must run the little-known command ``PC\layout``
to create a filesystem layout that resembles an installed Python::

    .\python.bat -m PC.layout --preset-default --copy installed -v

(Use the ``--help`` flag for more info about ``PC\layout``.)

Now you can use the "installed" Python executable::

    installed\python.exe -m pip install pyperformance
    installed\python.exe -m pyperformance run ...

Using an *actually* installed Python executable (e.g. via ``py``)
works fine too.


Run benchmarks
==============

Commands to compare Python 3.6 and Python 3.7 performance::

    pyperformance run --python=python3.6 -o py36.json
    pyperformance run --python=python3.7 -o py37.json
    pyperformance compare py36.json py37.json

Note: ``python3 -m pyperformance ...`` syntax works as well (ex: ``python3 -m
pyperformance run -o py37.json``), but requires to install pyperformance on each
tested Python version.

JSON files are produced by the pyperf module and so can be analyzed using pyperf
commands::

    python3 -m pyperf show py36.json
    python3 -m pyperf check py36.json
    python3 -m pyperf metadata py36.json
    python3 -m pyperf stats py36.json
    python3 -m pyperf hist py36.json
    python3 -m pyperf dump py36.json
    (...)

It's also possible to use pyperf to compare results of two JSON files::

    python3 -m pyperf compare_to py36.json py37.json --table

Basic commands
--------------

pyperformance actions::

    run                 Run benchmarks on the running python
    show                Display a benchmark file
    compare             Compare two benchmark files
    list                List benchmarks of the running Python
    list_groups         List benchmark groups of the running Python
    venv                Actions on the virtual environment

Common options
--------------

Options available to all commands::

  -h, --help            show this help message and exit

run
---

Run benchmarks on the running python.

Usage::

  pyperformance run [-h] [-r] [-f] [--debug-single-value] [-v] [-m]
                       [--affinity CPU_LIST] [-o FILENAME]
                       [--append FILENAME] [--manifest MANIFEST]
                       [--timeout TIMEOUT] [-b BM_LIST]
                       [--inherit-environ VAR_LIST] [-p PYTHON]
                       [--hook HOOK]

options::

  -h, --help            show this help message and exit
  -r, --rigorous        Spend longer running tests to get more
                        accurate results
  -f, --fast            Get rough answers quickly
  --debug-single-value  Debug: fastest mode, only compute a single
                        value
  -v, --verbose         Print more output
  -m, --track-memory    Track memory usage. This only works on Linux.
  --affinity CPU_LIST   Specify CPU affinity for benchmark runs. This
                        way, benchmarks can be forced to run on a
                        given CPU to minimize run to run variation.
  -o FILENAME, --output FILENAME
                        Run the benchmarks on only one interpreter and
                        write benchmark into FILENAME. Provide only
                        baseline_python, not changed_python.
  --append FILENAME     Add runs to an existing file, or create it if
                        it doesn't exist
  --timeout TIMEOUT     Specify a timeout in seconds for a single
                        benchmark run (default: disabled)
  --manifest MANIFEST   benchmark manifest file to use
  -b BM_LIST, --benchmarks BM_LIST
                        Comma-separated list of benchmarks to run. Can
                        contain both positive and negative arguments:
                        --benchmarks=run_this,also_this,-not_this. If
                        there are no positive arguments, we'll run all
                        benchmarks except the negative arguments.
                        Otherwise we run only the positive arguments.
  --inherit-environ VAR_LIST
                        Comma-separated list of environment variable
                        names that are inherited from the parent
                        environment when running benchmarking
                        subprocesses.
  -p PYTHON, --python PYTHON
                        Python executable (default: use running
                        Python)
  --same-loops SAME_LOOPS
                        Use the same number of loops as a previous run
                        (i.e., don't recalibrate). Should be a path to a
                        .json file from a previous run.
  --hook HOOK
                        Apply the given pyperf hook when running the
                        benchmarks.

show
----

Display a benchmark file.

Usage::

    show FILENAME

positional arguments::

  FILENAME

compare
-------

Compare two benchmark files.

Usage::

  pyperformance compare [-h] [-v] [-O STYLE] [--csv CSV_FILE]
                        [--inherit-environ VAR_LIST] [-p PYTHON]
                        baseline_file.json changed_file.json

positional arguments::

  baseline_file.json
  changed_file.json

options::

  -v, --verbose         Print more output
  -O STYLE, --output_style STYLE
                        What style the benchmark output should take.
                        Valid options are 'normal' and 'table'.
                        Default is normal.
  --csv CSV_FILE        Name of a file the results will be written to,
                        as a three-column CSV file containing minimum
                        runtimes for each benchmark.
  --inherit-environ VAR_LIST
                        Comma-separated list of environment variable
                        names that are inherited from the parent
                        environment when running benchmarking
                        subprocesses.
  -p PYTHON, --python PYTHON
                        Python executable (default: use running
                        Python)

list
----

List benchmarks of the running Python.

Usage::

  pyperformance list [-h] [--manifest MANIFEST] [-b BM_LIST]
                     [--inherit-environ VAR_LIST] [-p PYTHON]

options::

  --manifest MANIFEST   benchmark manifest file to use
  -b BM_LIST, --benchmarks BM_LIST
                        Comma-separated list of benchmarks to run. Can
                        contain both positive and negative arguments:
                        --benchmarks=run_this,also_this,-not_this. If
                        there are no positive arguments, we'll run all
                        benchmarks except the negative arguments.
                        Otherwise we run only the positive arguments.
  --inherit-environ VAR_LIST
                        Comma-separated list of environment variable
                        names that are inherited from the parent
                        environment when running benchmarking
                        subprocesses.
  -p PYTHON, --python PYTHON
                        Python executable (default: use running
                        Python)

Use ``python3 -m pyperformance list -b all`` to list all benchmarks.

list_groups
-----------

List benchmark groups of the running Python.

Usage::

  pyperformance list_groups [-h] [--manifest MANIFEST]
                            [--inherit-environ VAR_LIST]
                            [-p PYTHON]

options::

  --manifest MANIFEST   benchmark manifest file to use
  --inherit-environ VAR_LIST
                        Comma-separated list of environment variable
                        names that are inherited from the parent
                        environment when running benchmarking
                        subprocesses.
  -p PYTHON, --python PYTHON
                        Python executable (default: use running
                        Python)

venv
----

Actions on the virtual environment.

Actions::

  show      Display the path to the virtual environment and its status
            (created or not)
  create    Create the virtual environment
  recreate  Force the recreation of the the virtual environment
  remove    Remove the virtual environment

Common options::

  --venv VENV           Path to the virtual environment
  --inherit-environ VAR_LIST
                        Comma-separated list of environment variable
                        names that are inherited from the parent
                        environment when running benchmarking
                        subprocesses.
  -p PYTHON, --python PYTHON
                        Python executable (default: use running
                        Python)

venv show
~~~~~~~~~

Display the path to the virtual environment and its status (created or not).

Usage::

  pyperformance venv show [-h] [--venv VENV]
                          [--inherit-environ VAR_LIST] [-p PYTHON]

venv create
~~~~~~~~~~~

Create the virtual environment.

Usage::

  pyperformance venv create [-h] [--venv VENV]
                            [--manifest MANIFEST] [-b BM_LIST]
                            [--inherit-environ VAR_LIST]
                            [-p PYTHON]

options::

  --manifest MANIFEST   benchmark manifest file to use
  -b BM_LIST, --benchmarks BM_LIST
                        Comma-separated list of benchmarks to run. Can
                        contain both positive and negative arguments:
                        --benchmarks=run_this,also_this,-not_this. If
                        there are no positive arguments, we'll run all
                        benchmarks except the negative arguments.
                        Otherwise we run only the positive arguments.

venv recreate
~~~~~~~~~~~~~

Force the recreation of the the virtual environment.

Usage::

  pyperformance venv recreate [-h] [--venv VENV]
                              [--manifest MANIFEST] [-b BM_LIST]
                              [--inherit-environ VAR_LIST]
                              [-p PYTHON]

options::

  --manifest MANIFEST   benchmark manifest file to use
  -b BM_LIST, --benchmarks BM_LIST
                        Comma-separated list of benchmarks to run. Can
                        contain both positive and negative arguments:
                        --benchmarks=run_this,also_this,-not_this. If
                        there are no positive arguments, we'll run all
                        benchmarks except the negative arguments.
                        Otherwise we run only the positive arguments.

venv remove
~~~~~~~~~~~

Remove the virtual environment.

Usage::

  pyperformance venv remove [-h] [--venv VENV]
                            [--inherit-environ VAR_LIST]
                            [-p PYTHON]



Compile Python to run benchmarks
================================

pyperformance actions::

    compile             Compile and install CPython and run benchmarks on
                        installed Python
    compile_all         Compile and install CPython and run benchmarks on
                        installed Python on all branches and revisions of
                        CONFIG_FILE
    upload              Upload JSON results to a Codespeed website

All these commands require a configuration file.

Simple configuration usable for ``compile`` (but not for ``compile_all`` nor
``upload``), ``doc/benchmark.conf``:

.. literalinclude:: benchmark.conf
   :language: ini

Configuration file sample with comments, ``doc/benchmark.conf.sample``:

.. literalinclude:: benchmark.conf.sample
   :language: ini


.. _cmd-compile:

compile
-------

Compile Python, install Python and run benchmarks on the installed Python.

Usage::

  pyperformance compile [-h] [--patch PATCH] [-U] [-T]
                        [--inherit-environ VAR_LIST] [-p PYTHON]
                        config_file revision [branch]


positional arguments::

  config_file           Configuration filename
  revision              Python benchmarked revision
  branch                Git branch

options::

  --patch PATCH         Patch file
  -U, --no-update       Don't update the Git repository
  -T, --no-tune         Don't run 'pyperf system tune' to tune the
                        system for benchmarks
  --inherit-environ VAR_LIST
                        Comma-separated list of environment variable
                        names that are inherited from the parent
                        environment when running benchmarking
                        subprocesses.
  -p PYTHON, --python PYTHON
                        Python executable (default: use running
                        Python)

Notes:

* PGO is broken on Ubuntu 14.04 LTS with GCC 4.8.4-2ubuntu1~14.04:
  ``Modules/socketmodule.c:7743:1: internal compiler error: in edge_badness,
  at ipa-inline.c:895``


compile_all
-----------

Compile all branches and revisions of CONFIG_FILE.

Usage::

  pyperformance compile_all [-h] [--inherit-environ VAR_LIST] [-p PYTHON]
                            config_file

positional arguments::

  config_file           Configuration filename

options::

  --inherit-environ VAR_LIST
                        Comma-separated list of environment variable
                        names that are inherited from the parent
                        environment when running benchmarking
                        subprocesses.
  -p PYTHON, --python PYTHON
                        Python executable (default: use running
                        Python)

upload
------

Upload results from a JSON file to a Codespeed website.

Usage::

  pyperformance upload [-h] [--inherit-environ VAR_LIST] [-p PYTHON]
                       config_file json_file

positional arguments::

  config_file           Configuration filename
  json_file             JSON filename

options::

  --inherit-environ VAR_LIST
                        Comma-separated list of environment variable
                        names that are inherited from the parent
                        environment when running benchmarking
                        subprocesses.
  -p PYTHON, --python PYTHON
                        Python executable (default: use running
                        Python)


How to get stable benchmarks
============================

* Run ``python3 -m pyperf system tune`` command
* Compile Python using LTO (Link Time Optimization) and PGO (profile guided
  optimizations): use the :ref:`pyperformance compile <cmd-compile>` command with
  uses LTO and PGO by default
* See advices of the pyperf documentation:
  `How to get reproductible benchmark results
  <http://pyperf.readthedocs.io/en/latest/run_benchmark.html#how-to-get-reproductible-benchmark-results>`_.


pyperformance virtual environment
=================================

To run benchmarks, pyperformance first creates a virtual environment. It installs
requirements with fixed versions to get a reproductible environment. The system
Python has unknown module installed with unknown versions, and can have
``.pth`` files run at Python startup which can modify Python behaviour or at
least slow down Python startup.


What is the goal of pyperformance
=================================

A benchmark is always written for a specific purpose. Depending how the
benchmark is written and how the benchmark is run, the result can be different
and so have a different meaning.

The pyperformance benchmark suite has multiple goals:

* Help to detect performance regression in a Python implementation
* Validate that an optimization change makes Python faster and don't
  performance regressions, or only minor regressions
* Compare two implementations of Python, for example CPython and PyPy
* Showcase of Python performance which ideally would be representative
  of performances of applications running on production

Don't disable GC nor ASLR
-------------------------

The pyperf module and pyperformance benchmarks are designed to produce
reproductible results, but not at the price of running benchmarks in a special
mode which would not be used to run applications in production. For these
reasons, the Python garbage collector, Python randomized hash function and
system ASLR (Address Space Layout Randomization) are **not disabled**.
Benchmarks don't call ``gc.collect()`` neither since CPython implements it with
`stop-the-world
<https://en.wikipedia.org/wiki/Tracing_garbage_collection#Stop-the-world_vs._incremental_vs._concurrent>`_
and so applications don't call it to not kill performances.

Include outliers and spikes
---------------------------

Moreover, while the pyperf documentation explains how to reduce the random noise
of the system and other applications, some benchmarks use the system and so can
get different timing depending on the system workload, depending on I/O
performances, etc. Outliers and temporary spikes in results are **not
automatically removed**: values are summarized by computing the average
(arithmetic mean) and standard deviation which "contains" these spikes, instead
of using median and the median absolute deviation for example which to ignore
outliers. It is deliberate choice since applications running in production are
impacted by such temporary slowdown caused by various things like a garbage
collection or a JIT compilation.

Warmups and steady state
------------------------

A borderline issue are the benchmarks "warmups". The first values of each
worker process are always slower: 10% slower in the best case, it can be 1000%
slower or more on PyPy. Right now (2017-04-14), pyperformance ignore first values
considered as warmup until a benchmark reachs its "steady state". The "steady
state" can include temporary spikes every 5 values (ex: caused by the garbage
collector), and it can still imply further JIT compiler optimizations but with
a "low" impact on the average pyperformance.

To be clear "warmup" and "steady state" are a work-in-progress and a very
complex topic, especially on PyPy and its JIT compiler.


Notes
=====

Tool for comparing the performance of two Python implementations.

pyperformance will run Student's two-tailed T test on the benchmark results at the 95%
confidence level to indicate whether the observed difference is statistically
significant.

Omitting the ``-b`` option will result in the default group of benchmarks being
run Omitting ``-b`` is the same as specifying `-b default`.

To run every benchmark pyperformance knows about, use ``-b all``. To see a full
list of all available benchmarks, use `--help`.

Negative benchmarks specifications are also supported: `-b -2to3` will run every
benchmark in the default group except for 2to3 (this is the same as
`-b default,-2to3`). `-b all,-django` will run all benchmarks except the Django
templates benchmark. Negative groups (e.g., `-b -default`) are not supported.
Positive benchmarks are parsed before the negative benchmarks are subtracted.

If ``--track_memory`` is passed, pyperformance will continuously sample the
benchmark's memory usage. This currently only works on Linux 2.6.16 and higher
or Windows with PyWin32. Because ``--track_memory`` introduces performance
jitter while collecting memory measurements, only memory usage is reported in
the final report.
