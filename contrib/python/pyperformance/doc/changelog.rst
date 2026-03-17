Changelog
=========

Version 1.14.0
--------------
* Bump ``pyperf`` to 2.10.0
* Add base64 module benchmark (b64, b32, b16, a85, b85)
* Add FastAPI HTTP benchmark
* Add YAML parsing benchmark
* Respect rigorous setting in benchmark configuration files

Version 1.13.0 (2025-10-27)
--------------
* Re-enable xdsl benchmark
* Remove code for Python 3.7 and previous versions
* CI updates:
  * fix workflow ubuntu-latest
  * remove unused Azure Pipelines config
  * update checkout version
  * setup dependabot
  * update setup-python version

Version 1.12.0 (2025-10-24)
--------------
* Add xdsl benchmark (but disabled by default due to a regression)
* Add a new quadtree nbody simulation using the Barnes Hut algorithm
* Add argparse benchmark
* Add NetworkX benchmarks
* Add Sphinx benchmark
* Add decimal module benchmark
* Add BPE tokeniser benchmark
* Skip dask benchmark on Windows for Python 3.13
* Bump dask[distributed] to 2024.10.1 for Windows compatibility
* Bump greenlet to 3.2.4 for compatibility with 3.13+
* Bump Chameleon to 4.6.0
* Bump tornado to 6.5.0
* Bump pyperf to 2.9.0
* Bump sqlglot to V2
* Bump mypy to 1.18.2
* Fix check for editable mode
* Fix ``--same-loops`` handling
* Fix: pass ``--warmup`` and ``--timeout`` flags to pyperf
* Add ``-V``/``--version`` CLI argument
* Add ``--hook`` CLI argument
* Run ruff format and check under pre-commit and GitHub Actions
* Add support for experimental JIT builds
* Add Arm64 CI
* Add Free-threading CI
* Make Windows a non-experimental platform in CI
* Add support Python 3.14 and drop Python 3.7, 3.8, 3.9

Version 1.11.0 (2024-03-09)
--------------
* Add a --same-loops option to the run command to use the exact same number of
  loops as a previous run (without recalibrating).
* Bump pyperf to 2.6.3
* Fix the django_template benchmark for compatibilty with 3.13
* Fix benchmark.conf.sample

Version 1.10.0 (2023-10-22)
--------------
* Add benchmark for asyncio_webockets
* Expose --min-time from pyperf to pyperformance CLI
* Bump coverage to 7.3.2 for compatibilty with 3.13
* Bump greenlet to 3.0.0rc3 for compatibilty with 3.13

Version 1.0.9 (2023-06-14)
-------------
* Vendor lib2to3 for Python 3.13+
* Add TaskGroups variants to async_tree benchmarks

Version 1.0.8 (2023-06-02)
-------------

* Move the main requirements.txt file to pyperformance/requirements
  so that dependabot can only run on that one file
* Update dependencies of benchmarks not to specify setuptools
* On older versions of Python, skip benchmarks that use features
  introduced in newer Python versions
* Support ``--inherit-environ`` when reusing a venv
* Use tomllib/tomli over toml
* Update MANIFEST.in to include cert files for asyncio_tcp_ssl benchmark
* Fix undefined variable issue when raising VenvPipInstallFailedError
* Add mypy config; run mypy in CI
* Fix typo of str.partition from _pyproject_toml.py
* Add  version of Richards benchmark that uses super()
* Add a benchmark for runtime-checkable protocols
* Extend async tree benchmarks to cover eager task execution

Version 1.0.7 (2023-04-22)
-------------

* Upgrade pyperf from 2.5.0 to 2.6.0
* Clean unused imports and other small code details
* Migrage to the pyproject.toml based project
* Fix the django_template benchmark due to lack of distutils
* Add benchmark for toml
* Add benchmark for comprehensions
* Add benchmark for asyncio_tcp_ssl
* Add benchmark for asyncio_tcp
* Add benchmark for Dask scheduler
* Add the gc benchmarks to the MANIFEST file

Version 1.0.6 (2022-11-20)
-------------

* Upgrade pyperf from 2.4.1 to 2.5.0
* Add a benchmark to measure gc traversal
* Add jobs field in compile section to specify make -j param
* Add benchmark for Docutils
* Add async_generators benchmark
* Add benchmark for IPC
* Fix Manifest Group
* Fix installing dev build of pyperformance inside compile/compile_all
* Always upload, even when some benchmarks fail
* Add sqlglot benchmarks
* Support reporting geometric mean by tags
* Allow for specifying local wheels and sdists as dependencies
* Add a benchmark based on `python -m pprint`
* Add mdp back into the default group
* Add coroutines benchmark
* Reduce noise in generators benchmark
* Add benchmark for deepcopy
* Add coverage benchmark
* Add generators benchmark
* Add benchmark for async tree workloads
* Support relative paths to manifest files
* Add support for multiple benchmark groups in a manifest
* Fix --inherit-environ issue
* Use working Genshi 0.7.7

Version 1.0.4 (2022-01-25)
-------------

* Re-release support for user-defined benchmark after fixing problem
  with virtual environments.

Version 1.0.3 (2021-12-20)
-------------

* Support user-defined benchmark suites.

Version 1.0.2 (2021-05-11)
-------------

* Disable the henshi benchmark temporarily since is no longer compatible
  with Python 3.11.
* Reenable html5lib benchmark: html5lib 1.1 has been released.
* Update requirements.
* Replace Travis CI with GitHub Actions.
* The development branch ``master`` was renamed to ``main``.
  See https://sfconservancy.org/news/2020/jun/23/gitbranchname/ for the
  rationale.

Version 1.0.1 (2020-03-26)
--------------------------

* Drop usage of the six module since Python 2 is no longer supported.
  Remove Python 2 specific code.
* Update dependencies:

  * django: 3.0 => 3.0.4
  * dulwich: 0.19.14 => 0.19.15
  * mako: 1.1.0 = > 1.1.2
  * mercurial: 5.1.1 => 5.3.1
  * psutil: 5.6.7 => 5.7.0
  * pyperf: 1.7.0 => 2.0.0
  * sqlalchemy: 1.3.12 => 1.3.15
  * sympy: 1.5 => 1.5.1
  * tornado: 6.0.3 => 6.0.4

* Remove six, html5lib and mercurial requirements.
* pip-tools (pip-compile) is now used to update dependencies

Version 1.0.0 (2019-12-17)
--------------------------

* Enable pyflate benchmarks on Python 3.
* Remove ``spambayes`` benchmark: it is not compatible with Python 3.
* Remove ``2n3``:benchmark group.
* Drop Python 2.7 support: old Django and Tornado versions are
  not compatible with incoming Python 3.9.
* Disable html5lib benchmark temporarily, since it's no longer compatible
  with Python 3.9.
* Update requirements:

  * Django: 1.11.22 => 3.0
  * Mako: 1.0.14 => 1.1.0
  * SQLAlchemy: 1.3.6 => 1.3.12
  * certifi: 2019.6.16 => 2019.11.28
  * docutils: 0.15.1 => 0.15.2
  * dulwich: 0.19.11 => 0.19.14
  * mercurial: 5.0.2 => 5.1.1
  * psutil: 5.6. => 5.6.7
  * pyperf: 1.6.1 => 1.7.0
  * six: 1.12. =>  1.13.0
  * sympy: 1.4 => 1.5

Version 0.9.1 (2019-07-29)
--------------------------

* Enable hg_startup on Python 3
* Fix compatibility with Python 3.8 beta 2
* Update requirements:

  * certifi: 2019.3.9 => 2019.6.16
  * Chameleon: 3.6.1 => 3.6.2
  * Django: 1.11.20 => 1.11.22
  * docutils: 0.14 => 0.15.1.post1
  * Mako: 1.0.10 => 1.0.14
  * mercurial: 5.0 => 5.0.2
  * pathlib2: 2.3.3 => 2.3.4
  * psutil: 5.6.2 => 5.6.3
  * SQLAlchemy: 1.3.4 => 1.3.6

Version 0.9.0 (2019-05-29)
--------------------------

* Project renamed from "performance" to "pyperformance"
* Upgrade pyperf from version 1.6.0 to 1.6.1. The project has been renamed from
  "perf" to "pyperf". Update imports.
* Issue #54: Update Genshi to 0.7.3. It is now compatible with Python 3.8.
* Update requirements:

  * Mako: 1.0.9= > 1.0.10
  * SQLAlchemy: 1.3.3 => 1.3.4

Version 0.8.0 (2019-05-10)
--------------------------

* compile command: Add "pkg_only" option to benchmark.conf.
  Add support for native libraries that are installed but not on path.
  Patch by Robert Grimm.
* Update Travis configuration: use trusty image, use pip cache.
  Patch by Inada Naoki.
* Upgrade tornado to 5.1.1.
  Patch by Inada Naoki.
* Fix compile command on Mac OS: no program extension. Patch by Anthony Shaw.
* Update requirements:

  * Chameleon: 3.4 => 3.6.1
  * Django: 1.11.16 => 1.11.20
  * Genshi: 0.7.1 => 0.7.2
  * Mako: 1.0.7 => 1.0.9
  * MarkupSafe: 1.0 => 1.1.1
  * SQLAlchemy: 1.2.12 => 1.3.3
  * certifi: 2018.10.15 => 2019.3.9
  * dulwich: 0.19.6 => 0.19.11
  * mercurial: 4.7.2 => 5.0
  * mpmath: 1.0.0 => 1.1.0
  * pathlib2: 2.3.2 => 2.3.3
  * perf: 1.5.1 => 1.6.0
  * psutil: 5.4.7 => 5.6.2
  * six: 1.11.0 => 1.12.0
  * sympy: 1.3 => 1.4
  * tornado: 4.5.3 => 5.1.1

Version 0.7.0 (2018-10-16)
--------------------------

* python_startup: Add ``--exit`` option.
* Update requirements:

  * certifi: 2017.11.5 => 2018.10.15
  * Chameleon: 3.2 => 3.4
  * Django: 1.11.9 => 1.11.16
  * dulwich: 0.18.6 => 0.19.6
  * Genshi: 0.7 => 0.7.1
  * mercurial: 4.4.2 => 4.7.2
  * pathlib2: 2.3.0 => 2.3.2
  * psutil: 5.4.3 => 5.4.7
  * SQLAlchemy: 1.2.0 => 1.2.12
  * sympy: 1.1.1 => 1.3

* Fix issue #40 for pip 10 and newer: Remove indirect dependencies. Indirect
  dependencies were used to install cffi, but Mercurial 4.0 doesn't depend on
  cffi anymore.

Version 0.6.1 (2018-01-11)
--------------------------

* Fix inherit-environ: propagate to recursive invocations of ``performance``
  in ``compile`` and ``compile_all`` commands.
* Fix the ``--track-memory`` option thanks to the update to perf 1.5.
* Update requirements

  - certifi: 2017.4.17 => 2017.11.5
  - Chameleon: 3.1 => 3.2
  - Django: 1.11.3 => 1.11.9
  - docutils: 0.13.1 => 0.14
  - dulwich: 0.17.3 => 0.18.6
  - html5lib: 0.999999999 => 1.0.1
  - Mako: 1.0.6 => 1.0.7
  - mercurial: 4.2.2 => 4.4.2
  - mpmath: 0.19 => 1.0.0
  - perf: 1.4 => 1.5.1 (fix ``--track-memory`` option)
  - psutil: 5.2.2 => 5.4.3
  - pyaes: 1.6.0 => 1.6.1
  - six: 1.10.0 => 1.11.0
  - SQLAlchemy: 1.1.11 => 1.2.0
  - sympy: 1.0 => 1.1.1
  - tornado: 4.5.1 => 4.5.3

Version 0.6.0 (2017-07-06)
--------------------------

* Change ``warn`` to ``warning`` in `bm_logging.py`. In Python 3, Logger.warn()
  calls warnings.warn() to log a deprecation warning, so is slower than
  Logger.warning().
* Add again the ``logging_silent`` microbenchmark suite.
* compile command: update the Git repository before getting the revision
* Update requirements

  - perf: 1.3 => 1.4 (fix parse_cpu_list(): strip also NUL characters)
  - Django: 1.11.1 => 1.11.3
  - mercurial: 4.2 => 4.2.2
  - pathlib2: 2.2.1 => 2.3.0
  - SQLAlchemy: 1.1.10 => 1.1.11

Version 0.5.5 (2017-05-29)
--------------------------

* On the 2.x branch on CPython, ``compile`` now pass ``--enable-unicode=ucs4``
  to the ``configure`` script on all platforms, except on Windows which uses
  UTF-16 because of its 16-bit wchar_t.
* The ``float`` benchmark now uses ``__slots__`` on the ``Point`` class.
* Remove the following microbenchmarks. They have been moved to the
  `pymicrobench <https://github.com/vstinner/pymicrobench>`_ project because
  they are too short, not representative of real applications and are too
  unstable.

  - ``pybench`` microbenchmark suite
  - ``call_simple``
  - ``call_method``
  - ``call_method_unknown``
  - ``call_method_slots``
  - ``logging_silent``: values are faster than 1 ns on PyPy with 2^27 loops!
    (and around 0.7 us on CPython)

* Update requirements

  - Django: 1.11 => 1.11.1
  - SQLAlchemy: 1.1.9 => 1.1.10
  - certifi: 2017.1.23 => 2017.4.17
  - perf: 1.2 => 1.3
  - mercurial: 4.1.2 => 4.2
  - tornado: 4.4.3 => 4.5.1

Version 0.5.4 (2017-04-10)
--------------------------

* Create a new documentation at: http://pyperformance.readthedocs.io/
* Add "CPython results, 2017" to the doc: significant performance changes,
  significant optimizations, timeline, etc.
* The ``show`` command doesn't need to create a virtual env anymore.
* Add new commands:

  - ``pyperformance compile``: compile, install and benchmark
  - ``pyperformance compile_all``: benchmark multiple branches and
    revisions of Python
  - ``pyperformance upload``: upload a JSON file to a Codespeed

* setup.py: add dependencies to ``perf`` and ``six`` modules.
* bm_xml_etree now uses "_pure_python" in benchmark names if the accelerator is
  explicitly disabled.
* Upgrade requirements:

  - Django: 1.10.6 -> 1.11
  - SQLAlchemy: 1.1.6 -> 1.1.9
  - mercurial: 4.1.1 -> 4.1.2
  - perf: 1.1 => 1.2
  - psutil: 5.2.1 -> 5.2.2
  - tornado: 4.4.2 -> 4.4.3
  - webencodings: 0.5 -> 0.5.1

* perf 1.2 now calibrates the number of warmups on PyPy.
* On Python 3.5a0: force pip 7.1.2 and setuptools 18.5:
  https://sourceforge.net/p/pyparsing/bugs/100/

Version 0.5.3 (2017-03-27)
--------------------------

* Upgrade Dulwich to 0.17.3 to support PyPy older than 5.6:
  see https://github.com/jelmer/dulwich/issues/509
* Fix ResourceWarning warnings: close explicitly files and sockets.
* scripts: replace Mercurial commands with Git commands.
* Upgrade requirements:

  - dulwich: 0.17.1 => 0.17.3
  - perf: 1.0 => 1.1
  - psutil: 5.2.0 => 5.2.1

Version 0.5.2 (2017-03-17)
--------------------------

* Upgrade requirements:

  - certifi: 2016.9.26 => 2017.1.23
  - Chameleon: 3.0 => 3.1
  - Django: 1.10.5 => 1.10.6
  - MarkupSafe: 0.23 => 1.0
  - dulwich: 0.16.3 => 0.17.1
  - mercurial: 4.0.2 => 4.1.1
  - pathlib2: 2.2.0 => 2.2.1
  - perf: 0.9.3 => 1.0
  - psutil: 5.0.1 => 5.2.0
  - SQLAlchemy: 1.1.4 => 1.1.6

Version 0.5.1 (2017-01-16)
--------------------------

* Fix Windows support (upgrade perf from 0.9.0 to 0.9.3)
* Upgrade requirements:

  - Chameleon: 2.25 => 3.0
  - Django: 1.10.3 => 1.10.5
  - docutils: 0.12 => 0.13.1
  - dulwich: 0.15.0 => 0.16.3
  - mercurial: 4.0.0 => 4.0.2
  - perf: 0.9.0 => 0.9.3
  - psutil: 5.0.0 => 5.0.1

Version 0.5.0 (2016-11-16)
--------------------------

* Add ``mdp`` benchmark: battle with damages and topological sorting of nodes
  in a graph
* The ``default`` benchmark group now include all benchmarks but ``pybench``
* If a benchmark fails, log an error, continue to execute following
  benchmarks, but exit with error code 1.
* Remove deprecated benchmarks: ``threading_threaded_count`` and
  ``threading_iterative_count``. It wasn't possible to run them anyway.
* ``dulwich`` requirement is now optional since its installation fails
  on Windows.
* Upgrade requirements:

  - Mako: 1.0.5 => 1.0.6
  - Mercurial: 3.9.2 => 4.0.0
  - SQLAlchemy: 1.1.3 => 1.1.4
  - backports-abc: 0.4 => 0.5

Version 0.4.0 (2016-11-07)
--------------------------

* Add ``sqlalchemy_imperative`` benchmark: it wasn't registered properly
* The ``list`` command now only lists the benchmark that the ``run`` command
  will run. The ``list`` command gets a new ``-b/--benchmarks`` option.
* Rewrite the code creating the virtual environment to test correctly pip.
  Download and run ``get-pip.py`` if pip installation failed.
* Upgrade requirements:

  * perf: 0.8.2 => 0.9.0
  * Django: 1.10.2 => 1.10.3
  * Mako: 1.0.4 => 1.0.5
  * psutil: 4.3.1 => 5.0.0
  * SQLAlchemy: 1.1.2 => 1.1.3

* Remove ``virtualenv`` dependency

Version 0.3.2 (2016-10-19)
--------------------------

* Fix setup.py: include also ``performance/benchmarks/data/asyncio.git/``

Version 0.3.1 (2016-10-19)
--------------------------

* Add ``regex_dna`` benchmark
* The ``run`` command now fails with an error if no benchmark was run.
* genshi, logging, scimark, sympy and xml_etree scripts now run all
  sub-benchmarks by default
* Rewrite pybench using perf: remove the old legacy code to calibrate and run
  benchmarks, reuse perf.Runner API.
* Change heuristic to create the virtual environment, tried commands:

  * ``python -m venv``
  * ``python -m virtualenv``
  * ``virtualenv -p python``

* The creation of the virtual environment now ensures that pip works
  to detect "python3 -m venv" which doesn't install pip.
* Upgrade perf dependency from 0.7.12 to 0.8.2: update all benchmarks to
  the new perf 0.8 API (which introduces incompatible changes)
* Update SQLAlchemy from 1.1.1 to 1.1.2

Version 0.3.0 (2016-10-11)
--------------------------

New benchmarks:

* Add ``crypto_pyaes``: Benchmark a pure-Python implementation of the AES
  block-cipher in CTR mode using the pyaes module (version 1.6.0). Add
  ``pyaes`` dependency.
* Add ``sympy``: Benchmark on SymPy. Add ``scipy`` dependency.
* Add ``scimark`` benchmark
* Add ``deltablue``: DeltaBlue benchmark
* Add ``dulwich_log``: Iterate on commits of the asyncio Git repository using
  the Dulwich module. Add ``dulwich`` (and ``mpmath``) dependencies.
* Add ``pyflate``: Pyflate benchmark, tar/bzip2 decompressor in pure
  Python
* Add ``sqlite_synth`` benchmark: Benchmark Python aggregate for SQLite
* Add ``genshi`` benchmark: Render template to XML or plain text using the
  Genshi module. Add ``Genshi`` dependency.
* Add ``sqlalchemy_declarative`` and ``sqlalchemy_imperative`` benchmarks:
  SQLAlchemy Declarative and Imperative benchmarks using SQLite. Add
  ``SQLAlchemy`` dependency.

Enhancements:

* ``compare`` command now fails if the performance versions are different
* ``nbody``: add ``--reference`` and ``--iterations`` command line options.
* ``chaos``: add ``--width``, ``--height``, ``--thickness``, ``--filename``
  and ``--rng-seed`` command line options
* ``django_template``: add ``--table-size`` command line option
* ``json_dumps``: add ``--cases`` command line option
* ``pidigits``: add ``--digits`` command line option
* ``raytrace``: add ``--width``, ``--height`` and ``--filename`` command line
  options
* Port ``html5lib`` benchmark to Python 3
* Enable ``pickle_pure_python`` and ``unpickle_pure_python`` on Python 3
  (code was already compatible with Python 3)
* Creating the virtual environment doesn't inherit environment variables
  (especially ``PYTHONPATH``) by default anymore: ``--inherit-environ``
  command line option must now be used explicitly.

Bugfixes:

* ``chaos`` benchmark now also reset the ``random`` module at each sample
  to get more reproductible benchmark results
* Logging benchmarks now truncate the in-memory stream before each benchmark
  run

Rename benchmarks:

* Rename benchmarks to get a consistent name between the command line and
  benchmark name in the JSON file.
* Rename pickle benchmarks:

   - ``slowpickle`` becomes ``pickle_pure_python``
   - ``slowunpickle`` becomes ``unpickle_pure_python``
   - ``fastpickle`` becomes ``pickle``
   - ``fastunpickle`` becomes ``unpickle``

 * Rename ElementTree benchmarks: replace ``etree_`` prefix with
   ``xml_etree_``.
 * Rename ``hexiom2`` to ``hexiom_level25`` and explicitly pass ``--level=25``
   parameter
 * Rename ``json_load`` to ``json_loads``
 * Rename ``json_dump_v2`` to ``json_dumps`` (and remove the deprecated
   ``json_dump`` benchmark)
 * Rename ``normal_startup`` to ``python_startup``, and ``startup_nosite``
   to ``python_startup_no_site``
 * Rename ``threaded_count`` to ``threading_threaded_count``,
   rename ``iterative_count`` to ``threading_iterative_count``
 * Rename logging benchmarks:

   - ``silent_logging`` to ``logging_silent``
   - ``simple_logging`` to ``logging_simple``
   - ``formatted_logging`` to ``logging_format``

Minor changes:

* Update dependencies
* Remove broken ``--args`` command line option.


Version 0.2.2 (2016-09-19)
--------------------------

* Add a new ``show`` command to display a benchmark file
* Issue #11: Display Python version in compare. Display also the performance
  version.
* CPython issue #26383; csv output: don't truncate digits for timings shorter
  than 1 us
* compare: Use sample unit of benchmarks, format values in the table
  output using the unit
* compare: Fix the table output if benchmarks only contain a single sample
* Remove unused -C/--control_label and -E/--experiment_label options
* Update perf dependency to 0.7.11 to get Benchmark.get_unit() and
  BenchmarkSuite.get_metadata()

Version 0.2.1 (2016-09-10)
--------------------------

* Add ``--csv`` option to the ``compare`` command
* Fix ``compare -O table`` output format
* Freeze indirect dependencies in requirements.txt
* ``run``: add ``--track-memory`` option to track the memory peak usage
* Update perf dependency to 0.7.8 to support memory tracking and the new
  ``--inherit-environ`` command line option
* If ``virtualenv`` command fail, try another command to create the virtual
  environment: catch ``virtualenv`` error
* The first command to upgrade pip to version ``>= 6.0`` now uses the ``pip``
  binary rather than ``python -m pip`` to support pip 1.0 which doesn't support
  ``python -m pip`` CLI.
* Update Django (1.10.1), Mercurial (3.9.1) and psutil (4.3.1)
* Rename ``--inherit_env`` command line option to ``--inherit-environ`` and fix
  it

Version 0.2 (2016-09-01)
------------------------

* Update Django dependency to 1.10
* Update Chameleon dependency to 2.24
* Add the ``--venv`` command line option
* Convert Python startup, Mercurial startup and 2to3 benchmarks to perf scripts
  (bm_startup.py, bm_hg_startup.py and bm_2to3.py)
* Pass the ``--affinity`` option to perf scripts rather than using the
  ``taskset`` command
* Put more installer and optional requirements into
  ``performance/requirements.txt``
* Cached ``.pyc`` files are no more removed before running a benchmark.
  Use ``venv recreate`` command to update a virtual environment if required.
* The broken ``--track_memory`` option has been removed. It will be added back
  when it will be fixed.
* Add performance version to metadata
* Upgrade perf dependency to 0.7.5 to get ``Benchmark.update_metadata()``

Version 0.1.2 (2016-08-27)
--------------------------

* Windows is now supported
* Add a new ``venv`` command to show, create, recrete or remove the virtual
  environment.
* Fix pybench benchmark (update to perf 0.7.4 API)
* performance now tries to install the ``psutil`` module on CPython for better
  system metrics in metadata and CPU pinning on Python 2.
* The creation of the virtual environment now also tries ``virtualenv`` and
  ``venv`` Python modules, not only the virtualenv command.
* The development version of performance now installs performance
  with "pip install -e <path_to_performance>"
* The GitHub project was renamed from ``python/benchmarks``
  to ``python/performance``.

Version 0.1.1 (2016-08-24)
--------------------------

* Fix the creation of the virtual environment
* Rename pybenchmarks script to pyperformance
* Add -p/--python command line option
* Add __main__ module to be able to run: python3 -m performance

Version 0.1 (2016-08-24)
------------------------

* First release after the conversion to the perf module and move to GitHub
* Removed benchmarks

  - django_v2, django_v3
  - rietveld
  - spitfire (and psyco): Spitfire is not available on PyPI
  - pystone
  - gcbench
  - tuple_gc_hell


History
-------

Projected moved to https://github.com/python/performance in August 2016. Files
reorganized, benchmarks patched to use the perf module to run benchmark in
multiple processes.

Project started in December 2008 by Collin Winter and Jeffrey Yasskin for the
Unladen Swallow project. The project was hosted at
https://hg.python.org/benchmarks until Feb 2016
