+++++++++++++++++
Custom Benchmarks
+++++++++++++++++

pyperformance includes its own set of benchmarks (see :doc:`benchmarks`).
However, it also supports using custom benchmarks.

Using Custom Benchmarks
=======================

To use custom benchmarks, you will need to use the ``--manifest`` CLI
option and provide the path to the manifest file describing those
benchmarks.


The pyperformance File Formats
==============================

``pyperformance`` uses two file formats to identify benchmarks:

* manifest - a set of benchmarks
* metadata - a single benchmark

For each benchmark, there are two required files and several optional
ones.  Those files are expected to be in a specific directory structure
(unless customized in the metadata).

The structure (see below) is such that it's easy to maintain
a benchmark (or set of benchmarks) on GitHub and distribute it on PyPI.
It also simplifies publishing a Python project's benchmarks.
The alternative is pointing people at a repo.

Benchmarks can inherit metadata from other metadata files.
This is useful for keeping common metadata for a set of benchmarks
(e.g. "version") in one file.  Likewise, benchmarks for a Python
project can inherit metadata from the project's pyproject.toml.

Sometimes a benchmark will have one or more variants that run using
the same script.  Variants like this are supported by ``pyperformance``
without requiring much extra effort.


Benchmark Directory Structure
-----------------------------

Normally a benchmark is structured like this::

    bm_NAME/
       data/  # if needed
       requirements.txt  # lock file, if any
       pyproject.toml
       run_benchmark.py

(Note the "bm\_" prefix on the directory name.)

"pyproject.toml" holds the metadata.  "run_benchmark.py" holds
the actual benchmark code.  Both are necessary.

``pyperformance`` treats the metadata file as the fundamental source of
information about a benchmark.  A manifest for a set of benchmarks is
effectively a mapping of names to metadata files.  So a metadata file
is essential.  It can be located anywhere on disk.  However, if it
isn't located in the structure described above then the metadata must
identify where to find the other files.

Other than that, only a benchmark script (e.g. "run_benchmark.py" above)
is required.  All other files are optional.

When a benchmark has variants, each has its own metadata file next to
the normal "pyproject.toml", named "bm_NAME.toml".  (Note the "bm\_" prefix.)
The format of variant metadata files is exactly the same.  ``pyperformance``
treats them the same, except that the sibling "pyproject.toml" is
inherited by default.


Manifest Files
--------------

A manifest file identifies a set of benchmarks, as well as (optionally)
how they should be grouped.  ``pyperformance`` uses the manifest to
determine which benchmarks are available to run (and thus which to run
by default).

A manifest normally looks like this::

    [benchmarks]
    
    name	metafile
    bench1	somedir/bm_bench1/pyproject.toml
    bench2	somedir/pyproject.toml
    bench3	../anotherdir

The "benchmarks" section is a table with rows of tab-separated-values.
The "name" value is how ``pyperformance`` will identify the benchmark.
The "metafile" value is where ``pyperformance`` will look for the
benchmark's metadata.  If a metafile is a directory then it looks
for "pyproject.toml" in that directory.


Benchmark Groups
^^^^^^^^^^^^^^^^

The other sections in the manifest file relate to grouping::

    [benchmarks]
    
    name	metafile
    bench1	somedir/bm_bench1
    bench2	somedir/bm_bench2
    bench3	anotherdir/mybench.toml
    
    [groups]
    tag1
    tag2
    
    [group default]
    bench2
    bench3
    
    [group tricky]
    bench2

The "groups" section specifies available groups that may be identified
by benchmark tags (see about tags in the metadata section below).  Any
other group sections in the manifest are automatically added to the list
of available groups.

If no "default" group is specified then one is automatically added with
all benchmarks from the "benchmarks" section in it.  If there is no
"groups" section and no individual group sections (other than "default")
then the set of all tags of the known benchmarks is treated as "groups".
A group named "all" as also automatically added which has all known
benchmarks in it.

Benchmarks can be excluded from a group by using a ``-`` (minus) prefix.
Any benchmark alraedy in the list (at that point) that matches will be
dropped from the list.  If the first entry in the section is an
exclusion then all known benchmarks are first added to the list
before the exclusion is applied.

For example::

    [benchmarks]
    
    name	metafile
    bench1	somedir/bm_bench1
    bench2	somedir/bm_bench2
    bench3	anotherdir/mybench.toml
    
    [group default]
    -bench1

This means by default only "bench2" and "bench3" are run.


Merging Manifests
^^^^^^^^^^^^^^^^^

To combine manifests, use the ``[includes]`` section in the manifest::

    [includes]
    project1/benchmarks/MANIFEST
    project2/benchmarks/MANIFEST
    <default>

Note that ``<default>`` is the same as including the manifest file
for the default pyperformance benchmarks.


A Local Benchmark Suite
^^^^^^^^^^^^^^^^^^^^^^^

Often a project will have more than one benchmark that it will treat
as a suite.  ``pyperformance`` handles this without any extra work.

In the dirctory holding the manifest file put all the benchmarks.  Then
put ``<local>`` in the "metafile" column, like this::

    [benchmarks]
    
    name	metafile
    bench1	<local>
    bench2	<local>
    bench3	<local>
    bench4	<local>
    bench5	<local>

It will look for ``DIR/bm_NAME/pyproject.toml``.

If there are also variants, identify the main benchmark
in the "metafile" value, like this::

    [benchmarks]
    
    name	metafile
    bench1	<local>
    bench2	<local>
    bench3	<local>
    variant1	<local:bench3>
    variant2	<local:bench3>

``pyperformance`` will look for ``DIR/bm_BASE/bm_NAME.toml``, where "BASE"
is the part after "local:".


A Project's Benchmark Suite
^^^^^^^^^^^^^^^^^^^^^^^^^^^

A Python project can identify its benchmark suite by putting the path
to the manifest file in the project's top-level pyproject.toml.
Additional manifests can be identified as well::

    [tool.pyperformance]
    manifest = "..."
    manifests = ["...", "..."]

(Reminder: that is the pyproject.toml, not the manifest file.)


Benchmark Metadata Files
------------------------

A benchmark's metadata file (usually pyproject.toml) follows the format
specified in `PEP 621 <https://www.python.org/dev/peps/pep-0621>`_ and
`PEP 518 <https://www.python.org/dev/peps/pep-0518>`_.  So there are two
supported sections in the file: "project" and "tool.pyperformance".

A typical metadata file will look something like this::

    [project]
    version = "0.9.1"
    dependencies = ["pyperf"]
    dynamic = ["name"]
    
    [tool.pyperformance]
    name = "my_benchmark"

A highly detailed one might look like this::

    [project]
    name = "pyperformance_bm_json_dumps"
    version = "0.9.1"
    description = "A benchmark for json.dumps()"
    requires-python = ">=3.8"
    dependencies = ["pyperf"]
    urls = {repository = "https://github.com/python/pyperformance"}
    dynamic = ["version"]
    
    [tool.pyperformance]
    name = "json_dumps"
    tags = "serialize"
    runscript = "bench.py"
    datadir = ".data-files/extras"
    extra_opts = ["--special"]


Inheritance
^^^^^^^^^^^

For one benchmark to inherit from another (or from common metadata),
the "inherits" field is available::

    [project]
    dependencies = ["pyperf"]
    dynamic = ["name", "version"]
    
    [tool.pyperformance]
    name = "my_benchmark"
    inherits = "../common.toml"

All values in either section of the inherited metadata are treated
as defaults, on top of which the current metadata is applied.  In the
above example, for instance, a value for "version" in common.toml would
be used here.

If the "inherits" value is a directory (even for "..") then
"base.toml" in that directory will be inherited.

For variants, the base pyproject.toml is the default value for "inherits".


Inferred Values
^^^^^^^^^^^^^^^

In some situations, omitted values will be inferred from other available
data (even for required fields).

* ``project.name`` <= ``tool.pyperformance.name``
* ``project.*`` <= inherited metadata (except for "name" and "dynamic")
* ``tool.pyperformance.name`` <= metadata filename
* ``tool.pyperformance.*`` <= inherited metadata (except for "name" and "inherits")

When the name is inferred from the filename for a regularly structured
benchmark, the "bm\_" prefix is removed from the benchmark's directory.
If it is a variant that prefix is removed from the metadata filename,
as well as the .toml suffix.


The ``[project]`` Section
^^^^^^^^^^^^^^^^^^^^^^^^^

==================== ===== === === === ===
field                type  R   T   B   D
==================== ===== === === === ===
project.name         str   X   X        
project.version      ver   X       X   X
project.dependencies [str]         X    
project.dynamic      [str]              
==================== ===== === === === ===

"R": required
"T": inferred from the tool section
"B": inferred from the inherited metadata
"D": for default benchmarks, inferred from pyperformance

"dynamic" is required by PEP 621 for when a field will be filled in
dynamically by the tool.  This is especially important for required
fields.

All other PEP 621 fields are optional (e.g. ``requires-python = ">=3.8"``,
``{repository = "https://github.com/..."}``).


The ``[tool.pyperformance]`` Section
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

=============== ===== === === ===
field           type  R   B   F 
=============== ===== === === ===
tool.name       str   X       X
tool.tags       [str]     X    
tool.extra_opts [str]     X    
tool.inherits   file           
tool.runscript  file      X    
tool.datadir    file      X    
=============== ===== === === ===

"R": required
"B": inferred from the inherited metadata
"F": inferred from filename

* tags: optional list of names to group benchmarks
* extra_opts: optional list of args to pass to ``tool.runscript``
* runscript: the benchmark script to use instead of run_benchmark.py.
