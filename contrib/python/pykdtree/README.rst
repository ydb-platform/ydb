.. image:: https://github.com/storpipfugl/pykdtree/actions/workflows/deploy-wheels.yml/badge.svg?branch=master
    :target: https://github.com/storpipfugl/pykdtree/actions/workflows/deploy-wheels.yml

========
pykdtree
========

Objective
---------
pykdtree is a kd-tree implementation for fast nearest neighbour search in Python.
The aim is to be the fastest implementation around for common use cases (low dimensions and low number of neighbours) for both tree construction and queries.

The implementation is based on scipy.spatial.cKDTree and libANN by combining the best features from both and focus on implementation efficiency.

The interface is similar to that of scipy.spatial.cKDTree except only Euclidean distance measure is supported.

Queries are optionally multithreaded using OpenMP.

Installation
------------

Pykdtree can be installed via pip:

.. code-block:: bash

    pip install pykdtree
    
Or, if in a conda-based environment, with conda from the conda-forge channel:

.. code-block:: bash

    conda install -c conda-forge pykdtree
    
Note that by default these packages (the binary wheels on PyPI and the binary
package on conda-forge) are only built with OpenMP for linux platforms.
To attempt to build from source with OpenMP support do:

.. code-block:: bash

    export USE_OMP="probe"
    pip install --no-binary pykdtree pykdtree
    
This may not work on some systems that don't have OpenMP installed. See the below development
instructions for more guidance. Disabling OpenMP can be accomplished by setting `USE_OMP` to ``"0"``
in the above commands.

Development Installation
------------------------

If you wish to contribute to pykdtree then it is a good idea to install from source
so you can quickly see the effects of your changes.
By default pykdtree is built with OpenMP enabled queries on unix-like systems.
On linux this is done using libgomp. On OSX systems OpenMP is provided using the
clang compiler (conda environments use a separate compiler).

.. code-block:: bash

    $ cd <pykdtree_dir>
    $ pip install -e .

This installs pykdtree in an "editable" mode where changes to the Python files
are automatically reflected when running a new python interpreter instance
(ex. running a python script that uses pykdtree). It does not automatically rebuild
or recompile the `.mako` templates and `.pyx` Cython code in pykdtree. Editing
these files requires running the `pykdtree/render_template.py` script and then
rerunning the pip command above to recompile the Cython files.

If installation fails with undefined compiler flags or you want to use another OpenMP
implementation you may need to modify setup.py or specify additional pip command line
flags to match the library locations on your system.

Building without OpenMP support is controlled by the USE_OMP environment variable

.. code-block:: bash

    $ cd <pykdtree_dir>
    $ export USE_OMP=0
    $ pip install -e .

Note evironment variables are by default not exported when using sudo so in this case do

.. code-block:: bash

    $ USE_OMP=0 sudo -E pip install -e .


Control OpenMP usage
^^^^^^^^^^^^^^^^^^^^

The ``USE_OMP`` variable can be set to one of a couple different options. If
set to ``"probe"``, the installation process (``setup.py``) will attempt to
determine what variant of OpenMP is available based on the compiler being used,
the platform being run on, and the Python environment being run with. It will
then use the flags specified by one of the other ``USE_OMP`` modes. Note that
in the case of MacOS, it will also try to identify if OpenMP is available from
macports or homebrew and include the necessary include and library paths.

If set to ``"gcc"`` or ``"gomp"`` then compiler and linking flags will be set
appropriately for "GNU OpenMP" (gomp) library. If set to ``"clang"`` or 
``"omp"`` then the flags will be set to support the "omp" library. If set to
``"msvc"`` then flags will be set for the Microsoft Visual C++ compiler's
OpenMP variant. For backwards compatibility the previous ``"1"`` has the same
behavior as ``"probe"``. As mentioned above ``"0"`` can be used to disable
any detection of OpenMP or attempt to compile with it.

Usage
-----

The usage of pykdtree is similar to scipy.spatial.cKDTree so for now refer to its documentation

    >>> from pykdtree.kdtree import KDTree
    >>> kd_tree = KDTree(data_pts)
    >>> dist, idx = kd_tree.query(query_pts, k=8)

The number of threads to be used in OpenMP enabled queries can be controlled with the standard OpenMP environment variable OMP_NUM_THREADS.

The **leafsize** argument (number of data points per leaf) for the tree creation can be used to control the memory overhead of the kd-tree. pykdtree uses a default **leafsize=16**.
Increasing **leafsize** will reduce the memory overhead and construction time but increase query time.

pykdtree accepts data in double precision (numpy.float64) or single precision (numpy.float32) floating point. If data of another type is used an internal copy in double precision is made resulting in a memory overhead. If the kd-tree is constructed on single precision data the query points must be single precision as well.

Free-threading (no GIL) support
-------------------------------

Pykdtree is compiled with the necessary flags to be run from a free-threaded
Python interpreter. That is, it can be called without the GIL. Once a
``KDTree`` is constructed all state is stored internal to the object. Querying
the ``KDTree`` object can be done from multiple threads simultaneously.
``pykdtree`` has never acquired the GIL for low-level operations so performance
improvements are expected to be minimal on a free-threaded interpreter.

Any issues using ``pykdtree`` with free-threading should be filed as a GitHub
issue.

Multi-threading Gotchas
-----------------------

If using pykdtree from a multi-worker configuration, for example with the
``dask`` library, take care to control the number of dask and OpenMP workers.
On builds of pykdtree with OpenMP support (see "Control OpenMP usage" above),
OpenMP will default to one worker thread per logical core on your system. Dask
and libraries like it also tend to default to one worker thread per logical core.
These libraries can conflict resulting in cases like a dask worker thread
using pykdtree triggering OpenMP to create its workers. This has the potential of
creating N * N worker threads which can slow down your system as it tries to
manage and schedule that many threads.

In situations like this it is recommended to limit OpenMP to 1 or 2 workers by
defining the environment variable:

.. code-block:: bash

   OMP_NUM_THREADS=1

This essentially shifts the parallelism responsibility to the high-level dask
library rather than the low-level OpenMP library.

Benchmarks
----------
Comparison with scipy.spatial.cKDTree and libANN. This benchmark is on geospatial 3D data with 10053632 data points and 4276224 query points. The results are indexed relative to the construction time of scipy.spatial.cKDTree. A leafsize of 10 (scipy.spatial.cKDTree default) is used.

Note: libANN is *not* thread safe. In this benchmark libANN is compiled with "-O3 -funroll-loops -ffast-math -fprefetch-loop-arrays" in order to achieve optimum performance.

==================  =====================  ======  ========  ==================
Operation           scipy.spatial.cKDTree  libANN  pykdtree  pykdtree 4 threads
------------------  ---------------------  ------  --------  ------------------

Construction                          100     304        96                  96

query 1 neighbour                    1267     294       223                  70

Total 1 neighbour                    1367     598       319                 166

query 8 neighbours                   2193     625       449                 143

Total 8 neighbours                   2293     929       545                 293
==================  =====================  ======  ========  ==================

Looking at the combined construction and query this gives the following performance improvement relative to scipy.spatial.cKDTree

==========  ======  ========  ==================
Neighbours  libANN  pykdtree  pykdtree 4 threads
----------  ------  --------  ------------------
1            129%      329%                723%

8            147%      320%                682%
==========  ======  ========  ==================

Note: mileage will vary with the dataset at hand and computer architecture.

Test
----
Run the unit tests using pytest

.. code-block:: bash

    $ cd <pykdtree_dir>
    $ pytest

Installing on AppVeyor
----------------------

Pykdtree requires the "stdint.h" header file which is not available on certain
versions of Windows or certain Windows compilers including those on the
continuous integration platform AppVeyor. To get around this the header file(s)
can be downloaded and placed in the correct "include" directory. This can
be done by adding the `anaconda/missing-headers.ps1` script to your repository
and running it the install step of `appveyor.yml`:

    # install missing headers that aren't included with MSVC 2008
    # https://github.com/omnia-md/conda-recipes/pull/524
    - "powershell ./appveyor/missing-headers.ps1"

In addition to this, AppVeyor does not support OpenMP so this feature must be
turned off by adding the following to `appveyor.yml` in the
`environment` section:

    environment:
      global:
        # Don't build with openmp because it isn't supported in appveyor's compilers
        USE_OMP: "0"
