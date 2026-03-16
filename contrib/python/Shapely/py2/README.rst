=======
Shapely
=======

|travis| |appveyor| |coveralls|

.. |travis| image:: https://travis-ci.org/Toblerity/Shapely.svg?branch=maint-1.7
   :target: https://travis-ci.org/Toblerity/Shapely

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/Toblerity/Shapely?branch=maint-1.7&svg=true
   :target: https://ci.appveyor.com/project/frsci/shapely?branch=maint-1.7

.. |coveralls| image:: https://coveralls.io/repos/github/Toblerity/Shapely/badge.svg?branch=maint-1.7
   :target: https://coveralls.io/github/Toblerity/Shapely?branch=maint-1.7

Manipulation and analysis of geometric objects in the Cartesian plane.

.. image:: https://c2.staticflickr.com/6/5560/31301790086_b3472ea4e9_c.jpg
   :width: 800
   :height: 378

Shapely is a BSD-licensed Python package for manipulation and analysis of
planar geometric objects. It is based on the widely deployed `GEOS
<https://trac.osgeo.org/geos/>`__ (the engine of `PostGIS
<http://postgis.org>`__) and `JTS
<https://locationtech.github.io/jts/>`__ (from which GEOS is ported)
libraries. Shapely is not concerned with data formats or coordinate systems,
but can be readily integrated with packages that are. For more details, see:

* `Shapely GitHub repository <https://github.com/Toblerity/Shapely>`__
* `Shapely documentation and manual <https://shapely.readthedocs.io/en/latest/>`__

Usage
=====

Here is the canonical example of building an approximately circular patch by
buffering a point.

.. code-block:: pycon

    >>> from shapely.geometry import Point
    >>> patch = Point(0.0, 0.0).buffer(10.0)
    >>> patch
    <shapely.geometry.polygon.Polygon object at 0x...>
    >>> patch.area
    313.65484905459385

See the manual for more examples and guidance.

Requirements
============

Shapely 1.7 requires

* Python 2.7, >=3.5
* GEOS >=3.3

Installing Shapely
==================

Shapely may be installed from a source distribution or one of several kinds
of built distribution.

Built distributions
-------------------

Built distributions are the only option for users who do not have or do not
know how to use their platform's compiler and Python SDK, and a good option for
users who would rather not bother.

Linux, OS X, and Windows users can get Shapely wheels with GEOS included from the
Python Package Index with a recent version of pip (8+):

.. code-block:: console

    $ pip install shapely

Shapely is available via system package management tools like apt, yum, and
Homebrew, and is also provided by popular Python distributions like Canopy and
Anaconda. If you use the Conda package manager to install Shapely, be sure to
use the conda-forge channel.

Windows users have another good installation options: the wheels published at
https://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely. These can be installed
using pip by specifying the entire URL.

Source distributions
--------------------

If you want to build Shapely from source for compatibility with other modules
that depend on GEOS (such as cartopy or osgeo.ogr) or want to use a different
version of GEOS than the one included in the project wheels you should first
install the GEOS library, Cython, and Numpy on your system (using apt, yum,
brew, or other means) and then direct pip to ignore the binary wheels.

.. code-block:: console

    $ pip install shapely --no-binary shapely

If you've installed GEOS to a standard location, the geos-config program will
be used to get compiler and linker options. If geos-config is not on your
executable, it can be specified with a GEOS_CONFIG environment variable, e.g.:

.. code-block:: console

    $ GEOS_CONFIG=/path/to/geos-config pip install shapely

Integration
===========

Shapely does not read or write data files, but it can serialize and deserialize
using several well known formats and protocols. The shapely.wkb and shapely.wkt
modules provide dumpers and loaders inspired by Python's pickle module.

.. code-block:: pycon

    >>> from shapely.wkt import dumps, loads
    >>> dumps(loads('POINT (0 0)'))
    'POINT (0.0000000000000000 0.0000000000000000)'

Shapely can also integrate with other Python GIS packages using GeoJSON-like
dicts.

.. code-block:: pycon

    >>> import json
    >>> from shapely.geometry import mapping, shape
    >>> s = shape(json.loads('{"type": "Point", "coordinates": [0.0, 0.0]}'))
    >>> s
    <shapely.geometry.point.Point object at 0x...>
    >>> print(json.dumps(mapping(s)))
    {"type": "Point", "coordinates": [0.0, 0.0]}

Development and Testing
=======================

Dependencies for developing Shapely are listed in requirements-dev.txt. Cython
and Numpy are not required for production installations, only for development.
Use of a virtual environment is strongly recommended.

.. code-block:: console

    $ virtualenv .
    $ source bin/activate
    (env)$ pip install -r requirements-dev.txt
    (env)$ pip install -e .

The project uses pytest to run Shapely's suite of unittests and doctests.

.. code-block:: console

    (env)$ python -m pytest

Support
=======

Questions about using Shapely may be asked on the `GIS StackExchange
<https://gis.stackexchange.com/questions/tagged/shapely>`__ using the "shapely"
tag.

Bugs may be reported at https://github.com/Toblerity/Shapely/issues.
