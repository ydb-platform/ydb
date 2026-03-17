[![Install and Test Status](https://github.com/jswhit/pygrib/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/jswhit/pygrib/actions)
[![PyPI package](https://badge.fury.io/py/pygrib.svg)](http://python.org/pypi/pygrib)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/pygrib/badges/version.svg)](https://anaconda.org/conda-forge/pygrib)
[![DOI](https://zenodo.org/badge/28599617.svg)](https://zenodo.org/badge/latestdoi/28599617)

Provides a high-level interface to the ECWMF [ECCODES](https://confluence.ecmwf.int/display/ECC) C library for reading GRIB files.
There are limited capabilities for writing GRIB files (you can modify the contents of an existing file, but you can't create one from scratch).  See the online docs for 
[example usage](https://jswhit.github.io/pygrib/api.html#example-usage).

Quickstart
==========

The easiest way to get everything installed is to use [pip](https://py.pypa.io):

```
pip install pygrib
```

You can also use [conda](https://docs.conda.io/en/latest/):

```
conda install -c conda-forge pygrib
```

Alternately, clone the github repo and run `pip install -e .` (after setting `$ECCCODES_DIR`)
where `$ECCODES_DIR` is the path to the directory containing `include/grib_api.h`
and `lib/libeccodes.so`. If `ECCODES_DIR` is not specified, a few common locations
such as `$CONDA_PREFIX,/usr,/usr/local,/opt/local` will be searched.

For full installation instructions and API documentation, see https://jswhit.github.io/pygrib.

Sample [IPython](http://ipython.org/) notebooks illustrating pygrib usage: 
* http://nbviewer.jupyter.org/gist/jswhit/8635665
* https://github.com/scollis/HRRR/blob/master/notebooks/HRRR%20Grib.ipynb

Questions or problems use https://github.com/jswhit/pygrib/issues
