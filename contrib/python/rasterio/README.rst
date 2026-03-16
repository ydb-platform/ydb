========
Rasterio
========

Rasterio reads and writes geospatial raster data.

.. image:: https://github.com/rasterio/rasterio/actions/workflows/tests.yaml/badge.svg
   :target: https://github.com/rasterio/rasterio/actions/workflows/tests.yaml

.. image:: https://github.com/rasterio/rasterio/actions/workflows/test_gdal_latest.yaml/badge.svg
   :target: https://github.com/rasterio/rasterio/actions/workflows/test_gdal_latest.yaml

.. image:: https://github.com/rasterio/rasterio/actions/workflows/test_gdal_tags.yaml/badge.svg
   :target: https://github.com/rasterio/rasterio/actions/workflows/test_gdal_tags.yaml

.. image:: https://img.shields.io/pypi/v/rasterio
   :target: https://pypi.org/project/rasterio/

Geographic information systems use GeoTIFF and other formats to organize and
store gridded, or raster, datasets. Rasterio reads and writes these formats and
provides a Python API based on N-D arrays.

Rasterio 1.5+ works with Python >= 3.12, Numpy >= 2, and GDAL >= 3.8. Official
binary packages for Linux, macOS, and Windows with most built-in format drivers
plus HDF5, netCDF, and OpenJPEG2000 are available on PyPI.

Read the documentation for more details: https://rasterio.readthedocs.io/.

Example
=======

Here's an example of some basic features that Rasterio provides. Three bands
are read from an image and averaged to produce something like a panchromatic
band.  This new band is then written to a new single band TIFF.

.. code-block:: python

    import numpy as np
    import rasterio

    # Read raster bands directly to Numpy arrays.
    #
    with rasterio.open('tests/data/RGB.byte.tif') as src:
        r, g, b = src.read()

    # Combine arrays in place. Expecting that the sum will
    # temporarily exceed the 8-bit integer range, initialize it as
    # a 64-bit float (the numpy default) array. Adding other
    # arrays to it in-place converts those arrays "up" and
    # preserves the type of the total array.
    total = np.zeros(r.shape)

    for band in r, g, b:
        total += band

    total /= 3

    # Write the product as a raster band to a new 8-bit file. For
    # the new file's profile, we start with the meta attributes of
    # the source file, but then change the band count to 1, set the
    # dtype to uint8, and specify LZW compression.
    profile = src.profile
    profile.update(dtype=rasterio.uint8, count=1, compress='lzw')

    with rasterio.open('example-total.tif', 'w', **profile) as dst:
        dst.write(total.astype(rasterio.uint8), 1)

The output:

.. image:: http://farm6.staticflickr.com/5501/11393054644_74f54484d9_z_d.jpg
   :width: 640
   :height: 581

API Overview
============

Rasterio gives access to properties of a geospatial raster file.

.. code-block:: python

    with rasterio.open('tests/data/RGB.byte.tif') as src:
        print(src.width, src.height)
        print(src.crs)
        print(src.transform)
        print(src.count)
        print(src.indexes)

    # Printed:
    # (791, 718)
    # {u'units': u'm', u'no_defs': True, u'ellps': u'WGS84', u'proj': u'utm', u'zone': 18}
    # Affine(300.0379266750948, 0.0, 101985.0,
    #        0.0, -300.041782729805, 2826915.0)
    # 3
    # [1, 2, 3]

A rasterio dataset also provides methods for getting read/write windows (like
extended array slices) given georeferenced coordinates.

.. code-block:: python

    with rasterio.open('tests/data/RGB.byte.tif') as src:
        window = src.window(*src.bounds)
        print(window)
        print(src.read(window=window).shape)

    # Printed:
    # Window(col_off=0.0, row_off=0.0, width=791.0000000000002, height=718.0)
    # (3, 718, 791)

Rasterio CLI
============

Rasterio's command line interface, named "rio", is documented at `cli.rst
<https://github.com/rasterio/rasterio/blob/master/docs/cli.rst>`__. Its ``rio
insp`` command opens the hood of any raster dataset so you can poke around
using Python.

.. code-block:: pycon

    $ rio insp tests/data/RGB.byte.tif
    Rasterio 0.10 Interactive Inspector (Python 3.4.1)
    Type "src.meta", "src.read(1)", or "help(src)" for more information.
    >>> src.name
    'tests/data/RGB.byte.tif'
    >>> src.closed
    False
    >>> src.shape
    (718, 791)
    >>> src.crs
    {'init': 'epsg:32618'}
    >>> b, g, r = src.read()
    >>> b
    masked_array(data =
     [[-- -- -- ..., -- -- --]
     [-- -- -- ..., -- -- --]
     [-- -- -- ..., -- -- --]
     ...,
     [-- -- -- ..., -- -- --]
     [-- -- -- ..., -- -- --]
     [-- -- -- ..., -- -- --]],
                 mask =
     [[ True  True  True ...,  True  True  True]
     [ True  True  True ...,  True  True  True]
     [ True  True  True ...,  True  True  True]
     ...,
     [ True  True  True ...,  True  True  True]
     [ True  True  True ...,  True  True  True]
     [ True  True  True ...,  True  True  True]],
           fill_value = 0)

    >>> np.nanmin(b), np.nanmax(b), np.nanmean(b)
    (0, 255, 29.94772668847656)

Rio Plugins
-----------

Rio provides the ability to create subcommands using plugins.  See
`cli.rst <https://github.com/rasterio/rasterio/blob/master/docs/cli.rst#rio-plugins>`__
for more information on building plugins.

See the
`plugin registry <https://github.com/rasterio/rasterio/wiki/Rio-plugin-registry>`__
for a list of available plugins.


Installation
============

See `docs/installation.rst <docs/installation.rst>`__

Support
=======

The primary forum for questions about installation and usage of Rasterio is
https://rasterio.groups.io/g/main. The authors and other users will answer
questions when they have expertise to share and time to explain. Please take
the time to craft a clear question and be patient about responses.

Please do not bring these questions to Rasterio's issue tracker, which we want
to reserve for bug reports and other actionable issues.

Development and Testing
=======================

See `CONTRIBUTING.rst <CONTRIBUTING.rst>`__.

Documentation
=============

See `docs/ <docs/>`__.

License
=======

See `LICENSE.txt <LICENSE.txt>`__.

Authors
=======

The `rasterio` project was begun at Mapbox and was transferred to the `rasterio` Github organization in October 2021.

See `AUTHORS.txt <AUTHORS.txt>`__.

Changes
=======

See `CHANGES.txt <CHANGES.txt>`__.

Who is Using Rasterio?
======================

See `here <https://libraries.io/pypi/rasterio/usage>`__.
