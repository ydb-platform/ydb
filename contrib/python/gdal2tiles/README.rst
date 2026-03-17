==========
gdal2tiles
==========


.. image:: https://img.shields.io/pypi/v/gdal2tiles.svg
        :target: https://pypi.python.org/pypi/gdal2tiles

.. image:: https://img.shields.io/travis/tehamalab/gdal2tiles.svg
        :target: https://travis-ci.org/tehamalab/gdal2tiles

.. image:: https://readthedocs.org/projects/gdal2tiles/badge/?version=latest
        :target: https://gdal2tiles.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


A python library for generating map tiles inspired by gdal2tiles.py_ from GDAL_ project.


Dependancies
------------

- GDAL_ development header files, sometimes available as `libgdal-dev` or `libgdal-devel` packages.


Installation
------------

To install gdal2tiles library you can use pip:

.. code-block:: console

    $ pip install gdal2tiles


Basic usage
-----------

.. code-block:: python

    import gdal2tiles

    gdal2tiles.generate_tiles('/path/to/input_file', '/path/to/output_dir/')


You can also pass various keyword as optional keyword arguments to `generate_tiles()` function.
For example

.. code-block:: python

    gdal2tiles.generate_tiles('input_file', 'output_dir/', nb_processes=2, zoom='7-9')

OR

.. code-block:: python

    options = {'zoom': (7, 9), 'resume': True}
    gdal2tiles.generate_tiles('input_file', 'output_dir/', **options)


In general

.. code-block:: python

    gdal2tiles.generate_tiles(input_file, output_folder, **options)


Arguments:
    ``input_file`` *(str)*: Path to input file.

    ``output_folder`` *(str)*: Path to output folder.

    ``options``: Tile generation options.


Options:
    ``profile`` *(str)*: Tile cutting profile (mercator,geodetic,raster) - default
        'mercator' (Google Maps compatible)

    ``resampling`` *(str)*: Resampling method (average,near,bilinear,cubic,cubicsp
        line,lanczos,antialias) - default 'average'

    ``s_srs``: The spatial reference system used for the source input data

    ``zoom``: Zoom levels to render; format: '[int min, int max]',
        'min-max' or 'int/str zoomlevel'.

    ``tile_size`` *(int)*: Size of tiles to render - default 256

    ``resume`` *(bool)*: Resume mode. Generate only missing files.

    ``srcnodata``: NODATA transparency value to assign to the input data

    ``tmscompatible`` *(bool)*: When using the geodetic profile, specifies the base
        resolution as 0.703125 or 2 tiles at zoom level 0.

    ``verbose`` *(bool)*: Print status messages to stdout

    ``kml`` *(bool)*: Generate KML for Google Earth - default for 'geodetic'
                    profile and 'raster' in EPSG:4326. For a dataset with
                    different projection use with caution!

    ``url`` *(str)*: URL address where the generated tiles are going to be published

    ``webviewer`` *(str)*: Web viewer to generate (all,google,openlayers,none) -
        default 'all'

    ``title`` *(str)*: Title of the map

    ``copyright`` *(str)*: Copyright for the map

    ``googlekey`` (str): Google Maps API key from
        http://code.google.com/apis/maps/signup.html

    ``bingkey`` *(str)*: Bing Maps API key from https://www.bingmapsportal.com/

    ``nb_processes`` *(int)*: Number of processes to use for tiling.


.. _gdal2tiles.py: http://www.gdal.org/gdal2tiles.html
.. _GDAL: http://www.gdal.org/
