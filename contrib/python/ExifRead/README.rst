*******
EXIF.py
*******

.. image:: https://img.shields.io/github/license/ianare/exif-py
    :target: https://opensource.org/license/bsd-3-clause
    :alt: BSD-3-clause
.. image:: https://img.shields.io/pypi/v/ExifRead
    :target: https://pypi.org/project/ExifRead
    :alt: PyPi
.. image:: https://img.shields.io/pypi/dm/ExifRead
    :target: https://pypi.org/project/ExifRead
    :alt: BSD-3-clause
.. image:: http://www.mypy-lang.org/static/mypy_badge.svg
    :target: http://mypy-lang.org/
    :alt: Checked with mypy
.. image:: https://img.shields.io/github/actions/workflow/status/ianare/exif-py/test.yml
    :target: https://github.com/ianare/exif-py
    :alt: Tests

|

Easy to use Python module to extract Exif metadata from digital image files.

Pure Python, lightweight, no dependencies.

Supported formats: TIFF, JPEG, JPEG XL, PNG, Webp, HEIC, RAW


Compatibility
*************

EXIF.py is tested and officially supported on Python 3.7 to 3.13


Installation
************

Stable Version
==============
The recommended process is to install the `PyPI package <https://pypi.python.org/pypi/ExifRead>`_,
as it allows easily staying up to date::

    $ pip install exifread

See the `pip documentation <https://pip.pypa.io/en/latest/user_guide.html>`_ for more info.

EXIF.py is mature software and strives for stability.

Development Version
===================

After cloning the repo, use the provided Makefile::

  make venv install-all

Which will create a virtual environment and install development dependencies.

Usage
*****

Command line
============

Some examples::

    EXIF.py image1.jpg
    EXIF.py -dc image1.jpg image2.tiff
    find ~/Pictures -name "*.jpg" -o -name "*.tiff" | xargs EXIF.py

Show command line options::

    EXIF.py -h

Python Script
=============

.. code-block:: python

    import exifread

    # Open image file for reading (must be in binary mode)
    with open(file_path, "rb") as file_handle:

        # Return Exif tags
        tags = exifread.process_file(file_handle)

*Note:* To use this library in your project as a Git submodule, you should::

    from <submodule_folder> import exifread

Returned tags will be a dictionary mapping names of Exif tags to their
values in the file named by ``file_path``.
You can process the tags as you wish. In particular, you can iterate through all the tags with:

.. code-block:: python

    for tag, value in tags.items():
        if tag not in ('JPEGThumbnail', 'TIFFThumbnail', 'Filename', 'EXIF MakerNote'):
            print(f"Key: {tag}, value {value}")

An ``if`` statement is used to avoid printing out a few of the tags that tend to be long or boring.

The tags dictionary will include keys for all of the usual Exif tags, and will also include keys for
Makernotes used by some cameras, for which we have a good specification.

Note that the dictionary keys are the IFD name followed by the tag name. For example::

    'EXIF DateTimeOriginal', 'Image Orientation', 'MakerNote FocusMode'


Tag Descriptions
****************

Tags are divided into these main categories:

- ``Image``: information related to the main image (IFD0 of the Exif data).
- ``Thumbnail``: information related to the thumbnail image, if present (IFD1 of the Exif data).
- ``EXIF``: Exif information (sub-IFD).
- ``GPS``: GPS information (sub-IFD).
- ``Interoperability``: Interoperability information (sub-IFD).
- ``MakerNote``: Manufacturer specific information. There are no official published references for these tags.


Processing Options
******************

These options can be used both in command line mode and within a script.

Faster Processing
=================

Don't process makernote tags, don't extract the thumbnail image (if any).

Pass the ``-q`` or ``--quick`` command line arguments, or as:

.. code-block:: python

    tags = exifread.process_file(
        file_handle, details=False, extract_thumbnail=False
    )

To process makernotes only, without extracting the thumbnail image (if any):

.. code-block:: python

    tags = exifread.process_file(
        file_handle, details=True, extract_thumbnail=False
    )

To extract the thumbnail image (if any), without processing makernotes:

.. code-block:: python

    tags = exifread.process_file(
        file_handle, details=False, extract_thumbnail=True
    )

Stop at a Given Tag
===================

To stop processing the file after a specified tag is retrieved.

Pass the ``-t TAG`` or ``--stop-tag TAG`` argument, or as:

.. code-block:: python

    tags = exifread.process_file(file_handle, stop_tag='TAG')

where ``TAG`` is a valid tag name without the IFD, ex ``'DateTimeOriginal'``.

*The two above options are useful to speed up processing of large numbers of files.*

Strict Processing
=================

Return an error on invalid tags instead of silently ignoring.

Pass the ``-s`` or ``--strict`` argument, or as:

.. code-block:: python

    tags = exifread.process_file(file_handle, strict=True)

Built-in Types
==============

For easier serialization and programmatic use, this option returns a dictionary with values in built-in Python types
(int, float, str, bytes, list, None) instead of `IfdTag` objects.

Pass the ``-b`` or ``--builtin`` argument, or as:

.. code-block:: python

    tags = exifread.process_file(file_handle, builtin_types=True)

For direct JSON serialization, combine this option with ``details=False`` to avoid bytes in the output:

.. code-block:: python

    json.dumps(
        exifread.process_file(file_handle, details=False, builtin_types=True)
    )

Usage Example
=============

This example shows how to use the library to correct the orientation of an image
(using Pillow for the transformation) before e.g. displaying it.

.. code-block:: python

    import exifread
    from PIL import Image
    import logging

    def _read_img_and_correct_exif_orientation(path):
        im = Image.open(path)
        tags = {}
        with open(path, "rb") as file_handle:
            tags = exifread.process_file(file_handle, details=False)

        if "Image Orientation" in tags:
            orientation = tags["Image Orientation"]
            logging.basicConfig(level=logging.DEBUG)
            logging.debug("Orientation: %s (%s)", orientation, orientation.values)
            val = orientation.values
            if 2 in val:
                val += [4, 3]
            if 5 in val:
                val += [4, 6]
            if 7 in val:
                val += [4, 8]
            if 3 in val:
                logging.debug("Rotating by 180 degrees.")
                im = im.transpose(Image.ROTATE_180)
            if 4 in val:
                logging.debug("Mirroring horizontally.")
                im = im.transpose(Image.FLIP_TOP_BOTTOM)
            if 6 in val:
                logging.debug("Rotating by 270 degrees.")
                im = im.transpose(Image.ROTATE_270)
            if 8 in val:
                logging.debug("Rotating by 90 degrees.")
                im = im.transpose(Image.ROTATE_90)
        return im


License
*******

Copyright © 2002-2007 Gene Cash

Copyright © 2007-2025 Ianaré Sévi and contributors

A **huge** thanks to all the contributors over the years!

Originally written by Gene Cash & Thierry Bousch.

Available as open source under the terms of the **BSD-3-Clause license**.

See the LICENSE file for details.
