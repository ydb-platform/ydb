imagesize
=============

.. image:: https://travis-ci.org/shibukawa/imagesize_py.svg?branch=master
    :target: https://travis-ci.org/shibukawa/imagesize_py

This module analyzes JPEG/JPEG 2000/PNG/GIF/TIFF/SVG/Netpbm/WebP image headers and returns image size or DIP.

.. code:: python

   import imagesize

   width, height = imagesize.get("test.png")
   print(width, height)

   width, height = imagesize.getDPI("test.png")
   print(width, height)

This module is a pure Python module. You can use file like object like file or something like ``io.BytesIO``.

API
-----

* ``imagesize.get(filepath)``

  Returns image size (width, height).

* ``imagesize.getDPI(filepath)``

  Returns image DPI (width, height).

Benchmark
------------

It only parses headers, and ignores pixel data. So it is much faster than Pillow.

.. list-table::
   :header-rows: 1

   - * module
     * result
   - * imagesize (pure Python)
     * 1.077 seconds per 100 000 times
   - * Pillow
     * 10.569 seconds per 100 000 times

I tested on MacBookPro (2014/Core i7) with 125kB PNG files.

Development
---------------

Run test with the following command:

.. code:: bash

   python -m unittest

License
-----------

MIT License

Thanks
----------

I referred to the following code:

* http://markasread.net/post/17551554979/get-image-size-info-using-pure-python-code
* https://stackoverflow.com/questions/8032642/how-to-obtain-image-size-using-standard-python-class-without-using-external-lib

I use sample image from here:

* https://www.nightprogrammer.org/development/multipage-tiff-example-download-test-image-file/

Thank you for feedback:

* tk0miya (https://github.com/tk0miya)
* shimizukawa (https://github.com/shimizukawa)
* xantares (https://github.com/xantares)
* Ivan Zakharyaschev (https://github.com/imz)
* Jon Dufresne (https://github.com/jdufresne)
* Geoff Lankow (https://github.com/darktrojan)
* Hugo (https://github.com/hugovk)
* Jack Cherng (https://github.com/jfcherng)
* Tyler A. Young (https://github.com/s3cur3)
* Mark Browning (https://github.com/mabrowning)
* ossdev07 (https://github.com/ossdev07)
* Nicholas-Schaub (https://github.com/Nicholas-Schaub)
* Nuffknacker (https://github.com/Nuffknacker) 
* Hannes Römer (https://github.com/hroemer)
* mikey (https://github.com/ffreemt)
* Marco (https://github.com/marcoffee)
* ExtReMLapin (https://github.com/ExtReMLapin)
