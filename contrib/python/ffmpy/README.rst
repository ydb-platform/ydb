.. image:: https://github.com/Ch00k/ffmpy/workflows/ci/badge.svg
    :target: https://github.com/Ch00k/ffmpy/actions
    :alt: Tests

.. image:: https://codecov.io/gh/Ch00k/ffmpy/branch/master/graphs/badge.svg
    :target: https://codecov.io/github/Ch00k/ffmpy
    :alt: Coverage

.. image:: https://readthedocs.org/projects/ffmpy/badge/?version=latest
    :target: http://ffmpy.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation

.. image:: https://img.shields.io/pypi/v/ffmpy.svg
    :target: https://pypi.python.org/pypi/ffmpy
    :alt: Latest version


ffmpy
=====
*ffmpy* is a simple `FFmpeg <http://ffmpeg.org/>`_ command line wrapper. It implements a Pythonic interface for FFmpeg command line compilation and uses Python's `subprocess <https://docs.python.org/2/library/subprocess.html>`_ to execute the compiled command line.

*ffmpy* requires Python 3.9 or greater.

Installation
------------
::

  pip install ffmpy

Quick example
-------------
.. code:: python

  from ffmpy import FFmpeg
  ff = FFmpeg(
      inputs={'input.mp4': None},
      outputs={'output.avi': None}
  )
  ff.run()

This will take the ``input.mp4`` file in the current directory as the input, change the video container from MP4 to AVI without changing any other video parameters, and create a new output file ``output.avi`` in the current directory.

Documentation
-------------
https://ffmpy.readthedocs.io/

See `Examples <https://ffmpy.readthedocs.io/latest/examples.html>`_ section for usage examples.

License
-------
ffmpy is licensed under the terms of MIT license
