==============
timezonefinder
==============


..
    Note: can't include the badges file from the docs here, as it won't render on PyPI -> sync manually

.. image:: https://github.com/jannikmi/timezonefinder/actions/workflows/build.yml/badge.svg?branch=master
    :target: https://github.com/jannikmi/timezonefinder/actions?query=branch%3Amaster

.. image:: https://readthedocs.org/projects/timezonefinder/badge/?version=latest
    :alt: documentation status
    :target: https://timezonefinder.readthedocs.io/en/latest/?badge=latest

.. image:: https://img.shields.io/pypi/wheel/timezonefinder.svg
    :target: https://pypi.python.org/pypi/timezonefinder

.. image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit

.. image:: https://pepy.tech/badge/timezonefinder
    :alt: total PyPI downloads
    :target: https://pepy.tech/project/timezonefinder

.. image:: https://img.shields.io/pypi/v/timezonefinder.svg
    :alt: latest version on PyPI
    :target: https://pypi.python.org/pypi/timezonefinder

.. image:: https://img.shields.io/conda/vn/conda-forge/timezonefinder.svg
   :target: https://anaconda.org/conda-forge/timezonefinder
   :alt: latest version on conda-forge

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black




Notice: Looking for maintainers. Reach out if you want to contribute!
---------------------------------------------------------------------


This is a python package for looking up the corresponding timezone for given coordinates on earth entirely offline.


Quick Guide:

.. code-block:: console

    pip install timezonefinder


.. code-block:: python

    from timezonefinder import TimezoneFinder

    tf = TimezoneFinder()  # reuse

    query_points = [(13.358, 52.5061), ...]
    for lng, lat in query_points:
        tz = tf.timezone_at(lng=lng, lat=lat)  # 'Europe/Berlin'


For more refer to the `Documentation <https://timezonefinder.readthedocs.io/en/latest/>`__.

Also check:

`PyPI <https://pypi.python.org/pypi/timezonefinder/>`__

`online GUI and API <https://timezonefinder.michelfe.it>`__

`conda-forge feedstock <https://github.com/conda-forge/timezonefinder-feedstock>`__

ruby port: `timezone_finder <https://github.com/gunyarakun/timezone_finder>`__

`download stats <https://pepy.tech/project/timezonefinder>`__
