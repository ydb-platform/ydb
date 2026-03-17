=========================================================
f90nml - A Fortran namelist parser, generator, and editor
=========================================================

A Python module and command line tool for parsing Fortran namelist files

.. image:: https://ci.appveyor.com/api/projects/status/bcugyoqxiyyvemy8?svg=true
   :target: https://ci.appveyor.com/project/marshallward/f90nml

.. image:: https://coveralls.io/repos/marshallward/f90nml/badge.svg?branch=master
   :target: https://coveralls.io/r/marshallward/f90nml?branch=master

.. image:: https://zenodo.org/badge/DOI/10.5281/zenodo.3245482.svg
   :target: https://doi.org/10.5281/zenodo.3245482


Documentation
=============

The documentation for ``f90nml`` is available from Read The Docs.

   http://f90nml.readthedocs.org/en/latest/


About f90nml
============

``f90nml`` is a Python module and command line tool that provides a simple
interface for the reading, writing, and modifying Fortran namelist files.

A namelist file is parsed and converted into a ``Namelist`` object, which
behaves like a Python ``dict``.  Values are converted from Fortran data types
to equivalent primitive Python types.

The command line tool ``f90nml`` can be used to modify individual values inside
of a shell environment.  It can also be used to convert the data between
namelists and other configuration formats.  JSON and YAML formats are currently
supported.


Quick usage guide
=================

To read a namelist file ``sample.nml`` which contains the following namelists:

.. code-block:: fortran

   &config_nml
      input = 'wind.nc'
      steps = 864
      layout = 8, 16
      visc = 1.0e-4
      use_biharmonic = .false.
   /

we would use the following script:

.. code:: python

   import f90nml
   nml = f90nml.read('sample.nml')

which would would point ``nml`` to the following ``dict``:

.. code:: python

   nml = {
       'config_nml': {
           'input': 'wind.nc',
           'steps': 864,
           'layout': [8, 16],
           'visc': 0.0001,
           'use_biharmonic': False
       }
   }

File objects can also be used as inputs:

.. code:: python

   with open('sample.nml') as nml_file:
       nml = f90nml.read(nml_file)

To modify one of the values, say ``steps``, and save the output, manipulate the
``nml`` contents and write to disk using the ``write`` function:

.. code:: python

   nml['config_nml']['steps'] = 432
   nml.write('new_sample.nml')

Namelists can also be saved to file objects:

.. code:: python

   with open('target.nml') as nml_file:
      nml.write(nml_file)

To modify a namelist but preserve its comments and formatting, create a
namelist patch and apply it to a target file using the ``patch`` function:

.. code:: python

   patch_nml = {'config_nml': {'visc': 1e-6}}
   f90nml.patch('sample.nml', patch_nml, 'new_sample.nml')


Command line interface
----------------------

A command line tool is provided to manipulate namelist files within the shell:

.. code:: sh

   $ f90nml config.nml -g config_nml -v steps=432

.. code-block:: fortran

   &config_nml
      input = 'wind.nc'
      steps = 432
      layout = 8, 16
      visc = 1.0e-4
      use_biharmonic = .false.
   /

See the documentation for details.


Installation
============

``f90nml`` is available on PyPI and can be installed via pip::

   $ pip install f90nml

The latest version of ``f90nml`` can be installed from source::

   $ git clone https://github.com/marshallward/f90nml.git
   $ cd f90nml
   $ pip install .

conda
-----

There is a conda-forge feedstock (not maintained by the author)::

    $ conda install -c conda-forge f90nml

Information on supported versions and platforms, and detailed installation
instructions using ``conda`` and ``conda-forge`` is available here:

   https://github.com/conda-forge/f90nml-feedstock


Package distribution
--------------------

``f90nml`` is not distributed through any official packaging tools, but it is
available on Arch Linux via the AUR::

   $ git clone https://aur.archlinux.org/python-f90nml.git
   $ cd python-f90nml
   $ makepkg -sri

Volunteers are welcome to submit and maintain ``f90nml`` on other
distributions.


Local install
-------------

Users without install privileges can append the ``--user`` flag to ``pip`` from
the top ``f90nml`` directory::

   $ pip install --user .

If pip is not available, then ``setup.py`` can still be used::

   $ python setup.py install --user

When using ``setup.py`` locally, some users have reported that ``--prefix=``
may need to be appended to the command::

   $ python setup.py install --user --prefix=


YAML support
------------

The command line tool offers support for conversion between namelists and YAML
formatted output.  If PyYAML is already installed, then no other steps are
required.  To require YAML support, install the ``yaml`` extras package::

   $ pip install f90nml[yaml]

To install as a user::

   $ pip install --user .[yaml]


Contributing to ``f90nml``
==========================

Users are welcome to submit bug reports, feature requests, and code
contributions to this project through GitHub.  More information is available in
the `Contributing`_ guidelines.

.. _Contributing: http://f90nml.readthedocs.org/en/latest/contributing.html
