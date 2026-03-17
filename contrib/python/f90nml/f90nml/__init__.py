"""A Fortran 90 namelist parser and generator.

:copyright: Copyright 2014 Marshall Ward, see AUTHORS for details.
:license: Apache License, Version 2.0, see LICENSE for details.
"""
from f90nml.namelist import Namelist
from f90nml.parser import Parser
from f90nml.version import __version__


def read(nml_path):
    """Parse a Fortran namelist file and return its contents.

    File object usage:

    >>> with open(nml_path) as nml_file:
    >>>     nml = f90nml.read(nml_file)

    File path usage:

    >>> nml = f90nml.read(nml_path)

    This function is equivalent to the ``read`` function of the ``Parser``
    object.

    >>> parser = f90nml.Parser()
    >>> nml = parser.read(nml_file)
    """
    parser = Parser()
    return parser.read(nml_path)


def reads(nml_string):
    """Parse a Fortran namelist string and return its contents.

    >>> nml_str = '&data_nml x=1 y=2 /'
    >>> nml = f90nml.reads(nml_str)

    This function is equivalent to the ``reads`` function of the ``Parser``
    object.

    >>> parser = f90nml.Parser()
    >>> nml = parser.reads(nml_str)
    """
    parser = Parser()
    return parser.reads(nml_string)


def write(nml, nml_path, force=False, sort=False):
    """Save a namelist to disk using either a file object or its file path.

    File object usage:

    >>> with open(nml_path, 'w') as nml_file:
    >>>     f90nml.write(nml, nml_file)

    File path usage:

    >>> f90nml.write(nml, 'data.nml')

    This function is equivalent to the ``write`` function of the ``Namelist``
    object ``nml``.

    >>> nml.write('data.nml')

    By default, ``write`` will not overwrite an existing file.  To override
    this, use the ``force`` flag.

    >>> nml.write('data.nml', force=True)

    To alphabetically sort the ``Namelist`` keys, use the ``sort`` flag.

    >>> nml.write('data.nml', sort=True)
    """
    # Promote dicts to Namelists
    if not isinstance(nml, Namelist) and isinstance(nml, dict):
        nml_in = Namelist(nml)
    else:
        nml_in = nml

    nml_in.write(nml_path, force=force, sort=sort)


def patch(nml_path, nml_patch, out_path=None):
    """Create a new namelist based on an input namelist and reference dict.

    >>> f90nml.patch('data.nml', nml_patch, 'patched_data.nml')

    This function is equivalent to the ``read`` function of the ``Parser``
    object with the patch output arguments.

    >>> parser = f90nml.Parser()
    >>> nml = parser.read('data.nml', nml_patch, 'patched_data.nml')

    A patched namelist file will retain any formatting or comments from the
    original namelist file.  Any modified values will be formatted based on the
    settings of the ``Namelist`` object.
    """
    parser = Parser()

    return parser.read(nml_path, nml_patch, out_path)
