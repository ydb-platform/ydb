# Author:  Lisandro Dalcin
# Contact: dalcinl@gmail.com
"""This is the **MPI for Python** package.

The *Message Passing Interface* (MPI) is a standardized and portable
message-passing system designed to function on a wide variety of
parallel computers. The MPI standard defines the syntax and semantics
of library routines and allows users to write portable programs in the
main scientific programming languages (Fortran, C, or C++). Since its
release, the MPI specification has become the leading standard for
message-passing libraries for parallel computers.

*MPI for Python* provides MPI bindings for the Python programming
language, allowing any Python program to exploit multiple processors.
This package build on the MPI specification and provides an object
oriented interface which closely follows MPI-2 C++ bindings.

"""

__version__ = '3.1.6'
__author__ = 'Lisandro Dalcin'
__credits__ = 'MPI Forum, MPICH Team, Open MPI Team'


__all__ = ['MPI']


class Rc:
    """Runtime configuration options.

    Attributes
    ----------
    initialize : bool
        Automatic MPI initialization at import (default: True).
    threads : bool
        Request initialization with thread support (default: True).
    thread_level : {"multiple", "serialized", "funneled", "single"}
        Level of thread support to request (default: "multiple").
    finalize : None or bool
        Automatic MPI finalization at exit (default: None).
    fast_reduce : bool
        Use tree-based reductions for objects (default: True).
    recv_mprobe : bool
        Use matched probes to receive objects (default: True).
    errors : {"exception", "default", "fatal"}
        Error handling policy (default: "exception").

    """

    initialize = 'Y_PYTHON_IMPORT_TEST' not in __import__('os').environ
    threads = True
    thread_level = 'multiple'
    finalize = None
    fast_reduce = True
    recv_mprobe = True
    errors = 'exception'

    def __init__(self, **kwargs):
        self(**kwargs)

    def __call__(self, **kwargs):
        for key in kwargs:
            if not hasattr(self, key):
                raise TypeError("unexpected argument '{0}'".format(key))
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return '<{0}.rc>'.format(__name__)


rc = Rc()
__import__('sys').modules[__name__ + '.rc'] = rc


def get_include():
    """Return the directory in the package that contains header files.

    Extension modules that need to compile against mpi4py should use
    this function to locate the appropriate include directory. Using
    Python distutils (or perhaps NumPy distutils)::

      import mpi4py
      Extension('extension_name', ...
                include_dirs=[..., mpi4py.get_include()])

    """
    # pylint: disable=import-outside-toplevel
    from os.path import join, dirname
    return join(dirname(__file__), 'include')


def get_config():
    """Return a dictionary with information about MPI."""
    # pylint: disable=import-outside-toplevel
    from os.path import join, dirname
    from configparser import ConfigParser
    parser = ConfigParser()
    parser.read(join(dirname(__file__), 'mpi.cfg'))
    return dict(parser.items('mpi'))


def profile(name, *, path=None, logfile=None):
    """Support for the MPI profiling interface.

    Parameters
    ----------
    name : str
       Name of the profiler library to load.
    path : `sequence` of str, optional
       Additional paths to search for the profiler.
    logfile : str, optional
       Filename prefix for dumping profiler output.

    """
    # pylint: disable=import-outside-toplevel
    import os
    import sys
    from .dl import dlopen, dlerror, RTLD_NOW, RTLD_GLOBAL

    def lookup_dylib(name, path):
        # pylint: disable=missing-docstring
        pattern = []
        if sys.platform.startswith('win'):  # pragma: no cover
            pattern.append(('', '.dll'))
        elif sys.platform == 'darwin':  # pragma: no cover
            pattern.append(('lib', '.dylib'))
        elif os.name == 'posix':  # pragma: no cover
            pattern.append(('lib', '.so'))
        pattern.append(('', ''))
        for pth in path:
            for (lib, dso) in pattern:
                filename = os.path.join(pth, lib + name + dso)
                if os.path.isfile(filename):
                    return os.path.abspath(filename)
        return None

    if logfile:
        if name in ('mpe',):
            if 'MPE_LOGFILE_PREFIX' not in os.environ:
                os.environ['MPE_LOGFILE_PREFIX'] = logfile
        if name in ('vt', 'vt-mpi', 'vt-hyb'):
            if 'VT_FILE_PREFIX' not in os.environ:
                os.environ['VT_FILE_PREFIX'] = logfile

    if path is None:
        path = []
    elif isinstance(path, str):
        path = [path]
    else:
        path = list(path)
    prefix = os.path.dirname(__file__)
    path.append(os.path.join(prefix, 'lib-pmpi'))
    filename = lookup_dylib(name, path)
    if filename is None:
        raise ValueError("profiler '{0}' not found".format(name))

    handle = dlopen(filename, RTLD_NOW | RTLD_GLOBAL)
    if handle:
        registry = vars(profile).setdefault('registry', [])
        registry.append((name, (handle, filename)))
    else:
        from warnings import warn
        warn(dlerror())
