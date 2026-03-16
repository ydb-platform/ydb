import os.path
import cytoolz


__all__ = ['raises', 'no_default', 'include_dirs', 'consume']


try:
    # Attempt to get the no_default sentinel object from toolz
    from toolz.utils import no_default
except ImportError:
    no_default = '__no__default__'


def raises(err, lamda):
    try:
        lamda()
        return False
    except err:
        return True


def include_dirs():
    """ Return a list of directories containing the *.pxd files for ``cytoolz``

    Use this to include ``cytoolz`` in your own Cython project, which allows
    fast C bindinds to be imported such as ``from cytoolz cimport get``.

    Below is a minimal "setup.py" file using ``include_dirs``:

        from setuptools import setup
        from setuptools.extension import Extension
        from Cython.Build import cythonize

        import cytoolz.utils

        ext_modules=[
            Extension("mymodule",
                      ["mymodule.pyx"],
                      include_dirs=cytoolz.utils.include_dirs()
                     )
        ]
        ext_modules = cythonize(ext_modules)

        setup(
          name="mymodule",
          ext_modules=ext_modules,
        )
    """
    return os.path.split(cytoolz.__path__[0])


cpdef object consume(object seq):
    """
    Efficiently consume an iterable """
    for _ in seq:
        pass
