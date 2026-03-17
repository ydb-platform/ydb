""" Tools for working with Debian-related file formats """

__version__ = ""

try:
    # pylint: disable=no-member
    import debian._version
    __version__ = debian._version.__version__

except ImportError:
    try:
        from setuptools_scm import get_version
        __version__ = get_version(root="..")
    except ImportError:
        import warnings
        warnings.warn("_version.py not found and setuptools_scm not installed.")
    except Exception as e: # pylint: disable=broad-exception-caught
        import warnings
        warnings.warn("_version.py not found and setuptools_scm couldn't make a version.\n%s" % e)
    finally:
        # Fake a version string in desperation
        if not __version__:
            __version__ = '0.0.0-unknown'
