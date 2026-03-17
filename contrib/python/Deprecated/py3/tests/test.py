# coding: utf-8
import pkg_resources

import deprecated


def test_deprecated_has_docstring():
    # The deprecated package must have a docstring
    assert deprecated.__doc__ is not None
    assert "Deprecated Library" in deprecated.__doc__


def test_deprecated_has_version():
    # The deprecated package must have a valid version number
    assert deprecated.__version__ is not None
    version = pkg_resources.parse_version(deprecated.__version__)

    # .. note::
    #
    #    The classes ``SetuptoolsVersion`` and ``SetuptoolsLegacyVersion``
    #    are removed since setuptools >= 39.
    #    They are replaced by ``Version`` and ``LegacyVersion`` respectively.
    #
    #    To check if the version is good, we now use the solution explained here:
    #    https://github.com/pypa/setuptools/issues/1299

    assert 'Legacy' not in version.__class__.__name__
