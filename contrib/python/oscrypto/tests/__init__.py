# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import os
import unittest


__version__ = '1.3.0'
__version_info__ = (1, 3, 0)


_asn1crypto_module = None
_oscrypto_module = None


def local_oscrypto():
    """
    Make sure oscrypto is initialized and the backend is selected via env vars

    :return:
        A 2-element tuple with the (asn1crypto, oscrypto) modules
    """

    global _asn1crypto_module
    global _oscrypto_module

    if _oscrypto_module:
        return (_asn1crypto_module, _oscrypto_module)

    tests_dir = os.path.dirname(os.path.abspath(__file__))

    # If we are in a source checkout, load the local oscrypto module, and
    # local asn1crypto module if possible. Otherwise do a normal import.
    in_source_checkout = os.path.basename(tests_dir) == 'tests'

    if in_source_checkout:
        _asn1crypto_module = _import_from(
            'asn1crypto',
            os.path.abspath(os.path.join(tests_dir, '..', '..', 'asn1crypto'))
        )
    if _asn1crypto_module is None:
        import asn1crypto as _asn1crypto_module

    if in_source_checkout:
        _oscrypto_module = _import_from(
            'oscrypto',
            os.path.abspath(os.path.join(tests_dir, '..'))
        )
    if _oscrypto_module is None:
        import oscrypto as _oscrypto_module

    if os.environ.get('OSCRYPTO_USE_CTYPES'):
        _oscrypto_module.use_ctypes()

    # Configuring via env vars so CI for other packages doesn't need to do
    # anything complicated to get the alternate backends
    if os.environ.get('OSCRYPTO_USE_OPENSSL'):
        paths = os.environ.get('OSCRYPTO_USE_OPENSSL').split(',')
        if len(paths) != 2:
            raise ValueError('Value for OSCRYPTO_USE_OPENSSL env var must be two paths separated by a comma')
        _oscrypto_module.use_openssl(*paths)
    elif os.environ.get('OSCRYPTO_USE_WINLEGACY'):
        _oscrypto_module.use_winlegacy()

    return (_asn1crypto_module, _oscrypto_module)


def _import_from(mod, path, mod_dir=None):
    """
    Imports a module from a specific path

    :param mod:
        A unicode string of the module name

    :param path:
        A unicode string to the directory containing the module

    :param mod_dir:
        If the sub directory of "path" is different than the "mod" name,
        pass the sub directory as a unicode string

    :return:
        None if not loaded, otherwise the module
    """

    if mod_dir is None:
        mod_dir = mod

    if not os.path.exists(path):
        return None

    if not os.path.exists(os.path.join(path, mod_dir)):
        return None

    try:
        import imp
        mod_info = imp.find_module(mod_dir, [path])
        return imp.load_module(mod, *mod_info)
    except ImportError:
        return None


def make_suite():
    """
    Constructs a unittest.TestSuite() of all tests for the package. For use
    with setuptools.

    :return:
        A unittest.TestSuite() object
    """

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for test_class in test_classes():
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite


import pytest
@pytest.mark.skip
def test_classes():
    """
    Returns a list of unittest.TestCase classes for the package

    :return:
        A list of unittest.TestCase classes
    """

    _, oscrypto = local_oscrypto()

    if oscrypto.__version__ != __version__:
        raise AssertionError(
            ('oscrypto_tests version %s can not be run with ' % __version__) +
            ('oscrypto version %s' % oscrypto.__version__)
        )

    from .test_kdf import KDFTests
    from .test_keys import KeyTests
    from .test_asymmetric import AsymmetricTests
    from .test_symmetric import SymmetricTests
    from .test_trust_list import TrustListTests
    from .test_init import InitTests

    test_classes = [
        KDFTests,
        KeyTests,
        AsymmetricTests,
        SymmetricTests,
        TrustListTests,
        InitTests,
    ]
    if not os.environ.get('OSCRYPTO_SKIP_INTERNET_TESTS'):
        from .test_tls import TLSTests
        test_classes.append(TLSTests)

    return test_classes
