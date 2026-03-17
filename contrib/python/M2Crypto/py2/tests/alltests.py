from __future__ import absolute_import, print_function


def suite():
    from M2Crypto import m2  # noqa
    import os
    import unittest

    def my_import(name):
        # See http://docs.python.org/lib/built-in-funcs.html#l2h-6
        components = name.split('.')
        try:
            # python setup.py test
            mod = __import__(name)
            for comp in components[1:]:
                mod = getattr(mod, comp)
        except ImportError:
            # python tests/alltests.py
            mod = __import__(components[1])
        return mod

    modules_to_test = [
        'tests.test_aes',
        'tests.test_asn1',
        'tests.test_bio',
        'tests.test_bio_membuf',
        'tests.test_bio_file',
        'tests.test_bio_iobuf',
        'tests.test_bio_ssl',
        'tests.test_bn',
        'tests.test_authcookie',
        'tests.test_dh',
        'tests.test_dsa',
        'tests.test_engine',
        'tests.test_err',
        'tests.test_evp',
        'tests.test_obj',
        'tests.test_rand',
        'tests.test_rc4',
        'tests.test_rsa',
        'tests.test_smime',
        'tests.test_ssl_offline',
        'tests.test_threading',
        'tests.test_util',
        'tests.test_x509',
        'tests.test_timeout']
    if os.name == 'posix':
        modules_to_test.append('tests.test_ssl')
    elif os.name == 'nt':
        modules_to_test.append('tests.test_ssl_win')
    if m2.OPENSSL_VERSION_NUMBER >= 0x90800F and m2.OPENSSL_NO_EC == 0:
        modules_to_test.append('tests.test_ecdh')
        modules_to_test.append('tests.test_ecdsa')
        modules_to_test.append('tests.test_ec_curves')
    alltests = unittest.TestSuite()
    for module in map(my_import, modules_to_test):
        alltests.addTest(module.suite())

    print('Version of OpenSSL is {0:x} ({1:s})'.format(m2.OPENSSL_VERSION_NUMBER,
            m2.OPENSSL_VERSION_TEXT))

    return alltests


def dump_garbage():
    import gc
    print('\nGarbage:')
    gc.collect()
    if len(gc.garbage):

        print('\nLeaked objects:')
        for x in gc.garbage:
            s = str(x)
            if len(s) > 77:
                s = s[:73] + '...'
            print(type(x), '\n  ', s)

        print('There were %d leaks.' % len(gc.garbage))
    else:
        print('Python garbage collector did not detect any leaks.')
        print('However, it is still possible there are leaks in the C code.')


def runall(report_leaks=0):
    report_leaks = report_leaks

    if report_leaks:
        import gc
        gc.enable()
        gc.set_debug(gc.DEBUG_LEAK & ~gc.DEBUG_SAVEALL)

    import os
    import unittest
    from M2Crypto import Rand

    try:
        Rand.load_file('tests/randpool.dat', -1)
        unittest.TextTestRunner(verbosity=2).run(suite())
        Rand.save_file('tests/randpool.dat')
    finally:
        if os.name == 'posix':
            from .test_ssl import zap_servers
            zap_servers()

    if report_leaks:
        dump_garbage()
