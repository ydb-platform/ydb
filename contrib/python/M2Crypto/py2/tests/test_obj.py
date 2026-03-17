#!/usr/bin/env python

"""Unit tests for M2Crypto.m2 obj_* functions.
"""
from M2Crypto import ASN1, BIO, Rand, X509, m2, six
from tests import unittest

"""
These functions must be cleaned up and moved to some python module
Taken from CA managment code
"""


def x509_name2list(name):
    for i in range(0, name.entry_count()):
        yield X509.X509_Name_Entry(m2.x509_name_get_entry(name._ptr(), i),
                                   _pyfree=0)


def x509_name_entry2tuple(entry):
    bio = BIO.MemoryBuffer()
    m2.asn1_string_print(bio._ptr(), m2.x509_name_entry_get_data(entry._ptr()))
    return (
        six.ensure_text(m2.obj_obj2txt(
            m2.x509_name_entry_get_object(entry._ptr()), 0)),
        six.ensure_text(bio.getvalue()))


def tuple2x509_name_entry(tup):
    obj, data = tup
    # TODO This is evil, isn't it? Shouldn't we use only official API?
    # Something like X509.X509_Name.add_entry_by_txt()
    _x509_ne = m2.x509_name_entry_create_by_txt(None, six.ensure_str(obj),
                                                ASN1.MBSTRING_ASC,
                                                six.ensure_str(data), len(data))
    if not _x509_ne:
        raise ValueError("Invalid object indentifier: %s" % obj)
    return X509.X509_Name_Entry(_x509_ne, _pyfree=1)  # Prevent memory leaks


class ObjectsTestCase(unittest.TestCase):

    def callback(self, *args):
        pass

    def test_obj2txt(self):
        self.assertEqual(m2.obj_obj2txt(m2.obj_txt2obj("commonName", 0), 1),
                         b"2.5.4.3", b"2.5.4.3")
        self.assertEqual(m2.obj_obj2txt(m2.obj_txt2obj("commonName", 0), 0),
                         b"commonName", b"commonName")

    def test_nid(self):
        self.assertEqual(m2.obj_ln2nid("commonName"),
                         m2.obj_txt2nid("2.5.4.3"),
                         "ln2nid and txt2nid mismatch")
        self.assertEqual(m2.obj_ln2nid("CN"),
                         0, "ln2nid on sn")
        self.assertEqual(m2.obj_sn2nid("CN"),
                         m2.obj_ln2nid("commonName"),
                         "ln2nid and sn2nid mismatch")
        self.assertEqual(m2.obj_sn2nid("CN"),
                         m2.obj_obj2nid(m2.obj_txt2obj("CN", 0)), "obj2nid")
        self.assertEqual(m2.obj_txt2nid("__unknown"),
                         0, "__unknown")

    def test_tuple2tuple(self):
        tup = ("CN", "someCommonName")
        tup1 = x509_name_entry2tuple(tuple2x509_name_entry(tup))
        # tup1[0] is 'commonName', not 'CN'
        self.assertEqual(tup1[1], tup[1], tup1)
        self.assertEqual(x509_name_entry2tuple(tuple2x509_name_entry(tup1)),
                         tup1, tup1)

    def test_unknown(self):
        with self.assertRaises(ValueError):
            tuple2x509_name_entry(("__unknown", "_"))

    def test_x509_name(self):
        n = X509.X509_Name()
        # It seems this actually needs to be a real 2 letter country code
        n.C = b'US'
        n.SP = b'State or Province'
        n.L = b'locality name'
        n.O = b'orhanization name'
        n.OU = b'org unit'
        n.CN = b'common name'
        n.Email = b'bob@example.com'
        n.serialNumber = b'1234'
        n.SN = b'surname'
        n.GN = b'given name'

        n.givenName = b'name given'
        self.assertEqual(len(n), 11, len(n))

        # Thierry: this call to list seems extraneous...
        tl = [x509_name_entry2tuple(x) for x in x509_name2list(n)]

        self.assertEqual(len(tl), len(n), len(tl))

        x509_n = m2.x509_name_new()
        for o in [tuple2x509_name_entry(x) for x in tl]:
            m2.x509_name_add_entry(x509_n, o._ptr(), -1, 0)
            o._pyfree = 0  # Take care of underlying object
        n1 = X509.X509_Name(x509_n)

        self.assertEqual(n.as_text(), n1.as_text(), n1.as_text())

    # Detailed OpenSSL error message is visible in Python error message:
    @unittest.skipIf(m2.OPENSSL_VERSION_NUMBER >= 0x30000000, "Failing on OpenSSL3")
    def test_detailed_error_message(self):
        from M2Crypto import SMIME, X509
        s = SMIME.SMIME()
        x509 = X509.load_cert('tests/recipient.pem')
        sk = X509.X509_Stack()
        sk.push(x509)
        s.set_x509_stack(sk)

        st = X509.X509_Store()
        st.load_info('tests/recipient.pem')
        s.set_x509_store(st)

        p7, data = SMIME.smime_load_pkcs7('tests/sample-p7.pem')
        self.assertIsInstance(p7, SMIME.PKCS7, p7)

        try:
            s.verify(p7, data)
        except SMIME.PKCS7_Error as e:
            six.assertRegex(self, str(e),
                                     "unable to get local issuer certificate",
                                     "Not received expected error message")

def suite():
    t_suite = unittest.TestSuite()
    t_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ObjectsTestCase))
    return t_suite


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
