# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest

from dns import dnssec, name


class NSEC3Hash(unittest.TestCase):

    DATA = [
        # Source: https://tools.ietf.org/html/rfc5155#appendix-A
        ("example", "aabbccdd", 12, "0p9mhaveqvm6t7vbl5lop2u3t2rp3tom", 1),
        ("a.example", "aabbccdd", 12, "35mthgpgcu1qg68fab165klnsnk3dpvl", 1),
        ("ai.example", "aabbccdd", 12, "gjeqe526plbf1g8mklp59enfd789njgi", 1),
        ("ns1.example", "aabbccdd", 12, "2t7b4g4vsa5smi47k61mv5bv1a22bojr", 1),
        ("ns2.example", "aabbccdd", 12, "q04jkcevqvmu85r014c7dkba38o0ji5r", 1),
        ("w.example", "aabbccdd", 12, "k8udemvp1j2f7eg6jebps17vp3n8i58h", 1),
        ("*.w.example", "aabbccdd", 12, "r53bq7cc2uvmubfu5ocmm6pers9tk9en", 1),
        ("x.w.example", "aabbccdd", 12, "b4um86eghhds6nea196smvmlo4ors995", 1),
        ("y.w.example", "aabbccdd", 12, "ji6neoaepv8b5o6k4ev33abha8ht9fgc", 1),
        ("x.y.w.example", "aabbccdd", 12, "2vptu5timamqttgl4luu9kg21e0aor3s", 1),
        ("xx.example", "aabbccdd", 12, "t644ebqk9bibcna874givr6joj62mlhv", 1),
        (
            "2t7b4g4vsa5smi47k61mv5bv1a22bojr.example",
            "aabbccdd",
            12,
            "kohar7mbb8dc2ce8a9qvl8hon4k53uhi",
            1,
        ),
        # Source: generated with knsec3hash (Linux knot package)
        ("example.com", "9F1AB450CF71D6", 0, "qfo2sv6jaej4cm11a3npoorfrckdao2c", 1),
        ("example.com", "9F1AB450CF71D6", 1, "1nr64to0bb861lku97deb4ubbk6cl5qh", 1),
        ("example.com.", "AF6AB45CCF79D6", 6, "sale3fn6penahh1lq5oqtr5rcl1d113a", 1),
        ("test.domain.dev.", "", 6, "8q98lv9jgkhoq272e42c8blesivia7bu", 1),
        ("www.test.domain.dev.", "B4", 2, "nv7ti6brgh94ke2f3pgiigjevfgpo5j0", 1),
        ("*.test-domain.dev", "", 0, "o6uadafckb6hea9qpcgir2gl71vt23gu", 1),
        ("*.test-domain.dev", "", 45, "505k9g118d9sofnjhh54rr8fadgpa0ct", 1),
        # Alternate forms of parameters
        (
            name.from_text("example"),
            "aabbccdd",
            12,
            "0p9mhaveqvm6t7vbl5lop2u3t2rp3tom",
            1,
        ),
        (
            "example",
            b"\xaa\xbb\xcc\xdd",
            12,
            "0p9mhaveqvm6t7vbl5lop2u3t2rp3tom",
            1,
        ),
        ("*.test-domain.dev", None, 45, "505k9g118d9sofnjhh54rr8fadgpa0ct", 1),
        (
            "example",
            "aabbccdd",
            12,
            "0p9mhaveqvm6t7vbl5lop2u3t2rp3tom",
            dnssec.NSEC3Hash.SHA1
        ),
        ("example", "aabbccdd", 12, "0p9mhaveqvm6t7vbl5lop2u3t2rp3tom", "SHA1"),
        ("example", "aabbccdd", 12, "0p9mhaveqvm6t7vbl5lop2u3t2rp3tom", "sha1")
    ]

    def test_hash_function(self):
        for d in self.DATA:
            hash = dnssec.nsec3_hash(d[0], d[1], d[2], d[4])
            self.assertEqual(hash, d[3].upper(), "Error {}".format(d))

    def test_hash_invalid_salt_length(self):
        data = (
            "example.com",
            "9F1AB450CF71D",
            0,
            "qfo2sv6jaej4cm11a3npoorfrckdao2c",
            1,
        )
        with self.assertRaises(ValueError):
            hash = dnssec.nsec3_hash(data[0], data[1], data[2], data[4])

    def test_hash_invalid_algorithm(self):
        data = (
            "example.com",
            "9F1AB450CF71D",
            0,
            "qfo2sv6jaej4cm11a3npoorfrckdao2c",
            1,
        )
        with self.assertRaises(ValueError):
            dnssec.nsec3_hash(data[0], data[1], data[2], 10)
        with self.assertRaises(ValueError):
            dnssec.nsec3_hash(data[0], data[1], data[2], "foo")


if __name__ == "__main__":
    unittest.main()
