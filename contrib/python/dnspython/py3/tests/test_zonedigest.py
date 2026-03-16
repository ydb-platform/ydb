# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import io
import textwrap
import unittest

import dns.rdata
import dns.rrset
import dns.zone

class ZoneDigestTestCase(unittest.TestCase):
    # Examples from RFC 8976, fixed per errata.

    simple_example = textwrap.dedent('''
        example.      86400  IN  SOA     ns1 admin 2018031900 (
                                         1800 900 604800 86400 )
                      86400  IN  NS      ns1
                      86400  IN  NS      ns2
                      86400  IN  ZONEMD  2018031900 1 1 (
                                         c68090d90a7aed71
                                         6bc459f9340e3d7c
                                         1370d4d24b7e2fc3
                                         a1ddc0b9a87153b9
                                         a9713b3c9ae5cc27
                                         777f98b8e730044c )
        ns1           3600   IN  A       203.0.113.63
        ns2           3600   IN  AAAA    2001:db8::63
    ''')

    complex_example = textwrap.dedent('''
        example.      86400  IN  SOA     ns1 admin 2018031900 (
                                         1800 900 604800 86400 )
                      86400  IN  NS      ns1
                      86400  IN  NS      ns2
                      86400  IN  ZONEMD  2018031900 1 1 (
                                         a3b69bad980a3504
                                         e1cffcb0fd6397f9
                                         3848071c93151f55
                                         2ae2f6b1711d4bd2
                                         d8b39808226d7b9d
                                         b71e34b72077f8fe )
        ns1           3600   IN  A       203.0.113.63
        NS2           3600   IN  AAAA    2001:db8::63
        occluded.sub  7200   IN  TXT     "I'm occluded but must be digested"
        sub           7200   IN  NS      ns1
        duplicate     300    IN  TXT     "I must be digested just once"
        duplicate     300    IN  TXT     "I must be digested just once"
        foo.test.     555    IN  TXT     "out-of-zone data must be excluded"
        UPPERCASE     3600   IN  TXT     "canonicalize uppercase owner names"
        *             777    IN  PTR     dont-forget-about-wildcards
        mail          3600   IN  MX      20 MAIL1
        mail          3600   IN  MX      10 Mail2.Example.
        sortme        3600   IN  AAAA    2001:db8::5:61
        sortme        3600   IN  AAAA    2001:db8::3:62
        sortme        3600   IN  AAAA    2001:db8::4:63
        sortme        3600   IN  AAAA    2001:db8::1:65
        sortme        3600   IN  AAAA    2001:db8::2:64
        non-apex      900    IN  ZONEMD  2018031900 1 1 (
                                         616c6c6f77656420
                                         6275742069676e6f
                                         7265642e20616c6c
                                         6f77656420627574
                                         2069676e6f726564
                                         2e20616c6c6f7765 )
    ''')

    multiple_digests_example = textwrap.dedent('''
        example.      86400  IN  SOA     ns1 admin 2018031900 (
                                         1800 900 604800 86400 )
        example.      86400  IN  NS      ns1.example.
        example.      86400  IN  NS      ns2.example.
        example.      86400  IN  ZONEMD  2018031900 1 1 (
                                         62e6cf51b02e54b9
                                         b5f967d547ce4313
                                         6792901f9f88e637
                                         493daaf401c92c27
                                         9dd10f0edb1c56f8
                                         080211f8480ee306 )
        example.      86400  IN  ZONEMD  2018031900 1 2 (
                                         08cfa1115c7b948c
                                         4163a901270395ea
                                         226a930cd2cbcf2f
                                         a9a5e6eb85f37c8a
                                         4e114d884e66f176
                                         eab121cb02db7d65
                                         2e0cc4827e7a3204
                                         f166b47e5613fd27 )
        example.      86400  IN  ZONEMD  2018031900 1 240 (
                                         e2d523f654b9422a
                                         96c5a8f44607bbee )
        example.      86400  IN  ZONEMD  2018031900 241 1 (
                                         e1846540e33a9e41
                                         89792d18d5d131f6
                                         05fc283eaaaaaaaa
                                         aaaaaaaaaaaaaaaa
                                         aaaaaaaaaaaaaaaa
                                         aaaaaaaaaaaaaaaa)
        ns1.example.  3600   IN  A       203.0.113.63
        ns2.example.  86400  IN  TXT     "This example has multiple digests"
        NS2.EXAMPLE.  3600   IN  AAAA    2001:db8::63
    ''')

    def _get_zonemd(self, zone):
        return zone.get_rdataset(zone.origin, 'ZONEMD')

    def test_zonemd_simple(self):
        zone = dns.zone.from_text(self.simple_example, origin='example')
        zone.verify_digest()
        zonemd = self._get_zonemd(zone)
        self.assertEqual(zonemd[0],
                         zone.compute_digest(zonemd[0].hash_algorithm))

    def test_zonemd_simple_absolute(self):
        zone = dns.zone.from_text(self.simple_example, origin='example',
                                  relativize=False)
        zone.verify_digest()
        zonemd = self._get_zonemd(zone)
        self.assertEqual(zonemd[0],
                         zone.compute_digest(zonemd[0].hash_algorithm))

    def test_zonemd_complex(self):
        zone = dns.zone.from_text(self.complex_example, origin='example')
        zone.verify_digest()
        zonemd = self._get_zonemd(zone)
        self.assertEqual(zonemd[0],
                         zone.compute_digest(zonemd[0].hash_algorithm))

    def test_zonemd_multiple_digests(self):
        zone = dns.zone.from_text(self.multiple_digests_example,
                                  origin='example')
        zone.verify_digest()

        zonemd = self._get_zonemd(zone)
        for rr in zonemd:
            if rr.scheme == 1 and rr.hash_algorithm in (1, 2):
                zone.verify_digest(rr)
                self.assertEqual(rr, zone.compute_digest(rr.hash_algorithm))
            else:
                with self.assertRaises(dns.zone.DigestVerificationFailure):
                    zone.verify_digest(rr)

    def test_zonemd_no_digest(self):
        zone = dns.zone.from_text(self.simple_example, origin='example')
        zone.delete_rdataset(dns.name.empty, 'ZONEMD')
        with self.assertRaises(dns.zone.NoDigest):
            zone.verify_digest()

    sha384_hash = 'ab' * 48
    sha512_hash = 'ab' * 64

    def test_zonemd_parse_rdata(self):
        dns.rdata.from_text('IN', 'ZONEMD', '100 1 1 ' + self.sha384_hash)
        dns.rdata.from_text('IN', 'ZONEMD', '100 1 2 ' + self.sha512_hash)
        dns.rdata.from_text('IN', 'ZONEMD', '100 100 1 ' + self.sha384_hash)
        dns.rdata.from_text('IN', 'ZONEMD', '100 1 100 abcd')

    def test_zonemd_unknown_scheme(self):
        zone = dns.zone.from_text(self.simple_example, origin='example')
        with self.assertRaises(dns.zone.UnsupportedDigestScheme):
            zone.compute_digest(dns.zone.DigestHashAlgorithm.SHA384, 2)

    def test_zonemd_unknown_hash_algorithm(self):
        zone = dns.zone.from_text(self.simple_example, origin='example')
        with self.assertRaises(dns.zone.UnsupportedDigestHashAlgorithm):
            zone.compute_digest(5)

    def test_zonemd_invalid_digest_length(self):
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('IN', 'ZONEMD', '100 1 2 ' + self.sha384_hash)
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('IN', 'ZONEMD', '100 2 1 ' + self.sha512_hash)

    def test_zonemd_parse_rdata_reserved(self):
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('IN', 'ZONEMD', '100 0 1 ' + self.sha384_hash)
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('IN', 'ZONEMD', '100 1 0 ' + self.sha384_hash)

    sorting_zone = textwrap.dedent('''
    @  86400  IN  SOA     ns1 admin 2018031900 (
                                     1800 900 604800 86400 )
              86400  IN  NS      ns1
              86400  IN  NS      ns2
              86400  IN  RP      n1.example. a.
              86400  IN  RP      n1. b.
    ''')
    
    def test_relative_zone_sorting(self):
        z1 = dns.zone.from_text(self.sorting_zone, 'example.', relativize=True)
        z2 = dns.zone.from_text(self.sorting_zone, 'example.', relativize=False)
        zmd1 = z1.compute_digest(dns.zone.DigestHashAlgorithm.SHA384)
        zmd2 = z2.compute_digest(dns.zone.DigestHashAlgorithm.SHA384)
        self.assertEqual(zmd1, zmd2)
