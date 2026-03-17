# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2003-2017 Nominum, Inc.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND NOMINUM DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL NOMINUM BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import socket
import sys
import time
import unittest

try:
    import ssl
    have_ssl = True
except Exception:
    have_ssl = False

import dns.exception
import dns.inet
import dns.message
import dns.name
import dns.rdataclass
import dns.rdatatype
import dns.query
import dns.tsigkeyring
import dns.zone

# Some tests require the internet to be available to run, so let's
# skip those if it's not there.
_network_available = True
try:
    socket.gethostbyname('dnspython.org')
except socket.gaierror:
    _network_available = False

# Some tests use a "nano nameserver" for testing.  It requires trio
# and threading, so try to import it and if it doesn't work, skip
# those tests.
try:
    from .nanonameserver import Server
    _nanonameserver_available = True
except ImportError:
    _nanonameserver_available = False
    class Server(object):
        pass

# Probe for IPv4 and IPv6
query_addresses = []
for (af, address) in ((socket.AF_INET, '8.8.8.8'),
                      (socket.AF_INET6, '2001:4860:4860::8888')):
    try:
        with socket.socket(af, socket.SOCK_DGRAM) as s:
            # Connecting a UDP socket is supposed to return ENETUNREACH if
            # no route to the network is present.
            s.connect((address, 53))
        query_addresses.append(address)
    except Exception:
        pass

keyring = dns.tsigkeyring.from_text({'name': 'tDz6cfXXGtNivRpQ98hr6A=='})

@unittest.skipIf(not _network_available, "Internet not reachable")
class QueryTests(unittest.TestCase):

    def testQueryUDP(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            q = dns.message.make_query(qname, dns.rdatatype.A)
            response = dns.query.udp(q, address, timeout=2)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)

    def testQueryUDPWithSocket(self):
        for address in query_addresses:
            with socket.socket(dns.inet.af_for_address(address),
                               socket.SOCK_DGRAM) as s:
                s.setblocking(0)
                qname = dns.name.from_text('dns.google.')
                q = dns.message.make_query(qname, dns.rdatatype.A)
                response = dns.query.udp(q, address, sock=s, timeout=2)
                rrs = response.get_rrset(response.answer, qname,
                                         dns.rdataclass.IN, dns.rdatatype.A)
                self.assertTrue(rrs is not None)
                seen = set([rdata.address for rdata in rrs])
                self.assertTrue('8.8.8.8' in seen)
                self.assertTrue('8.8.4.4' in seen)

    def testQueryTCP(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            q = dns.message.make_query(qname, dns.rdatatype.A)
            response = dns.query.tcp(q, address, timeout=2)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)

    def testQueryTCPWithSocket(self):
        for address in query_addresses:
            with socket.socket(dns.inet.af_for_address(address),
                               socket.SOCK_STREAM) as s:
                ll = dns.inet.low_level_address_tuple((address, 53))
                s.settimeout(2)
                s.connect(ll)
                s.setblocking(0)
                qname = dns.name.from_text('dns.google.')
                q = dns.message.make_query(qname, dns.rdatatype.A)
                response = dns.query.tcp(q, None, sock=s, timeout=2)
                rrs = response.get_rrset(response.answer, qname,
                                         dns.rdataclass.IN, dns.rdatatype.A)
                self.assertTrue(rrs is not None)
                seen = set([rdata.address for rdata in rrs])
                self.assertTrue('8.8.8.8' in seen)
                self.assertTrue('8.8.4.4' in seen)

    def testQueryTLS(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            q = dns.message.make_query(qname, dns.rdatatype.A)
            response = dns.query.tls(q, address, timeout=2)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)

    @unittest.skipUnless(have_ssl, "No SSL support")
    def testQueryTLSWithSocket(self):
        for address in query_addresses:
            with socket.socket(dns.inet.af_for_address(address),
                               socket.SOCK_STREAM) as base_s:
                ll = dns.inet.low_level_address_tuple((address, 853))
                base_s.settimeout(2)
                base_s.connect(ll)
                ctx = ssl.create_default_context()
                with ctx.wrap_socket(base_s, server_hostname='dns.google') as s:
                    s.setblocking(0)
                    qname = dns.name.from_text('dns.google.')
                    q = dns.message.make_query(qname, dns.rdatatype.A)
                    response = dns.query.tls(q, None, sock=s, timeout=2)
                    rrs = response.get_rrset(response.answer, qname,
                                             dns.rdataclass.IN, dns.rdatatype.A)
                    self.assertTrue(rrs is not None)
                    seen = set([rdata.address for rdata in rrs])
                    self.assertTrue('8.8.8.8' in seen)
                    self.assertTrue('8.8.4.4' in seen)

    def testQueryUDPFallback(self):
        for address in query_addresses:
            qname = dns.name.from_text('.')
            q = dns.message.make_query(qname, dns.rdatatype.DNSKEY)
            (_, tcp) = dns.query.udp_with_fallback(q, address, timeout=2)
            self.assertTrue(tcp)

    def testQueryUDPFallbackWithSocket(self):
        for address in query_addresses:
            af = dns.inet.af_for_address(address)
            with socket.socket(af, socket.SOCK_DGRAM) as udp_s:
                udp_s.setblocking(0)
                with socket.socket(af, socket.SOCK_STREAM) as tcp_s:
                    ll = dns.inet.low_level_address_tuple((address, 53))
                    tcp_s.settimeout(2)
                    tcp_s.connect(ll)
                    tcp_s.setblocking(0)
                    qname = dns.name.from_text('.')
                    q = dns.message.make_query(qname, dns.rdatatype.DNSKEY)
                    (_, tcp) = dns.query.udp_with_fallback(q, address,
                                                           udp_sock=udp_s,
                                                           tcp_sock=tcp_s,
                                                           timeout=2)
                    self.assertTrue(tcp)

    def testQueryUDPFallbackNoFallback(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            q = dns.message.make_query(qname, dns.rdatatype.A)
            (_, tcp) = dns.query.udp_with_fallback(q, address, timeout=2)
            self.assertFalse(tcp)

    def testUDPReceiveQuery(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener:
            listener.bind(('127.0.0.1', 0))
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
                sender.bind(('127.0.0.1', 0))
                q = dns.message.make_query('dns.google', dns.rdatatype.A)
                dns.query.send_udp(sender, q, listener.getsockname())
                expiration = time.time() + 2
                (q, _, addr) = dns.query.receive_udp(listener,
                                                     expiration=expiration)
                self.assertEqual(addr, sender.getsockname())


# for brevity
_d_and_s = dns.query._destination_and_source

class DestinationAndSourceTests(unittest.TestCase):

    def test_af_inferred_from_where(self):
        (af, d, s) = _d_and_s('1.2.3.4', 53, None, 0)
        self.assertEqual(af, socket.AF_INET)

    def test_af_inferred_from_where(self):
        (af, d, s) = _d_and_s('1::2', 53, None, 0)
        self.assertEqual(af, socket.AF_INET6)

    def test_af_inferred_from_source(self):
        (af, d, s) = _d_and_s('https://example/dns-query', 443,
                              '1.2.3.4', 0, False)
        self.assertEqual(af, socket.AF_INET)

    def test_af_mismatch(self):
        def bad():
            (af, d, s) = _d_and_s('1::2', 53, '1.2.3.4', 0)
        self.assertRaises(ValueError, bad)

    def test_source_port_but_no_af_inferred(self):
        def bad():
            (af, d, s) = _d_and_s('https://example/dns-query', 443,
                                  None, 12345, False)
        self.assertRaises(ValueError, bad)

    def test_where_must_be_an_address(self):
        def bad():
            (af, d, s) = _d_and_s('not a valid address', 53, '1.2.3.4', 0)
        self.assertRaises(ValueError, bad)

    def test_destination_is_none_of_where_url(self):
        (af, d, s) = _d_and_s('https://example/dns-query', 443, None, 0, False)
        self.assertEqual(d, None)

    def test_v4_wildcard_source_set(self):
        (af, d, s) = _d_and_s('1.2.3.4', 53, None, 12345)
        self.assertEqual(s, ('0.0.0.0', 12345))

    def test_v6_wildcard_source_set(self):
        (af, d, s) = _d_and_s('1::2', 53, None, 12345)
        self.assertEqual(s, ('::', 12345, 0, 0))


class AddressesEqualTestCase(unittest.TestCase):

    def test_v4(self):
        self.assertTrue(dns.query._addresses_equal(socket.AF_INET,
                                                   ('10.0.0.1', 53),
                                                   ('10.0.0.1', 53)))
        self.assertFalse(dns.query._addresses_equal(socket.AF_INET,
                                                    ('10.0.0.1', 53),
                                                    ('10.0.0.2', 53)))

    def test_v6(self):
        self.assertTrue(dns.query._addresses_equal(socket.AF_INET6,
                                                   ('1::1', 53),
                                                   ('0001:0000::1', 53)))
        self.assertFalse(dns.query._addresses_equal(socket.AF_INET6,
                                                   ('::1', 53),
                                                   ('::2', 53)))

    def test_mixed(self):
        self.assertFalse(dns.query._addresses_equal(socket.AF_INET,
                                                    ('10.0.0.1', 53),
                                                    ('::2', 53)))


axfr_zone = '''
$TTL 300
@ SOA ns1 root 1 7200 900 1209600 86400
@ NS ns1
@ NS ns2
ns1 A 10.0.0.1
ns2 A 10.0.0.1
'''

class AXFRNanoNameserver(Server):

    def handle(self, request):
        self.zone = dns.zone.from_text(axfr_zone, origin=self.origin)
        self.origin = self.zone.origin
        items = []
        soa = self.zone.find_rrset(dns.name.empty, dns.rdatatype.SOA)
        response = dns.message.make_response(request.message)
        response.flags |= dns.flags.AA
        response.answer.append(soa)
        items.append(response)
        response = dns.message.make_response(request.message)
        response.question = []
        response.flags |= dns.flags.AA
        for (name, rdataset) in self.zone.iterate_rdatasets():
            if rdataset.rdtype == dns.rdatatype.SOA and \
               name == dns.name.empty:
                continue
            rrset = dns.rrset.RRset(name, rdataset.rdclass, rdataset.rdtype,
                                    rdataset.covers)
            rrset.update(rdataset)
            response.answer.append(rrset)
        items.append(response)
        response = dns.message.make_response(request.message)
        response.question = []
        response.flags |= dns.flags.AA
        response.answer.append(soa)
        items.append(response)
        return items

ixfr_message = '''id 12345
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
example. 300 IN SOA ns1.example. root.example. 4 7200 900 1209600 86400
example. 300 IN SOA ns1.example. root.example. 2 7200 900 1209600 86400
deleted.example. 300 IN A 10.0.0.1
changed.example. 300 IN A 10.0.0.2
example. 300 IN SOA ns1.example. root.example. 3 7200 900 1209600 86400
changed.example. 300 IN A 10.0.0.4
added.example. 300 IN A 10.0.0.3
example. 300 SOA ns1.example. root.example. 3 7200 900 1209600 86400
example. 300 IN SOA ns1.example. root.example. 4 7200 900 1209600 86400
added2.example. 300 IN A 10.0.0.5
example. 300 IN SOA ns1.example. root.example. 4 7200 900 1209600 86400
'''

ixfr_trailing_junk = ixfr_message + 'junk.example. 300 IN A 10.0.0.6'

ixfr_up_to_date_message = '''id 12345
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
example. 300 IN SOA ns1.example. root.example. 2 7200 900 1209600 86400
'''

axfr_trailing_junk = '''id 12345
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN AXFR
;ANSWER
example. 300 IN SOA ns1.example. root.example. 3 7200 900 1209600 86400
added.example. 300 IN A 10.0.0.3
added2.example. 300 IN A 10.0.0.5
changed.example. 300 IN A 10.0.0.4
example. 300 IN SOA ns1.example. root.example. 3 7200 900 1209600 86400
junk.example. 300 IN A 10.0.0.6
'''

class IXFRNanoNameserver(Server):

    def __init__(self, response_text):
        super().__init__()
        self.response_text = response_text

    def handle(self, request):
        try:
            r = dns.message.from_text(self.response_text, one_rr_per_rrset=True)
            r.id = request.message.id
            return r
        except Exception:
            pass

@unittest.skipIf(not _nanonameserver_available, "nanonameserver required")
class XfrTests(unittest.TestCase):

    def test_axfr(self):
        expected = dns.zone.from_text(axfr_zone, origin='example')
        with AXFRNanoNameserver(origin='example') as ns:
            xfr = dns.query.xfr(ns.tcp_address[0], 'example',
                                port=ns.tcp_address[1])
            zone = dns.zone.from_xfr(xfr)
            self.assertEqual(zone, expected)

    def test_axfr_tsig(self):
        expected = dns.zone.from_text(axfr_zone, origin='example')
        with AXFRNanoNameserver(origin='example', keyring=keyring) as ns:
            xfr = dns.query.xfr(ns.tcp_address[0], 'example',
                                port=ns.tcp_address[1],
                                keyring=keyring, keyname='name')
            zone = dns.zone.from_xfr(xfr)
            self.assertEqual(zone, expected)

    def test_axfr_root_tsig(self):
        expected = dns.zone.from_text(axfr_zone, origin='.')
        with AXFRNanoNameserver(origin='.', keyring=keyring) as ns:
            xfr = dns.query.xfr(ns.tcp_address[0], '.',
                                port=ns.tcp_address[1],
                                keyring=keyring, keyname='name')
            zone = dns.zone.from_xfr(xfr)
            self.assertEqual(zone, expected)

    def test_axfr_udp(self):
        def bad():
            with AXFRNanoNameserver(origin='example') as ns:
                xfr = dns.query.xfr(ns.udp_address[0], 'example',
                                    port=ns.udp_address[1], use_udp=True)
                l = list(xfr)
        self.assertRaises(ValueError, bad)

    def test_axfr_bad_rcode(self):
        def bad():
            # We just use Server here as by default it will refuse.
            with Server() as ns:
                xfr = dns.query.xfr(ns.tcp_address[0], 'example',
                                    port=ns.tcp_address[1])
                l = list(xfr)
        self.assertRaises(dns.query.TransferError, bad)

    def test_axfr_trailing_junk(self):
        # we use the IXFR server here as it returns messages
        def bad():
            with IXFRNanoNameserver(axfr_trailing_junk) as ns:
                xfr = dns.query.xfr(ns.tcp_address[0], 'example',
                                    dns.rdatatype.AXFR,
                                    port=ns.tcp_address[1])
                l = list(xfr)
        self.assertRaises(dns.exception.FormError, bad)

    def test_ixfr_tcp(self):
        with IXFRNanoNameserver(ixfr_message) as ns:
            xfr = dns.query.xfr(ns.tcp_address[0], 'example',
                                dns.rdatatype.IXFR,
                                port=ns.tcp_address[1],
                                serial=2,
                                relativize=False)
            l = list(xfr)
            self.assertEqual(len(l), 1)
            expected = dns.message.from_text(ixfr_message,
                                             one_rr_per_rrset=True)
            expected.id = l[0].id
            self.assertEqual(l[0], expected)

    def test_ixfr_udp(self):
        with IXFRNanoNameserver(ixfr_message) as ns:
            xfr = dns.query.xfr(ns.udp_address[0], 'example',
                                dns.rdatatype.IXFR,
                                port=ns.udp_address[1],
                                serial=2,
                                relativize=False, use_udp=True)
            l = list(xfr)
            self.assertEqual(len(l), 1)
            expected = dns.message.from_text(ixfr_message,
                                             one_rr_per_rrset=True)
            expected.id = l[0].id
            self.assertEqual(l[0], expected)

    def test_ixfr_up_to_date(self):
        with IXFRNanoNameserver(ixfr_up_to_date_message) as ns:
            xfr = dns.query.xfr(ns.tcp_address[0], 'example',
                                dns.rdatatype.IXFR,
                                port=ns.tcp_address[1],
                                serial=2,
                                relativize=False)
            l = list(xfr)
            self.assertEqual(len(l), 1)
            expected = dns.message.from_text(ixfr_up_to_date_message,
                                             one_rr_per_rrset=True)
            expected.id = l[0].id
            self.assertEqual(l[0], expected)

    def test_ixfr_trailing_junk(self):
        def bad():
            with IXFRNanoNameserver(ixfr_trailing_junk) as ns:
                xfr = dns.query.xfr(ns.tcp_address[0], 'example',
                                    dns.rdatatype.IXFR,
                                    port=ns.tcp_address[1],
                                    serial=2,
                                    relativize=False)
                l = list(xfr)
        self.assertRaises(dns.exception.FormError, bad)

    def test_ixfr_base_serial_mismatch(self):
        def bad():
            with IXFRNanoNameserver(ixfr_message) as ns:
                xfr = dns.query.xfr(ns.tcp_address[0], 'example',
                                    dns.rdatatype.IXFR,
                                    port=ns.tcp_address[1],
                                    serial=1,
                                    relativize=False)
                l = list(xfr)
        self.assertRaises(dns.exception.FormError, bad)

class TSIGNanoNameserver(Server):

    def handle(self, request):
        response = dns.message.make_response(request.message)
        response.set_rcode(dns.rcode.REFUSED)
        response.flags |= dns.flags.RA
        try:
            if request.qtype == dns.rdatatype.A and \
               request.qclass == dns.rdataclass.IN:
                rrs = dns.rrset.from_text(request.qname, 300,
                                          'IN', 'A', '1.2.3.4')
                response.answer.append(rrs)
                response.set_rcode(dns.rcode.NOERROR)
                response.flags |= dns.flags.AA
        except Exception:
            pass
        return response

@unittest.skipIf(not _nanonameserver_available, "nanonameserver required")
class TsigTests(unittest.TestCase):

    def test_tsig(self):
        with TSIGNanoNameserver(keyring=keyring) as ns:
            qname = dns.name.from_text('example.com')
            q = dns.message.make_query(qname, 'A')
            q.use_tsig(keyring=keyring, keyname='name')
            response = dns.query.udp(q, ns.udp_address[0],
                                     port=ns.udp_address[1])
            self.assertTrue(response.had_tsig)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('1.2.3.4' in seen)

@unittest.skipIf(sys.platform == 'win32',
                 'low level tests do not work on win32')
class LowLevelWaitTests(unittest.TestCase):

    def test_wait_for(self):
        try:
            (l, r) = socket.socketpair()
            # already expired
            with self.assertRaises(dns.exception.Timeout):
                dns.query._wait_for(l, True, True, True, 0)
            # simple timeout
            with self.assertRaises(dns.exception.Timeout):
                dns.query._wait_for(l, False, False, False, time.time() + 0.05)
            # writable no timeout (not hanging is passing)
            dns.query._wait_for(l, False, True, False, None)
        finally:
            l.close()
            r.close()


class MiscTests(unittest.TestCase):
    def test_matches_destination(self):
        self.assertTrue(dns.query._matches_destination(socket.AF_INET,
                                                       ('10.0.0.1', 1234),
                                                       ('10.0.0.1', 1234),
                                                       True))
        self.assertTrue(dns.query._matches_destination(socket.AF_INET6,
                                                       ('1::2', 1234),
                                                       ('0001::2', 1234),
                                                       True))
        self.assertTrue(dns.query._matches_destination(socket.AF_INET,
                                                       ('10.0.0.1', 1234),
                                                       None,
                                                       True))
        self.assertFalse(dns.query._matches_destination(socket.AF_INET,
                                                        ('10.0.0.1', 1234),
                                                        ('10.0.0.2', 1234),
                                                        True))
        self.assertFalse(dns.query._matches_destination(socket.AF_INET,
                                                        ('10.0.0.1', 1234),
                                                        ('10.0.0.1', 1235),
                                                        True))
        with self.assertRaises(dns.query.UnexpectedSource):
            dns.query._matches_destination(socket.AF_INET,
                                           ('10.0.0.1', 1234),
                                           ('10.0.0.1', 1235),
                                           False)
