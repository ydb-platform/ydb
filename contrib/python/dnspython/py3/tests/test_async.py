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

import asyncio
import random
import socket
import sys
import time
import unittest

import dns.asyncbackend
import dns.asyncquery
import dns.asyncresolver
import dns.message
import dns.name
import dns.query
import dns.rdataclass
import dns.rdatatype
import dns.resolver


# Some tests require TLS so skip those if it's not there.
ssl = dns.query.ssl
try:
    ssl.create_default_context()
    _ssl_available = True
except Exception:
    _ssl_available = False


# Some tests require the internet to be available to run, so let's
# skip those if it's not there.
_network_available = True
try:
    socket.gethostbyname('dnspython.org')
except socket.gaierror:
    _network_available = False


# Look for systemd-resolved, as it does dangling CNAME responses incorrectly.
#
# Currently we simply check if the nameserver is 127.0.0.53.
_systemd_resolved_present = False
try:
    _resolver = dns.resolver.Resolver()
    if _resolver.nameservers == ['127.0.0.53']:
        _systemd_resolved_present = True
except Exception:
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

KNOWN_ANYCAST_DOH_RESOLVER_URLS = ['https://cloudflare-dns.com/dns-query',
                                   'https://dns.google/dns-query',
                                   # 'https://dns11.quad9.net/dns-query',
                                   ]


class AsyncDetectionTests(unittest.TestCase):
    sniff_result = 'asyncio'

    def async_run(self, afunc):
        try:
            runner = asyncio.run
        except AttributeError:
            # this is only needed for 3.6
            def old_runner(awaitable):
                loop = asyncio.get_event_loop()
                return loop.run_until_complete(awaitable)
            runner = old_runner
        return runner(afunc())

    def test_sniff(self):
        dns.asyncbackend._default_backend = None
        async def run():
            self.assertEqual(dns.asyncbackend.sniff(), self.sniff_result)
        self.async_run(run)

    def test_get_default_backend(self):
        dns.asyncbackend._default_backend = None
        async def run():
            backend = dns.asyncbackend.get_default_backend()
            self.assertEqual(backend.name(), self.sniff_result)
        self.async_run(run)

class NoSniffioAsyncDetectionTests(AsyncDetectionTests):
    expect_raise = False

    def setUp(self):
        dns.asyncbackend._no_sniffio = True

    def tearDown(self):
        dns.asyncbackend._no_sniffio = False

    def test_sniff(self):
        dns.asyncbackend._default_backend = None
        if self.expect_raise:
            async def abad():
                dns.asyncbackend.sniff()
            def bad():
                self.async_run(abad)
            self.assertRaises(dns.asyncbackend.AsyncLibraryNotFoundError, bad)
        else:
            super().test_sniff()

    def test_get_default_backend(self):
        dns.asyncbackend._default_backend = None
        if self.expect_raise:
            async def abad():
                dns.asyncbackend.get_default_backend()
            def bad():
                self.async_run(abad)
            self.assertRaises(dns.asyncbackend.AsyncLibraryNotFoundError, bad)
        else:
            super().test_get_default_backend()


class MiscBackend(unittest.TestCase):
    def test_sniff_without_run_loop(self):
        dns.asyncbackend._default_backend = None
        def bad():
            dns.asyncbackend.sniff()
        self.assertRaises(dns.asyncbackend.AsyncLibraryNotFoundError, bad)

    def test_bogus_backend(self):
        def bad():
            dns.asyncbackend.get_backend('bogus')
        self.assertRaises(NotImplementedError, bad)


class MiscQuery(unittest.TestCase):
    def test_source_tuple(self):
        t = dns.asyncquery._source_tuple(socket.AF_INET, None, 0)
        self.assertEqual(t, None)
        t = dns.asyncquery._source_tuple(socket.AF_INET6, None, 0)
        self.assertEqual(t, None)
        t = dns.asyncquery._source_tuple(socket.AF_INET, '1.2.3.4', 53)
        self.assertEqual(t, ('1.2.3.4', 53))
        t = dns.asyncquery._source_tuple(socket.AF_INET6, '1::2', 53)
        self.assertEqual(t, ('1::2', 53))
        t = dns.asyncquery._source_tuple(socket.AF_INET, None, 53)
        self.assertEqual(t, ('0.0.0.0', 53))
        t = dns.asyncquery._source_tuple(socket.AF_INET6, None, 53)
        self.assertEqual(t, ('::', 53))


@unittest.skipIf(not _network_available, "Internet not reachable")
class AsyncTests(unittest.TestCase):
    connect_udp = sys.platform == 'win32'

    def setUp(self):
        self.backend = dns.asyncbackend.set_default_backend('asyncio')

    def async_run(self, afunc):
        try:
            runner = asyncio.run
        except AttributeError:
            # this is only needed for 3.6
            def old_runner(awaitable):
                loop = asyncio.get_event_loop()
                return loop.run_until_complete(awaitable)
            runner = old_runner
        return runner(afunc())

    def testResolve(self):
        async def run():
            answer = await dns.asyncresolver.resolve('dns.google.', 'A')
            return set([rdata.address for rdata in answer])
        seen = self.async_run(run)
        self.assertTrue('8.8.8.8' in seen)
        self.assertTrue('8.8.4.4' in seen)

    def testResolveAddress(self):
        async def run():
            return await dns.asyncresolver.resolve_address('8.8.8.8')
        answer = self.async_run(run)
        dnsgoogle = dns.name.from_text('dns.google.')
        self.assertEqual(answer[0].target, dnsgoogle)

    def testCanonicalNameNoCNAME(self):
        cname = dns.name.from_text('www.google.com')
        async def run():
            return await dns.asyncresolver.canonical_name('www.google.com')
        self.assertEqual(self.async_run(run), cname)

    def testCanonicalNameCNAME(self):
        name = dns.name.from_text('www.dnspython.org')
        cname = dns.name.from_text('dmfrjf4ips8xa.cloudfront.net')
        async def run():
            return await dns.asyncresolver.canonical_name(name)
        self.assertEqual(self.async_run(run), cname)

    @unittest.skipIf(_systemd_resolved_present, "systemd-resolved in use")
    def testCanonicalNameDangling(self):
        name = dns.name.from_text('dangling-cname.dnspython.org')
        cname = dns.name.from_text('dangling-target.dnspython.org')
        async def run():
            return await dns.asyncresolver.canonical_name(name)
        self.assertEqual(self.async_run(run), cname)


    def testZoneForName1(self):
        async def run():
            name = dns.name.from_text('www.dnspython.org.')
            return await dns.asyncresolver.zone_for_name(name)
        ezname = dns.name.from_text('dnspython.org.')
        zname = self.async_run(run)
        self.assertEqual(zname, ezname)

    def testZoneForName2(self):
        async def run():
            name = dns.name.from_text('a.b.www.dnspython.org.')
            return await dns.asyncresolver.zone_for_name(name)
        ezname = dns.name.from_text('dnspython.org.')
        zname = self.async_run(run)
        self.assertEqual(zname, ezname)

    def testZoneForName3(self):
        async def run():
            name = dns.name.from_text('dnspython.org.')
            return await dns.asyncresolver.zone_for_name(name)
        ezname = dns.name.from_text('dnspython.org.')
        zname = self.async_run(run)
        self.assertEqual(zname, ezname)

    def testZoneForName4(self):
        def bad():
            name = dns.name.from_text('dnspython.org', None)
            async def run():
                return await dns.asyncresolver.zone_for_name(name)
            self.async_run(run)
        self.assertRaises(dns.resolver.NotAbsolute, bad)

    def testQueryUDP(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            async def run():
                q = dns.message.make_query(qname, dns.rdatatype.A)
                return await dns.asyncquery.udp(q, address, timeout=2)
            response = self.async_run(run)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)

    def testQueryUDPWithSocket(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            async def run():
                if self.connect_udp:
                    dtuple=(address, 53)
                else:
                    dtuple=None
                async with await self.backend.make_socket(
                        dns.inet.af_for_address(address),
                        socket.SOCK_DGRAM, 0, None, dtuple) as s:
                    q = dns.message.make_query(qname, dns.rdatatype.A)
                    return await dns.asyncquery.udp(q, address, sock=s,
                                                    timeout=2)
            response = self.async_run(run)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)

    def testQueryTCP(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            async def run():
                q = dns.message.make_query(qname, dns.rdatatype.A)
                return await dns.asyncquery.tcp(q, address, timeout=2)
            response = self.async_run(run)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)

    def testQueryTCPWithSocket(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            async def run():
                async with await self.backend.make_socket(
                        dns.inet.af_for_address(address),
                        socket.SOCK_STREAM, 0,
                        None,
                        (address, 53), 2) as s:
                    # for basic coverage
                    await s.getsockname()
                    q = dns.message.make_query(qname, dns.rdatatype.A)
                    return await dns.asyncquery.tcp(q, address, sock=s,
                                                    timeout=2)
            response = self.async_run(run)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)

    @unittest.skipIf(not _ssl_available, "SSL not available")
    def testQueryTLS(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            async def run():
                q = dns.message.make_query(qname, dns.rdatatype.A)
                return await dns.asyncquery.tls(q, address, timeout=2)
            response = self.async_run(run)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)

    @unittest.skipIf(not _ssl_available, "SSL not available")
    def testQueryTLSWithSocket(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            async def run():
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                async with await self.backend.make_socket(
                        dns.inet.af_for_address(address),
                        socket.SOCK_STREAM, 0,
                        None,
                        (address, 853), 2,
                        ssl_context, None) as s:
                    # for basic coverage
                    await s.getsockname()
                    q = dns.message.make_query(qname, dns.rdatatype.A)
                    return await dns.asyncquery.tls(q, '8.8.8.8', sock=s,
                                                    timeout=2)
            response = self.async_run(run)
            rrs = response.get_rrset(response.answer, qname,
                                     dns.rdataclass.IN, dns.rdatatype.A)
            self.assertTrue(rrs is not None)
            seen = set([rdata.address for rdata in rrs])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)

    def testQueryUDPFallback(self):
        for address in query_addresses:
            qname = dns.name.from_text('.')
            async def run():
                q = dns.message.make_query(qname, dns.rdatatype.DNSKEY)
                return await dns.asyncquery.udp_with_fallback(q, address,
                                                              timeout=2)
            (_, tcp) = self.async_run(run)
            self.assertTrue(tcp)

    def testQueryUDPFallbackNoFallback(self):
        for address in query_addresses:
            qname = dns.name.from_text('dns.google.')
            async def run():
                q = dns.message.make_query(qname, dns.rdatatype.A)
                return await dns.asyncquery.udp_with_fallback(q, address,
                                                              timeout=2)
            (_, tcp) = self.async_run(run)
            self.assertFalse(tcp)

    def testUDPReceiveQuery(self):
        if self.connect_udp:
            self.skipTest('test needs connectionless sockets')
        async def run():
            async with await self.backend.make_socket(
                    socket.AF_INET, socket.SOCK_DGRAM,
                    source=('127.0.0.1', 0)) as listener:
                listener_address = await listener.getsockname()
                async with await self.backend.make_socket(
                        socket.AF_INET, socket.SOCK_DGRAM,
                        source=('127.0.0.1', 0)) as sender:
                    sender_address = await sender.getsockname()
                    q = dns.message.make_query('dns.google', dns.rdatatype.A)
                    await dns.asyncquery.send_udp(sender, q, listener_address)
                    expiration = time.time() + 2
                    (_, _, recv_address) = await dns.asyncquery.receive_udp(
                            listener, expiration=expiration)
                    return (sender_address, recv_address)
        (sender_address, recv_address) = self.async_run(run)
        self.assertEqual(sender_address, recv_address)

    def testUDPReceiveTimeout(self):
        if self.connect_udp:
            self.skipTest('test needs connectionless sockets')
        async def arun():
            async with await self.backend.make_socket(socket.AF_INET,
                                                      socket.SOCK_DGRAM, 0,
                                                      ('127.0.0.1', 0)) as s:
                try:
                    # for basic coverage
                    await s.getpeername()
                except Exception:
                    # we expect failure as we haven't connected the socket
                    pass
                await s.recvfrom(1000, 0.05)
        def run():
            self.async_run(arun)
        self.assertRaises(dns.exception.Timeout, run)

    @unittest.skipIf(not dns.query._have_httpx, "httpx not available")
    def testDOHGetRequest(self):
        if self.backend.name() == 'curio':
            self.skipTest('anyio dropped curio support')
        async def run():
            nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
            q = dns.message.make_query('example.com.', dns.rdatatype.A)
            r = await dns.asyncquery.https(q, nameserver_url, post=False,
                                           timeout=4)
            self.assertTrue(q.is_response(r))
        self.async_run(run)

    @unittest.skipIf(not dns.query._have_httpx, "httpx not available")
    def testDOHGetRequestHttp1(self):
        if self.backend.name() == 'curio':
            self.skipTest('anyio dropped curio support')
        async def run():
            saved_have_http2 = dns.query._have_http2
            try:
                dns.query._have_http2 = False
                nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
                q = dns.message.make_query('example.com.', dns.rdatatype.A)
                r = await dns.asyncquery.https(q, nameserver_url, post=False,
                                               timeout=4)
                self.assertTrue(q.is_response(r))
            finally:
                dns.query._have_http2 = saved_have_http2
        self.async_run(run)

    @unittest.skipIf(not dns.query._have_httpx, "httpx not available")
    def testDOHPostRequest(self):
        if self.backend.name() == 'curio':
            self.skipTest('anyio dropped curio support')
        async def run():
            nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
            q = dns.message.make_query('example.com.', dns.rdatatype.A)
            r = await dns.asyncquery.https(q, nameserver_url, post=True,
                                           timeout=4)
            self.assertTrue(q.is_response(r))
        self.async_run(run)

    @unittest.skipIf(not dns.query._have_httpx, "httpx not available")
    def testResolverDOH(self):
        if self.backend.name() == 'curio':
            self.skipTest('anyio dropped curio support')
        async def run():
            res = dns.asyncresolver.Resolver(configure=False)
            res.nameservers = ['https://dns.google/dns-query']
            answer = await res.resolve('dns.google', 'A', backend=self.backend)
            seen = set([rdata.address for rdata in answer])
            self.assertTrue('8.8.8.8' in seen)
            self.assertTrue('8.8.4.4' in seen)
        self.async_run(run)

    def testSleep(self):
        async def run():
            before = time.time()
            await self.backend.sleep(0.1)
            after = time.time()
            self.assertTrue(after - before >= 0.1)
        self.async_run(run)

try:
    import trio
    import sniffio

    class TrioAsyncDetectionTests(AsyncDetectionTests):
        sniff_result = 'trio'
        def async_run(self, afunc):
            return trio.run(afunc)

    class TrioNoSniffioAsyncDetectionTests(NoSniffioAsyncDetectionTests):
        expect_raise = True
        def async_run(self, afunc):
            return trio.run(afunc)

    class TrioAsyncTests(AsyncTests):
        connect_udp = False
        def setUp(self):
            self.backend = dns.asyncbackend.set_default_backend('trio')

        def async_run(self, afunc):
            return trio.run(afunc)
except ImportError:
    pass

try:
    import curio
    import sniffio

    class CurioAsyncDetectionTests(AsyncDetectionTests):
        sniff_result = 'curio'
        def async_run(self, afunc):
            return curio.run(afunc)

    class CurioNoSniffioAsyncDetectionTests(NoSniffioAsyncDetectionTests):
        expect_raise = True
        def async_run(self, afunc):
            return curio.run(afunc)

    class CurioAsyncTests(AsyncTests):
        connect_udp = False
        def setUp(self):
            self.backend = dns.asyncbackend.set_default_backend('curio')

        def async_run(self, afunc):
            return curio.run(afunc)
except ImportError:
    pass
