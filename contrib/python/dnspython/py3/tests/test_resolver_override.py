# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import socket
import sys
import unittest

import dns.name
import dns.rdataclass
import dns.rdatatype
import dns.resolver

# Some tests require the internet to be available to run, so let's
# skip those if it's not there.
_network_available = True
try:
    socket.gethostbyname('dnspython.org')
except socket.gaierror:
    _network_available = False


@unittest.skipIf(not _network_available, "Internet not reachable")
class OverrideSystemResolverTestCase(unittest.TestCase):

    def setUp(self):
        self.res = dns.resolver.Resolver(configure=False)
        self.res.nameservers = ['8.8.8.8']
        self.res.cache = dns.resolver.LRUCache()
        dns.resolver.override_system_resolver(self.res)

    def tearDown(self):
        dns.resolver.restore_system_resolver()
        self.res = None

    def test_override(self):
        self.assertTrue(socket.getaddrinfo is
                        dns.resolver._getaddrinfo)
        socket.gethostbyname('www.dnspython.org')
        answer = self.res.cache.get((dns.name.from_text('www.dnspython.org.'),
                                dns.rdatatype.A, dns.rdataclass.IN))
        self.assertTrue(answer is not None)
        self.res.cache.flush()
        socket.gethostbyname_ex('www.dnspython.org')
        answer = self.res.cache.get((dns.name.from_text('www.dnspython.org.'),
                                dns.rdatatype.A, dns.rdataclass.IN))
        self.assertTrue(answer is not None)
        self.res.cache.flush()
        socket.getfqdn('8.8.8.8')
        answer = self.res.cache.get(
            (dns.name.from_text('8.8.8.8.in-addr.arpa.'),
             dns.rdatatype.PTR, dns.rdataclass.IN))
        self.assertTrue(answer is not None)
        self.res.cache.flush()
        socket.gethostbyaddr('8.8.8.8')
        answer = self.res.cache.get(
            (dns.name.from_text('8.8.8.8.in-addr.arpa.'),
             dns.rdatatype.PTR, dns.rdataclass.IN))
        self.assertTrue(answer is not None)
        # restoring twice is harmless, so we restore now instead of
        # waiting for tearDown so we can assert that it worked
        dns.resolver.restore_system_resolver()
        self.assertTrue(socket.getaddrinfo is
                        dns.resolver._original_getaddrinfo)

    def equivalent_info(self, a, b):
        if len(a) != len(b):
            return False
        for x in a:
            if x not in b:
                # Windows does not set the protocol to non-zero, so try
                # looking for a zero protocol.
                y = (x[0], x[1], 0, x[3], x[4])
                if y not in b:
                    print('NOT EQUIVALENT')
                    print(a)
                    print(b)
                    return False
        return True

    def equivalent(self, *args, **kwargs):
        a = socket.getaddrinfo(*args, **kwargs)
        b = dns.resolver._original_getaddrinfo(*args, **kwargs)
        return self.equivalent_info(a, b)

    @unittest.skipIf(sys.platform == 'win32',
                     'avoid windows original getaddrinfo issues')
    def test_basic_getaddrinfo(self):
        self.assertTrue(self.equivalent('dns.google', 53, socket.AF_INET,
                                        socket.SOCK_DGRAM))
        self.assertTrue(self.equivalent('dns.google', 53, socket.AF_INET6,
                                        socket.SOCK_DGRAM))
        self.assertTrue(self.equivalent('dns.google', None, socket.AF_UNSPEC,
                                        socket.SOCK_DGRAM))
        self.assertTrue(self.equivalent('8.8.8.8', 53, socket.AF_INET,
                                        socket.SOCK_DGRAM))
        self.assertTrue(self.equivalent('2001:4860:4860::8888', 53,
                                        socket.AF_INET6, socket.SOCK_DGRAM))
        self.assertTrue(self.equivalent('8.8.8.8', 53, socket.AF_INET,
                                        socket.SOCK_DGRAM,
                                        flags=socket.AI_NUMERICHOST))
        self.assertTrue(self.equivalent('2001:4860:4860::8888', 53,
                                        socket.AF_INET6, socket.SOCK_DGRAM,
                                        flags=socket.AI_NUMERICHOST))

    def test_getaddrinfo_nxdomain(self):
        try:
            socket.getaddrinfo('nxdomain.dnspython.org.', 53)
            self.assertTrue(False) # should not happen!
        except socket.gaierror as e:
            self.assertEqual(e.errno, socket.EAI_NONAME)

    def test_getaddrinfo_service(self):
        a = socket.getaddrinfo('dns.google', 'domain')
        b = socket.getaddrinfo('dns.google', 53)
        self.assertTrue(self.equivalent_info(a, b))
        try:
            socket.getaddrinfo('dns.google', 'domain',
                               flags=socket.AI_NUMERICSERV)
            self.assertTrue(False) # should not happen!
        except socket.gaierror as e:
            self.assertEqual(e.errno, socket.EAI_NONAME)

    def test_getaddrinfo_only_service(self):
        infos = socket.getaddrinfo(service=53, family=socket.AF_INET,
                                   socktype=socket.SOCK_DGRAM,
                                   proto=socket.IPPROTO_UDP)
        self.assertEqual(len(infos), 1)
        info = infos[0]
        self.assertEqual(info[0], socket.AF_INET)
        self.assertEqual(info[1], socket.SOCK_DGRAM)
        self.assertEqual(info[2], socket.IPPROTO_UDP)
        self.assertEqual(info[4], ('127.0.0.1', 53))

    def test_unknown_service_fails(self):
        with self.assertRaises(socket.gaierror):
            socket.getaddrinfo('dns.google.', 'bogus-service')

    def test_getnameinfo_tcp(self):
        info = socket.getnameinfo(('8.8.8.8', 53))
        self.assertEqual(info, ('dns.google', 'domain'))

    def test_getnameinfo_udp(self):
        info = socket.getnameinfo(('8.8.8.8', 53), socket.NI_DGRAM)
        self.assertEqual(info, ('dns.google', 'domain'))


# Give up on testing this for now as all of the names I've considered
# using for testing are part of CDNs and there is deep magic in
# gethostbyaddr() that python's getfqdn() is using.  At any rate,
# the problem is that dnspython just gives up whereas the native python
# code is looking up www.dnspython.org, picking a CDN IPv4 address
# (sometimes) and returning the reverse lookup of that address (i.e.
# the domain name of the CDN server).  This isn't what I'd consider the
# FQDN of www.dnspython.org to be!
#
#    def test_getfqdn(self):
#        b = socket.getfqdn('www.dnspython.org')
#        # we do this now because python's original getfqdn calls
#        # gethostbyaddr() and we don't want it to call us!
#        dns.resolver.restore_system_resolver()
#        a = dns.resolver._original_getfqdn('www.dnspython.org')
#        self.assertEqual(dns.name.from_text(a), dns.name.from_text(b))

    def test_gethostbyaddr(self):
        a = dns.resolver._original_gethostbyaddr('8.8.8.8')
        b = socket.gethostbyaddr('8.8.8.8')
        # We only test elements 0 and 2 as we don't set aliases currently!
        self.assertEqual(a[0], b[0])
        self.assertEqual(a[2], b[2])
        a = dns.resolver._original_gethostbyaddr('2001:4860:4860::8888')
        b = socket.gethostbyaddr('2001:4860:4860::8888')
        self.assertEqual(a[0], b[0])
        self.assertEqual(a[2], b[2])


class FakeResolver:
    def resolve(self, *args, **kwargs):
        raise dns.exception.Timeout


class OverrideSystemResolverUsingFakeResolverTestCase(unittest.TestCase):

    def setUp(self):
        self.res = FakeResolver()
        dns.resolver.override_system_resolver(self.res)

    def tearDown(self):
        dns.resolver.restore_system_resolver()
        self.res = None

    def test_temporary_failure(self):
        with self.assertRaises(socket.gaierror):
            socket.getaddrinfo('dns.google')

    # We don't need the fake resolver for the following tests, but we
    # don't need the live network either, so we're testing here.

    def test_no_host_or_service_fails(self):
        with self.assertRaises(socket.gaierror):
            socket.getaddrinfo()

    def test_AI_ADDRCONFIG_fails(self):
        with self.assertRaises(socket.gaierror):
            socket.getaddrinfo('dns.google', flags=socket.AI_ADDRCONFIG)

    def test_gethostbyaddr_of_name_fails(self):
        with self.assertRaises(socket.gaierror):
            socket.gethostbyaddr('bogus')


@unittest.skipIf(not _network_available, "Internet not reachable")
class OverrideSystemResolverUsingDefaultResolverTestCase(unittest.TestCase):

    def setUp(self):
        self.res = FakeResolver()
        dns.resolver.override_system_resolver()

    def tearDown(self):
        dns.resolver.restore_system_resolver()
        self.res = None

    def test_override(self):
        self.assertEqual(dns.resolver._resolver, dns.resolver.default_resolver)
