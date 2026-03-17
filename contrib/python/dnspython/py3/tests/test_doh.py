# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2003-2007, 2009-2011 Nominum, Inc.
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
import unittest
import random
import socket
try:
    import ssl
    _have_ssl = True
except Exception:
    _have_ssl = False

import dns.message
import dns.query
import dns.rdatatype
import dns.resolver

if dns.query._have_requests:
    import requests
    from requests.exceptions import SSLError

if dns.query._have_httpx:
    import httpx

# Probe for IPv4 and IPv6
resolver_v4_addresses = []
resolver_v6_addresses = []
try:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(4)
        s.connect(('8.8.8.8', 53))
    resolver_v4_addresses = [
        '1.1.1.1',
        '8.8.8.8',
        # '9.9.9.9',
    ]
except Exception:
    pass
try:
    with socket.socket(socket.AF_INET6, socket.SOCK_DGRAM) as s:
        s.connect(('2001:4860:4860::8888', 53))
    resolver_v6_addresses = [
        '2606:4700:4700::1111',
        # Google says 404
        # '2001:4860:4860::8888',
        # '2620:fe::fe',
    ]
except Exception:
    pass

KNOWN_ANYCAST_DOH_RESOLVER_URLS = ['https://cloudflare-dns.com/dns-query',
                                   'https://dns.google/dns-query',
                                   # 'https://dns11.quad9.net/dns-query',
                                   ]

# Some tests require the internet to be available to run, so let's
# skip those if it's not there.
_network_available = True
try:
    socket.gethostbyname('dnspython.org')
except socket.gaierror:
    _network_available = False


@unittest.skipUnless(dns.query._have_requests and _network_available,
                     "Python requests cannot be imported; no DNS over HTTPS (DOH)")
class DNSOverHTTPSTestCaseRequests(unittest.TestCase):
    def setUp(self):
        self.session = requests.sessions.Session()

    def tearDown(self):
        self.session.close()

    def test_get_request(self):
        nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
        q = dns.message.make_query('example.com.', dns.rdatatype.A)
        r = dns.query.https(q, nameserver_url, session=self.session, post=False,
                            timeout=4)
        self.assertTrue(q.is_response(r))

    def test_post_request(self):
        nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
        q = dns.message.make_query('example.com.', dns.rdatatype.A)
        r = dns.query.https(q, nameserver_url, session=self.session, post=True,
                            timeout=4)
        self.assertTrue(q.is_response(r))

    def test_build_url_from_ip(self):
        self.assertTrue(resolver_v4_addresses or resolver_v6_addresses)
        if resolver_v4_addresses:
            nameserver_ip = random.choice(resolver_v4_addresses)
            q = dns.message.make_query('example.com.', dns.rdatatype.A)
            # For some reason Google's DNS over HTTPS fails when you POST to
            # https://8.8.8.8/dns-query
            # So we're just going to do GET requests here
            r = dns.query.https(q, nameserver_ip, session=self.session,
                                post=False, timeout=4)

            self.assertTrue(q.is_response(r))
        if resolver_v6_addresses:
            nameserver_ip = random.choice(resolver_v6_addresses)
            q = dns.message.make_query('example.com.', dns.rdatatype.A)
            r = dns.query.https(q, nameserver_ip, session=self.session,
                                post=False, timeout=4)
            self.assertTrue(q.is_response(r))

    def test_bootstrap_address(self):
        # We test this to see if v4 is available
        if resolver_v4_addresses:
            ip = '185.228.168.168'
            invalid_tls_url = 'https://{}/doh/family-filter/'.format(ip)
            valid_tls_url = 'https://doh.cleanbrowsing.org/doh/family-filter/'
            q = dns.message.make_query('example.com.', dns.rdatatype.A)
            # make sure CleanBrowsing's IP address will fail TLS certificate
            # check
            with self.assertRaises(SSLError):
                dns.query.https(q, invalid_tls_url, session=self.session,
                                timeout=4)
            # use host header
            r = dns.query.https(q, valid_tls_url, session=self.session,
                                bootstrap_address=ip, timeout=4)
            self.assertTrue(q.is_response(r))

    def test_new_session(self):
        nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
        q = dns.message.make_query('example.com.', dns.rdatatype.A)
        r = dns.query.https(q, nameserver_url, timeout=4)
        self.assertTrue(q.is_response(r))

    def test_resolver(self):
        res = dns.resolver.Resolver(configure=False)
        res.nameservers = ['https://dns.google/dns-query']
        answer = res.resolve('dns.google', 'A')
        seen = set([rdata.address for rdata in answer])
        self.assertTrue('8.8.8.8' in seen)
        self.assertTrue('8.8.4.4' in seen)


@unittest.skipUnless(dns.query._have_httpx and _network_available and _have_ssl,
                     "Python httpx cannot be imported; no DNS over HTTPS (DOH)")
class DNSOverHTTPSTestCaseHttpx(unittest.TestCase):
    def setUp(self):
        self.session = httpx.Client(http1=True, http2=True, verify=True)

    def tearDown(self):
        self.session.close()

    def test_get_request(self):
        nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
        q = dns.message.make_query('example.com.', dns.rdatatype.A)
        r = dns.query.https(q, nameserver_url, session=self.session, post=False,
                            timeout=4)
        self.assertTrue(q.is_response(r))

    def test_get_request_http1(self):
        saved_have_http2 = dns.query._have_http2
        try:
            dns.query._have_http2 = False
            nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
            q = dns.message.make_query('example.com.', dns.rdatatype.A)
            r = dns.query.https(q, nameserver_url, session=self.session, post=False,
                                timeout=4)
            self.assertTrue(q.is_response(r))
        finally:
            dns.query._have_http2 = saved_have_http2

    def test_post_request(self):
        nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
        q = dns.message.make_query('example.com.', dns.rdatatype.A)
        r = dns.query.https(q, nameserver_url, session=self.session, post=True,
                            timeout=4)
        self.assertTrue(q.is_response(r))

    def test_build_url_from_ip(self):
        self.assertTrue(resolver_v4_addresses or resolver_v6_addresses)
        if resolver_v4_addresses:
            nameserver_ip = random.choice(resolver_v4_addresses)
            q = dns.message.make_query('example.com.', dns.rdatatype.A)
            # For some reason Google's DNS over HTTPS fails when you POST to
            # https://8.8.8.8/dns-query
            # So we're just going to do GET requests here
            r = dns.query.https(q, nameserver_ip, session=self.session,
                                post=False, timeout=4)

            self.assertTrue(q.is_response(r))
        if resolver_v6_addresses:
            nameserver_ip = random.choice(resolver_v6_addresses)
            q = dns.message.make_query('example.com.', dns.rdatatype.A)
            r = dns.query.https(q, nameserver_ip, session=self.session,
                                post=False, timeout=4)
            self.assertTrue(q.is_response(r))

    def test_bootstrap_address_fails(self):
        # We test this to see if v4 is available
        if resolver_v4_addresses:
            ip = '185.228.168.168'
            invalid_tls_url = 'https://{}/doh/family-filter/'.format(ip)
            valid_tls_url = 'https://doh.cleanbrowsing.org/doh/family-filter/'
            q = dns.message.make_query('example.com.', dns.rdatatype.A)
            # make sure CleanBrowsing's IP address will fail TLS certificate
            # check.  On 3.6 we get ssl.CertificateError instead of
            # httpx.ConnectError.
            with self.assertRaises((httpx.ConnectError, ssl.CertificateError)):
                dns.query.https(q, invalid_tls_url, session=self.session,
                                timeout=4)
            # We can't do the Host header and SNI magic with httpx, but
            # we are demanding httpx be used by providing a session, so
            # we should get a NoDOH exception.
            with self.assertRaises(dns.query.NoDOH):
                dns.query.https(q, valid_tls_url, session=self.session,
                                bootstrap_address=ip, timeout=4)

    def test_new_session(self):
        nameserver_url = random.choice(KNOWN_ANYCAST_DOH_RESOLVER_URLS)
        q = dns.message.make_query('example.com.', dns.rdatatype.A)
        r = dns.query.https(q, nameserver_url, timeout=4)
        self.assertTrue(q.is_response(r))

    def test_resolver(self):
        res = dns.resolver.Resolver(configure=False)
        res.nameservers = ['https://dns.google/dns-query']
        answer = res.resolve('dns.google', 'A')
        seen = set([rdata.address for rdata in answer])
        self.assertTrue('8.8.8.8' in seen)
        self.assertTrue('8.8.4.4' in seen)


if __name__ == '__main__':
    unittest.main()
