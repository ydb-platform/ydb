# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest

import dns.exception
import dns.flags
import dns.message
import dns.renderer
import dns.tsig
import dns.tsigkeyring

basic_answer = \
    """flags QR
edns 0
payload 4096
;QUESTION
foo.example. IN A
;ANSWER
foo.example. 30 IN A 10.0.0.1
foo.example. 30 IN A 10.0.0.2
"""

class RendererTestCase(unittest.TestCase):
    def test_basic(self):
        r = dns.renderer.Renderer(flags=dns.flags.QR, max_size=512)
        qname = dns.name.from_text('foo.example')
        r.add_question(qname, dns.rdatatype.A)
        rds = dns.rdataset.from_text('in', 'a', 30, '10.0.0.1', '10.0.0.2')
        r.add_rdataset(dns.renderer.ANSWER, qname, rds)
        r.add_edns(0, 0, 4096)
        r.write_header()
        wire = r.get_wire()
        message = dns.message.from_wire(wire)
        expected = dns.message.from_text(basic_answer)
        # Our rendered message purposely has a random query id so we
        # exercise that code, so copy it into the expected message.
        expected.id = message.id
        self.assertEqual(message, expected)

    def test_tsig(self):
        r = dns.renderer.Renderer(flags=dns.flags.RD, max_size=512)
        qname = dns.name.from_text('foo.example')
        r.add_question(qname, dns.rdatatype.A)
        keyring = dns.tsigkeyring.from_text({'key' : '12345678'})
        keyname = next(iter(keyring))
        r.write_header()
        r.add_tsig(keyname, keyring[keyname], 300, r.id, 0, b'', b'',
                   dns.tsig.HMAC_SHA256)
        wire = r.get_wire()
        message = dns.message.from_wire(wire, keyring=keyring)
        expected = dns.message.make_query(qname, dns.rdatatype.A)
        expected.id = message.id
        self.assertEqual(message, expected)

    def test_multi_tsig(self):
        qname = dns.name.from_text('foo.example')
        keyring = dns.tsigkeyring.from_text({'key' : '12345678'})
        keyname = next(iter(keyring))

        r = dns.renderer.Renderer(flags=dns.flags.RD, max_size=512)
        r.add_question(qname, dns.rdatatype.A)
        r.write_header()
        ctx = r.add_multi_tsig(None, keyname, keyring[keyname], 300, r.id, 0,
                               b'', b'', dns.tsig.HMAC_SHA256)
        wire = r.get_wire()
        message = dns.message.from_wire(wire, keyring=keyring, multi=True)
        expected = dns.message.make_query(qname, dns.rdatatype.A)
        expected.id = message.id
        self.assertEqual(message, expected)

        r = dns.renderer.Renderer(flags=dns.flags.RD, max_size=512)
        r.add_question(qname, dns.rdatatype.A)
        r.write_header()
        ctx = r.add_multi_tsig(ctx, keyname, keyring[keyname], 300, r.id, 0,
                               b'', b'', dns.tsig.HMAC_SHA256)
        wire = r.get_wire()
        message = dns.message.from_wire(wire, keyring=keyring,
                                        tsig_ctx=message.tsig_ctx, multi=True)
        expected = dns.message.make_query(qname, dns.rdatatype.A)
        expected.id = message.id
        self.assertEqual(message, expected)


    def test_going_backwards_fails(self):
        r = dns.renderer.Renderer(flags=dns.flags.QR, max_size=512)
        qname = dns.name.from_text('foo.example')
        r.add_question(qname, dns.rdatatype.A)
        r.add_edns(0, 0, 4096)
        rds = dns.rdataset.from_text('in', 'a', 30, '10.0.0.1', '10.0.0.2')
        def bad():
            r.add_rdataset(dns.renderer.ANSWER, qname, rds)
        self.assertRaises(dns.exception.FormError, bad)
