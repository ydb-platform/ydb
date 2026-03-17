# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest
from unittest.mock import Mock
import time
import base64

import dns.rcode
import dns.tsig
import dns.tsigkeyring
import dns.message
import dns.rdtypes.ANY.TKEY

keyring = dns.tsigkeyring.from_text(
    {
        'keyname.' : 'NjHwPsMKjdN++dOfE5iAiQ=='
    }
)

keyname = dns.name.from_text('keyname')


class TSIGTestCase(unittest.TestCase):

    def test_get_context(self):
        key = dns.tsig.Key('foo.com', 'abcd', 'hmac-sha256')
        ctx = dns.tsig.get_context(key)
        self.assertEqual(ctx.name, 'hmac-sha256')
        key = dns.tsig.Key('foo.com', 'abcd', 'hmac-sha512')
        ctx = dns.tsig.get_context(key)
        self.assertEqual(ctx.name, 'hmac-sha512')
        bogus = dns.tsig.Key('foo.com', 'abcd', 'bogus')
        with self.assertRaises(NotImplementedError):
            dns.tsig.get_context(bogus)

    def test_tsig_message_properties(self):
        m = dns.message.make_query('example', 'a')
        self.assertIsNone(m.keyname)
        self.assertIsNone(m.keyalgorithm)
        self.assertIsNone(m.tsig_error)
        m.use_tsig(keyring, keyname)
        self.assertEqual(m.keyname, keyname)
        self.assertEqual(m.keyalgorithm, dns.tsig.default_algorithm)
        self.assertEqual(m.tsig_error, dns.rcode.NOERROR)
        m = dns.message.make_query('example', 'a')
        m.use_tsig(keyring, keyname, tsig_error=dns.rcode.BADKEY)
        self.assertEqual(m.tsig_error, dns.rcode.BADKEY)

    def test_verify_mac_for_context(self):
        key = dns.tsig.Key('foo.com', 'abcd', 'hmac-sha512')
        ctx = dns.tsig.get_context(key)
        bad_expected = b'xxxxxxxxxx'
        with self.assertRaises(dns.tsig.BadSignature):
            ctx.verify(bad_expected)

    def test_validate(self):
        # make message and grab the TSIG
        m = dns.message.make_query('example', 'a')
        m.use_tsig(keyring, keyname, algorithm=dns.tsig.HMAC_SHA256)
        w = m.to_wire()
        tsig = m.tsig[0]

        # get the time and create a key with matching characteristics
        now = int(time.time())
        key = dns.tsig.Key('foo.com', 'abcd', 'hmac-sha256')

        # add enough to the time to take it over the fudge amount
        with self.assertRaises(dns.tsig.BadTime):
            dns.tsig.validate(w, key, dns.name.from_text('foo.com'),
                              tsig, now + 1000, b'', 0)

        # change the key name
        with self.assertRaises(dns.tsig.BadKey):
            dns.tsig.validate(w, key, dns.name.from_text('bar.com'),
                              tsig, now, b'', 0)

        # change the key algorithm
        key = dns.tsig.Key('foo.com', 'abcd', 'hmac-sha512')
        with self.assertRaises(dns.tsig.BadAlgorithm):
            dns.tsig.validate(w, key, dns.name.from_text('foo.com'),
                              tsig, now, b'', 0)

    def test_gssapi_context(self):
        def verify_signature(data, mac):
            if data == b'throw':
                raise Exception
            return None

        # mock out the gssapi context to return some dummy values
        gssapi_context_mock = Mock()
        gssapi_context_mock.get_signature.return_value = b'xxxxxxxxxxx'
        gssapi_context_mock.verify_signature.side_effect = verify_signature

        # create the key and add it to the keyring
        keyname = 'gsstsigtest'
        key = dns.tsig.Key(keyname, gssapi_context_mock, 'gss-tsig')
        ctx = dns.tsig.get_context(key)
        self.assertEqual(ctx.name, 'gss-tsig')
        gsskeyname = dns.name.from_text(keyname)
        keyring[gsskeyname] = key

        # make sure we can get the keyring (no exception == success)
        text = dns.tsigkeyring.to_text(keyring)
        self.assertNotEqual(text, '')

        # test exceptional case for _verify_mac_for_context
        with self.assertRaises(dns.tsig.BadSignature):
            ctx.update(b'throw')
            ctx.verify(b'bogus')
        gssapi_context_mock.verify_signature.assert_called()
        self.assertEqual(gssapi_context_mock.verify_signature.call_count, 1)

        # simulate case where TKEY message is used to establish the context;
        # first, the query from the client
        tkey_message = dns.message.make_query(keyname, 'tkey', 'any')

        # test existent/non-existent keys in the keyring
        adapted_keyring = dns.tsig.GSSTSigAdapter(keyring)

        fetched_key = adapted_keyring(tkey_message, gsskeyname)
        self.assertEqual(fetched_key, key)
        key = adapted_keyring(None, gsskeyname)
        self.assertEqual(fetched_key, key)
        key = adapted_keyring(tkey_message, "dummy")
        self.assertEqual(key, None)

        # create a response, TKEY and turn it into bytes, simulating the server
        # sending the response to the query
        tkey_response = dns.message.make_response(tkey_message)
        key = base64.b64decode('KEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEY')
        tkey = dns.rdtypes.ANY.TKEY.TKEY(dns.rdataclass.ANY,
                                         dns.rdatatype.TKEY,
                                         dns.name.from_text('gss-tsig.'),
                                         1594203795, 1594206664,
                                         3, 0, key)

        # add the TKEY answer and sign it
        tkey_response.set_rcode(dns.rcode.NOERROR)
        tkey_response.answer = [
            dns.rrset.from_rdata(dns.name.from_text(keyname), 0, tkey)]
        tkey_response.use_tsig(keyring=dns.tsig.GSSTSigAdapter(keyring),
                               keyname=gsskeyname,
                               algorithm=dns.tsig.GSS_TSIG)

        # "send" it to the client
        tkey_wire = tkey_response.to_wire()

        # grab the response from the "server" and simulate the client side
        dns.message.from_wire(tkey_wire, dns.tsig.GSSTSigAdapter(keyring))

        # assertions to make sure the "gssapi" functions were called
        gssapi_context_mock.get_signature.assert_called()
        self.assertEqual(gssapi_context_mock.get_signature.call_count, 1)
        gssapi_context_mock.verify_signature.assert_called()
        self.assertEqual(gssapi_context_mock.verify_signature.call_count, 2)
        gssapi_context_mock.step.assert_called()
        self.assertEqual(gssapi_context_mock.step.call_count, 1)

        # create example message and go to/from wire to simulate sign/verify
        # of regular messages
        a_message = dns.message.make_query('example', 'a')
        a_message.use_tsig(dns.tsig.GSSTSigAdapter(keyring), gsskeyname)
        a_wire = a_message.to_wire()
        # not raising is passing
        dns.message.from_wire(a_wire, dns.tsig.GSSTSigAdapter(keyring))

        # assertions to make sure the "gssapi" functions were called again
        gssapi_context_mock.get_signature.assert_called()
        self.assertEqual(gssapi_context_mock.get_signature.call_count, 2)
        gssapi_context_mock.verify_signature.assert_called()
        self.assertEqual(gssapi_context_mock.verify_signature.call_count, 3)

    def test_sign_and_validate(self):
        m = dns.message.make_query('example', 'a')
        m.use_tsig(keyring, keyname)
        w = m.to_wire()
        # not raising is passing
        dns.message.from_wire(w, keyring)

    def test_validate_with_bad_keyring(self):
        m = dns.message.make_query('example', 'a')
        m.use_tsig(keyring, keyname)
        w = m.to_wire()

        # keyring == None is an error
        with self.assertRaises(dns.message.UnknownTSIGKey):
            dns.message.from_wire(w, None)
        # callable keyring that returns None is an error
        with self.assertRaises(dns.message.UnknownTSIGKey):
            dns.message.from_wire(w, lambda m, n: None)

    def test_sign_and_validate_with_other_data(self):
        m = dns.message.make_query('example', 'a')
        m.use_tsig(keyring, keyname, other_data=b'other')
        w = m.to_wire()
        # not raising is passing
        dns.message.from_wire(w, keyring)

    def test_sign_respond_and_validate(self):
        mq = dns.message.make_query('example', 'a')
        mq.use_tsig(keyring, keyname)
        wq = mq.to_wire()
        mq_with_tsig = dns.message.from_wire(wq, keyring)
        mr = dns.message.make_response(mq)
        mr.use_tsig(keyring, keyname)
        wr = mr.to_wire()
        dns.message.from_wire(wr, keyring, request_mac=mq_with_tsig.mac)

    def make_message_pair(self, qname='example', rdtype='A', tsig_error=0):
        q = dns.message.make_query(qname, rdtype)
        q.use_tsig(keyring=keyring, keyname=keyname)
        q.to_wire()  # to set q.mac
        r = dns.message.make_response(q, tsig_error=tsig_error)
        return(q, r)

    def test_peer_errors(self):
        items = [(dns.rcode.BADSIG, dns.tsig.PeerBadSignature),
                 (dns.rcode.BADKEY, dns.tsig.PeerBadKey),
                 (dns.rcode.BADTIME, dns.tsig.PeerBadTime),
                 (dns.rcode.BADTRUNC, dns.tsig.PeerBadTruncation),
                 (99, dns.tsig.PeerError),
                 ]
        for err, ex in items:
            q, r = self.make_message_pair(tsig_error=err)
            w = r.to_wire()
            def bad():
                dns.message.from_wire(w, keyring=keyring, request_mac=q.mac)
            self.assertRaises(ex, bad)

    def _test_truncated_algorithm(self, alg, length):
        key = dns.tsig.Key('foo', b'abcdefg', algorithm=alg)
        q = dns.message.make_query('example', 'a')
        q.use_tsig(key)
        q2 = dns.message.from_wire(q.to_wire(), keyring=key)

        self.assertTrue(q2.had_tsig)
        self.assertEqual(q2.tsig[0].algorithm, q.tsig[0].algorithm)
        self.assertEqual(len(q2.tsig[0].mac), length // 8)

    def test_hmac_sha256_128(self):
        self._test_truncated_algorithm(dns.tsig.HMAC_SHA256_128, 128)

    def test_hmac_sha384_192(self):
        self._test_truncated_algorithm(dns.tsig.HMAC_SHA384_192, 192)

    def test_hmac_sha512_256(self):
        self._test_truncated_algorithm(dns.tsig.HMAC_SHA512_256, 256)

    def _test_text_format(self, alg):
        key = dns.tsig.Key('foo', b'abcdefg', algorithm=alg)
        q = dns.message.make_query('example', 'a')
        q.use_tsig(key)
        _ = q.to_wire()

        text = q.tsig[0].to_text()
        tsig2 = dns.rdata.from_text('ANY', 'TSIG', text)
        self.assertEqual(tsig2, q.tsig[0])

        q = dns.message.make_query('example', 'a')
        q.use_tsig(key, other_data=b'abc')
        q.use_tsig(key)
        _ = q.to_wire()

        text = q.tsig[0].to_text()
        tsig2 = dns.rdata.from_text('ANY', 'TSIG', text)
        self.assertEqual(tsig2, q.tsig[0])

    def test_text_hmac_sha256_128(self):
        self._test_text_format(dns.tsig.HMAC_SHA256_128)

    def test_text_hmac_sha384_192(self):
        self._test_text_format(dns.tsig.HMAC_SHA384_192)

    def test_text_hmac_sha512_256(self):
        self._test_text_format(dns.tsig.HMAC_SHA512_256)

    def test_non_gss_key_repr(self):
        key = dns.tsig.Key('foo', b'0123456789abcdef' * 2,
                           algorithm=dns.tsig.HMAC_SHA256)
        self.assertEqual(repr(key),
                         "<DNS key name='foo.', algorithm='hmac-sha256.', " +
                         "secret=" +
                         "'MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY='>")

    def test_gss_key_repr(self):
        key = dns.tsig.Key('foo', None,
                           algorithm=dns.tsig.GSS_TSIG)
        self.assertEqual(repr(key),
                         "<DNS key name='foo.', algorithm='gss-tsig.'>")
