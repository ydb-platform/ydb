# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import base64
import unittest

import dns.tsig
import dns.tsigkeyring

text_keyring = {
    'keyname.' : ('hmac-sha256.', 'NjHwPsMKjdN++dOfE5iAiQ==')
}

alt_text_keyring = {
    'keyname.' : (dns.tsig.HMAC_SHA256, 'NjHwPsMKjdN++dOfE5iAiQ==')
}

old_text_keyring = {
    'keyname.' : 'NjHwPsMKjdN++dOfE5iAiQ=='
}

key = dns.tsig.Key('keyname.', 'NjHwPsMKjdN++dOfE5iAiQ==')

rich_keyring = { key.name : key }

old_rich_keyring = { key.name : key.secret }

class TSIGKeyRingTestCase(unittest.TestCase):

    def test_from_text(self):
        """text keyring -> rich keyring"""
        rkeyring = dns.tsigkeyring.from_text(text_keyring)
        self.assertEqual(rkeyring, rich_keyring)

    def test_from_alt_text(self):
        """alternate format text keyring -> rich keyring"""
        rkeyring = dns.tsigkeyring.from_text(alt_text_keyring)
        self.assertEqual(rkeyring, rich_keyring)

    def test_from_old_text(self):
        """old format text keyring -> rich keyring"""
        rkeyring = dns.tsigkeyring.from_text(old_text_keyring)
        self.assertEqual(rkeyring, old_rich_keyring)

    def test_to_text(self):
        """text keyring -> rich keyring -> text keyring"""
        tkeyring = dns.tsigkeyring.to_text(rich_keyring)
        self.assertEqual(tkeyring, text_keyring)

    def test_old_to_text(self):
        """text keyring -> rich keyring -> text keyring"""
        tkeyring = dns.tsigkeyring.to_text(old_rich_keyring)
        self.assertEqual(tkeyring, old_text_keyring)

    def test_from_and_to_text(self):
        """text keyring -> rich keyring -> text keyring"""
        rkeyring = dns.tsigkeyring.from_text(text_keyring)
        tkeyring = dns.tsigkeyring.to_text(rkeyring)
        self.assertEqual(tkeyring, text_keyring)

    def test_old_from_and_to_text(self):
        """text keyring -> rich keyring -> text keyring"""
        rkeyring = dns.tsigkeyring.from_text(old_text_keyring)
        tkeyring = dns.tsigkeyring.to_text(rkeyring)
        self.assertEqual(tkeyring, old_text_keyring)
