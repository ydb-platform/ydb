# -*- coding: utf-8 -*-
import requests
import requests_oauthlib
import oauthlib
import os.path
from io import StringIO
import unittest

from unittest import mock


@mock.patch("oauthlib.oauth1.rfc5849.generate_timestamp")
@mock.patch("oauthlib.oauth1.rfc5849.generate_nonce")
class OAuth1Test(unittest.TestCase):
    def testFormEncoded(self, generate_nonce, generate_timestamp):
        """OAuth1 assumes form encoded if content type is not specified."""
        generate_nonce.return_value = "abc"
        generate_timestamp.return_value = "1"
        oauth = requests_oauthlib.OAuth1("client_key")
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        r = requests.Request(
            method="POST",
            url="http://a.b/path?query=retain",
            auth=oauth,
            data="this=really&is=&+form=encoded",
            headers=headers,
        )
        a = r.prepare()

        self.assertEqual(a.url, "http://a.b/path?query=retain")
        self.assertEqual(a.body, b"this=really&is=&+form=encoded")
        self.assertEqual(
            a.headers.get("Content-Type"), b"application/x-www-form-urlencoded"
        )

        # guess content-type
        r = requests.Request(
            method="POST",
            url="http://a.b/path?query=retain",
            auth=oauth,
            data="this=really&is=&+form=encoded",
        )
        b = r.prepare()
        self.assertEqual(b.url, "http://a.b/path?query=retain")
        self.assertEqual(b.body, b"this=really&is=&+form=encoded")
        self.assertEqual(
            b.headers.get("Content-Type"), b"application/x-www-form-urlencoded"
        )

        self.assertEqual(a.headers.get("Authorization"), b.headers.get("Authorization"))

    def testNonFormEncoded(self, generate_nonce, generate_timestamp):
        """OAuth signature only depend on body if it is form encoded."""
        generate_nonce.return_value = "abc"
        generate_timestamp.return_value = "1"
        oauth = requests_oauthlib.OAuth1("client_key")

        r = requests.Request(
            method="POST",
            url="http://a.b/path?query=retain",
            auth=oauth,
            data="this really is not form encoded",
        )
        a = r.prepare()

        r = requests.Request(
            method="POST", url="http://a.b/path?query=retain", auth=oauth
        )
        b = r.prepare()

        self.assertEqual(a.headers.get("Authorization"), b.headers.get("Authorization"))

        r = requests.Request(
            method="POST",
            url="http://a.b/path?query=retain",
            auth=oauth,
            files={"test": StringIO("hello")},
        )
        c = r.prepare()

        self.assertEqual(b.headers.get("Authorization"), c.headers.get("Authorization"))

    @unittest.skip("test uses real http://httpbin.org")
    def testCanPostBinaryData(self, generate_nonce, generate_timestamp):
        """
        Test we can post binary data. Should prevent regression of the
        UnicodeDecodeError issue.
        """
        generate_nonce.return_value = "abc"
        generate_timestamp.return_value = "1"
        oauth = requests_oauthlib.OAuth1("client_key")
        import yatest.common
        dirname = yatest.common.test_source_path()
        fname = os.path.join(dirname, "test.bin")

        with open(fname, "rb") as f:
            r = requests.post(
                "http://httpbin.org/post",
                data={"hi": "there"},
                files={"media": (os.path.basename(f.name), f)},
                headers={"content-type": "application/octet-stream"},
                auth=oauth,
            )
            self.assertEqual(r.status_code, 200)

    @unittest.skip("test uses real http://httpbin.org")
    def test_url_is_native_str(self, generate_nonce, generate_timestamp):
        """
        Test that the URL is always a native string.
        """
        generate_nonce.return_value = "abc"
        generate_timestamp.return_value = "1"
        oauth = requests_oauthlib.OAuth1("client_key")

        r = requests.get("http://httpbin.org/get", auth=oauth)
        self.assertIsInstance(r.request.url, str)

    @unittest.skip("test uses real http://httpbin.org")
    def test_content_type_override(self, generate_nonce, generate_timestamp):
        """
        Content type should only be guessed if none is given.
        """
        generate_nonce.return_value = "abc"
        generate_timestamp.return_value = "1"
        oauth = requests_oauthlib.OAuth1("client_key")
        data = "a"
        r = requests.post("http://httpbin.org/get", data=data, auth=oauth)
        self.assertEqual(
            r.request.headers.get("Content-Type"), b"application/x-www-form-urlencoded"
        )
        r = requests.post(
            "http://httpbin.org/get",
            auth=oauth,
            data=data,
            headers={"Content-type": "application/json"},
        )
        self.assertEqual(r.request.headers.get("Content-Type"), b"application/json")

    def test_register_client_class(self, generate_timestamp, generate_nonce):
        class ClientSubclass(oauthlib.oauth1.Client):
            pass

        self.assertTrue(hasattr(requests_oauthlib.OAuth1, "client_class"))

        self.assertEqual(requests_oauthlib.OAuth1.client_class, oauthlib.oauth1.Client)

        normal = requests_oauthlib.OAuth1("client_key")

        self.assertIsInstance(normal.client, oauthlib.oauth1.Client)
        self.assertNotIsInstance(normal.client, ClientSubclass)

        requests_oauthlib.OAuth1.client_class = ClientSubclass

        self.assertEqual(requests_oauthlib.OAuth1.client_class, ClientSubclass)

        custom = requests_oauthlib.OAuth1("client_key")

        self.assertIsInstance(custom.client, oauthlib.oauth1.Client)
        self.assertIsInstance(custom.client, ClientSubclass)

        overridden = requests_oauthlib.OAuth1(
            "client_key", client_class=oauthlib.oauth1.Client
        )

        self.assertIsInstance(overridden.client, oauthlib.oauth1.Client)
        self.assertNotIsInstance(normal.client, ClientSubclass)
