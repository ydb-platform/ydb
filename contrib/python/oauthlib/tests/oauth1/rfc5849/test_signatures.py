# -*- coding: utf-8 -*-
from oauthlib.oauth1.rfc5849.signature import (
    base_string_uri, collect_parameters, normalize_parameters,
    sign_hmac_sha1_with_client, sign_hmac_sha256_with_client,
    sign_hmac_sha512_with_client, sign_plaintext_with_client,
    sign_rsa_sha1_with_client, sign_rsa_sha256_with_client,
    sign_rsa_sha512_with_client, signature_base_string, verify_hmac_sha1,
    verify_hmac_sha256, verify_hmac_sha512, verify_plaintext, verify_rsa_sha1,
    verify_rsa_sha256, verify_rsa_sha512,
)

from tests.unittest import TestCase

# ################################################################

class MockRequest:
    """
    Mock of a request used by the verify_* functions.
    """

    def __init__(self,
                 method: str,
                 uri_str: str,
                 params: list,
                 signature: str):
        """
        The params is a list of (name, value) tuples. It is not a dictionary,
        because there can be multiple parameters with the same name.
        """
        self.uri = uri_str
        self.http_method = method
        self.params = params
        self.signature = signature


# ################################################################

class MockClient:
    """
    Mock of client credentials used by the sign_*_with_client functions.

    For HMAC, set the client_secret and resource_owner_secret.

    For RSA, set the rsa_key to either a PEM formatted PKCS #1 public key or
    PEM formatted PKCS #1 private key.
    """
    def __init__(self,
                 client_secret: str = None,
                 resource_owner_secret: str = None,
                 rsa_key: str = None):
        self.client_secret = client_secret
        self.resource_owner_secret = resource_owner_secret
        self.rsa_key = rsa_key  # used for private or public key: a poor design!


# ################################################################

class SignatureTests(TestCase):
    """
    Unit tests for the oauthlib/oauth1/rfc5849/signature.py module.

    The tests in this class are organised into sections, to test the
    functions relating to:

      - Signature base string calculation
      - HMAC-based signature methods
      - RSA-based signature methods
      - PLAINTEXT signature method

    Each section is separated by a comment beginning with "====".

    Those comments have been formatted to remain visible when the code is
    collapsed using PyCharm's code folding feature. That is, those section
    heading comments do not have any other comment lines around it, so they
    don't get collapsed when the contents of the class is collapsed. While
    there is a "Sequential comments" option in the code folding configuration,
    by default they are folded.

    They all use some/all of the example test vector, defined in the first
    section below.
    """

    # ==== Example test vector =======================================

    eg_signature_base_string =\
        'POST&http%3A%2F%2Fexample.com%2Frequest&a2%3Dr%2520b%26a3%3D2%2520q' \
        '%26a3%3Da%26b5%3D%253D%25253D%26c%2540%3D%26c2%3D%26oauth_consumer_' \
        'key%3D9djdj82h48djs9d2%26oauth_nonce%3D7d8f3e4a%26oauth_signature_m' \
        'ethod%3DHMAC-SHA1%26oauth_timestamp%3D137131201%26oauth_token%3Dkkk' \
        '9d7dh3k39sjv7'

    # The _signature base string_ above is copied from the end of
    # RFC 5849 section 3.4.1.1.
    #
    # It corresponds to the three values below.
    #
    # The _normalized parameters_ below is copied from the end of
    # RFC 5849 section 3.4.1.3.2.

    eg_http_method = 'POST'

    eg_base_string_uri = 'http://example.com/request'

    eg_normalized_parameters =\
        'a2=r%20b&a3=2%20q&a3=a&b5=%3D%253D&c%40=&c2=&oauth_consumer_key=9dj' \
        'dj82h48djs9d2&oauth_nonce=7d8f3e4a&oauth_signature_method=HMAC-SHA1' \
        '&oauth_timestamp=137131201&oauth_token=kkk9d7dh3k39sjv7'

    # The above _normalized parameters_ corresponds to the parameters below.
    #
    # The parameters below is copied from the table at the end of
    # RFC 5849 section 3.4.1.3.1.

    eg_params = [
        ('b5', '=%3D'),
        ('a3', 'a'),
        ('c@', ''),
        ('a2', 'r b'),
        ('oauth_consumer_key', '9djdj82h48djs9d2'),
        ('oauth_token', 'kkk9d7dh3k39sjv7'),
        ('oauth_signature_method', 'HMAC-SHA1'),
        ('oauth_timestamp', '137131201'),
        ('oauth_nonce', '7d8f3e4a'),
        ('c2', ''),
        ('a3', '2 q'),
    ]

    # The above parameters correspond to parameters from the three values below.
    #
    # These come from RFC 5849 section 3.4.1.3.1.

    eg_uri_query = 'b5=%3D%253D&a3=a&c%40=&a2=r%20b'

    eg_body = 'c2&a3=2+q'

    eg_authorization_header =\
        'OAuth realm="Example", oauth_consumer_key="9djdj82h48djs9d2",' \
        ' oauth_token="kkk9d7dh3k39sjv7", oauth_signature_method="HMAC-SHA1",' \
        ' oauth_timestamp="137131201", oauth_nonce="7d8f3e4a",' \
        ' oauth_signature="djosJKDKJSD8743243%2Fjdk33klY%3D"'

    # ==== Signature base string calculating function tests ==========

    def test_signature_base_string(self):
        """
        Test the ``signature_base_string`` function.
        """

        # Example from RFC 5849

        self.assertEqual(
            self.eg_signature_base_string,
            signature_base_string(
                self.eg_http_method,
                self.eg_base_string_uri,
                self.eg_normalized_parameters))

        # Test method is always uppercase in the signature base string

        for test_method in ['POST', 'Post', 'pOST', 'poST', 'posT', 'post']:
            self.assertEqual(
                self.eg_signature_base_string,
                signature_base_string(
                    test_method,
                    self.eg_base_string_uri,
                    self.eg_normalized_parameters))

    def test_base_string_uri(self):
        """
        Test the ``base_string_uri`` function.
        """

        # ----------------
        # Examples from the OAuth 1.0a specification: RFC 5849.

        # First example from RFC 5849 section 3.4.1.2.
        #
        #     GET /r%20v/X?id=123 HTTP/1.1
        #     Host: EXAMPLE.COM:80
        #
        # Note: there is a space between "r" and "v"

        self.assertEqual(
            'http://example.com/r%20v/X',
            base_string_uri('http://EXAMPLE.COM:80/r v/X?id=123'))

        # Second example from RFC 5849 section 3.4.1.2.
        #
        #     GET /?q=1 HTTP/1.1
        #     Host: www.example.net:8080

        self.assertEqual(
            'https://www.example.net:8080/',
            base_string_uri('https://www.example.net:8080/?q=1'))

        # ----------------
        # Scheme: will always be in lowercase

        for uri in [
            'foobar://www.example.com',
            'FOOBAR://www.example.com',
            'Foobar://www.example.com',
            'FooBar://www.example.com',
            'fOObAR://www.example.com',
        ]:
            self.assertEqual('foobar://www.example.com/', base_string_uri(uri))

        # ----------------
        # Host: will always be in lowercase

        for uri in [
            'http://www.example.com',
            'http://WWW.EXAMPLE.COM',
            'http://www.EXAMPLE.com',
            'http://wWW.eXAMPLE.cOM',
        ]:
            self.assertEqual('http://www.example.com/', base_string_uri(uri))

        # base_string_uri has an optional host parameter that can be used to
        # override the URI's netloc (or used as the host if there is no netloc)
        # The "netloc" refers to the "hostname[:port]" part of the URI.

        self.assertEqual(
            'http://actual.example.com/',
            base_string_uri('http://IGNORE.example.com', 'ACTUAL.example.com'))

        self.assertEqual(
            'http://override.example.com/path',
            base_string_uri('http:///path', 'OVERRIDE.example.com'))

        # ----------------
        # Host: valid host allows for IPv4 and IPv6

        self.assertEqual(
            'https://192.168.0.1/',
            base_string_uri('https://192.168.0.1')
        )
        self.assertEqual(
            'https://192.168.0.1:13000/',
            base_string_uri('https://192.168.0.1:13000')
        )
        self.assertEqual(
            'https://[123:db8:fd00:1000::5]:13000/',
            base_string_uri('https://[123:db8:fd00:1000::5]:13000')
        )
        self.assertEqual(
            'https://[123:db8:fd00:1000::5]/',
            base_string_uri('https://[123:db8:fd00:1000::5]')
        )

        # ----------------
        # Port: default ports always excluded; non-default ports always included

        self.assertEqual(
            "http://www.example.com/",
            base_string_uri("http://www.example.com:80/"))  # default port

        self.assertEqual(
            "https://www.example.com/",
            base_string_uri("https://www.example.com:443/"))  # default port

        self.assertEqual(
            "https://www.example.com:999/",
            base_string_uri("https://www.example.com:999/"))  # non-default port

        self.assertEqual(
            "http://www.example.com:443/",
            base_string_uri("HTTP://www.example.com:443/"))  # non-default port

        self.assertEqual(
            "https://www.example.com:80/",
            base_string_uri("HTTPS://www.example.com:80/"))  # non-default port

        self.assertEqual(
            "http://www.example.com/",
            base_string_uri("http://www.example.com:/"))  # colon but no number

        # ----------------
        # Paths

        self.assertEqual(
            'http://www.example.com/',
            base_string_uri('http://www.example.com'))  # no slash

        self.assertEqual(
            'http://www.example.com/',
            base_string_uri('http://www.example.com/'))  # with slash

        self.assertEqual(
            'http://www.example.com:8080/',
            base_string_uri('http://www.example.com:8080'))  # no slash

        self.assertEqual(
            'http://www.example.com:8080/',
            base_string_uri('http://www.example.com:8080/'))  # with slash

        self.assertEqual(
            'http://www.example.com/foo/bar',
            base_string_uri('http://www.example.com/foo/bar'))  # no slash
        self.assertEqual(
            'http://www.example.com/foo/bar/',
            base_string_uri('http://www.example.com/foo/bar/'))  # with slash

        # ----------------
        # Query parameters & fragment IDs do not appear in the base string URI

        self.assertEqual(
            'https://www.example.com/path',
            base_string_uri('https://www.example.com/path?foo=bar'))

        self.assertEqual(
            'https://www.example.com/path',
            base_string_uri('https://www.example.com/path#fragment'))

        # ----------------
        # Percent encoding
        #
        # RFC 5849 does not specify what characters are percent encoded, but in
        # one of its examples it shows spaces being percent encoded.
        # So it is assumed that spaces must be encoded, but we don't know what
        # other characters are encoded or not.

        self.assertEqual(
            'https://www.example.com/hello%20world',
            base_string_uri('https://www.example.com/hello world'))

        self.assertEqual(
            'https://www.hello%20world.com/',
            base_string_uri('https://www.hello world.com/'))

        # ----------------
        # Errors detected

        # base_string_uri expects a string
        self.assertRaises(ValueError, base_string_uri, None)
        self.assertRaises(ValueError, base_string_uri, 42)
        self.assertRaises(ValueError, base_string_uri, b'http://example.com')

        # Missing scheme is an error
        self.assertRaises(ValueError, base_string_uri, '')
        self.assertRaises(ValueError, base_string_uri, ' ')  # single space
        self.assertRaises(ValueError, base_string_uri, 'http')
        self.assertRaises(ValueError, base_string_uri, 'example.com')

        # Missing host is an error
        self.assertRaises(ValueError, base_string_uri, 'http:')
        self.assertRaises(ValueError, base_string_uri, 'http://')
        self.assertRaises(ValueError, base_string_uri, 'http://:8080')

        # Port is not a valid TCP/IP port number
        self.assertRaises(ValueError, base_string_uri, 'http://eg.com:0')
        self.assertRaises(ValueError, base_string_uri, 'http://eg.com:-1')
        self.assertRaises(ValueError, base_string_uri, 'http://eg.com:65536')
        self.assertRaises(ValueError, base_string_uri, 'http://eg.com:3.14')
        self.assertRaises(ValueError, base_string_uri, 'http://eg.com:BAD')
        self.assertRaises(ValueError, base_string_uri, 'http://eg.com:NaN')
        self.assertRaises(ValueError, base_string_uri, 'http://eg.com: ')
        self.assertRaises(ValueError, base_string_uri, 'http://eg.com:42:42')

    def test_collect_parameters(self):
        """
        Test the ``collect_parameters`` function.
        """

        # ----------------
        # Examples from the OAuth 1.0a specification: RFC 5849.

        params = collect_parameters(
            self.eg_uri_query,
            self.eg_body,
            {'Authorization': self.eg_authorization_header})

        # Check params contains the same pairs as control_params, ignoring order
        self.assertEqual(sorted(self.eg_params), sorted(params))

        # ----------------
        # Examples with no parameters

        self.assertEqual([], collect_parameters('', '', {}))

        self.assertEqual([], collect_parameters(None, None, None))

        self.assertEqual([], collect_parameters())

        self.assertEqual([], collect_parameters(headers={'foo': 'bar'}))

        # ----------------
        # Test effect of exclude_oauth_signature"

        no_sig = collect_parameters(
            headers={'authorization': self.eg_authorization_header})
        with_sig = collect_parameters(
            headers={'authorization': self.eg_authorization_header},
            exclude_oauth_signature=False)

        self.assertEqual(sorted(no_sig + [('oauth_signature',
                                           'djosJKDKJSD8743243/jdk33klY=')]),
                         sorted(with_sig))

        # ----------------
        # Test effect of "with_realm" as well as header name case insensitivity

        no_realm = collect_parameters(
                headers={'authorization': self.eg_authorization_header},
                with_realm=False)
        with_realm = collect_parameters(
                headers={'AUTHORIZATION': self.eg_authorization_header},
                with_realm=True)

        self.assertEqual(sorted(no_realm + [('realm', 'Example')]),
                         sorted(with_realm))

    def test_normalize_parameters(self):
        """
        Test the ``normalize_parameters`` function.
        """

        # headers = {'Authorization': self.authorization_header}
        # parameters = collect_parameters(
        #     uri_query=self.uri_query, body=self.body, headers=headers)
        # normalized = normalize_parameters(parameters)
        #
        # # Unicode everywhere and always
        # self.assertIsInstance(normalized, str)
        #
        # # Lets see if things are in order
        # # check to see that querystring keys come in alphanumeric order:
        # querystring_keys = ['a2', 'a3', 'b5', 'oauth_consumer_key',
        #                     'oauth_nonce', 'oauth_signature_method',
        #                     'oauth_timestamp', 'oauth_token']
        # index = -1  # start at -1 because the 'a2' key starts at index 0
        # for key in querystring_keys:
        #     self.assertGreater(normalized.index(key), index)
        #     index = normalized.index(key)

        # ----------------
        # Example from the OAuth 1.0a specification: RFC 5849.
        # Params from end of section 3.4.1.3.1. and the expected
        # normalized parameters from the end of section 3.4.1.3.2.

        self.assertEqual(self.eg_normalized_parameters,
                         normalize_parameters(self.eg_params))

    # ==== HMAC-based signature method tests =========================

    hmac_client = MockClient(
        client_secret='ECrDNoq1VYzzzzzzzzzyAK7TwZNtPnkqatqZZZZ',
        resource_owner_secret='just-a-string    asdasd')

    # The following expected signatures were calculated by putting the value of
    # the eg_signature_base_string in a file ("base-str.txt") and running:
    #
    # echo -n `cat base-str.txt` | openssl dgst -hmac KEY -sha1 -binary| base64
    #
    # Where the KEY is the concatenation of the client_secret, an ampersand and
    # the resource_owner_secret. But those values need to be encoded properly,
    # so the spaces in the resource_owner_secret must be represented as '%20'.
    #
    # Note: the "echo -n" is needed to remove the last newline character, which
    # most text editors will add.

    expected_signature_hmac_sha1 = \
        'wsdNmjGB7lvis0UJuPAmjvX/PXw='

    expected_signature_hmac_sha256 = \
        'wdfdHUKXHbOnOGZP8WFAWMSAmWzN3EVBWWgXGlC/Eo4='

    expected_signature_hmac_sha512 = \
        'u/vlyZFDxOWOZ9UUXwRBJHvq8/T4jCA74ocRmn2ECnjUBTAeJiZIRU8hDTjS88Tz' \
        '1fGONffMpdZxUkUTW3k1kg=='

    def test_sign_hmac_sha1_with_client(self):
        """
        Test sign and verify with HMAC-SHA1.
        """
        self.assertEqual(
            self.expected_signature_hmac_sha1,
            sign_hmac_sha1_with_client(self.eg_signature_base_string,
                                       self.hmac_client))
        self.assertTrue(verify_hmac_sha1(
            MockRequest('POST',
                        'http://example.com/request',
                        self.eg_params,
                        self.expected_signature_hmac_sha1),
            self.hmac_client.client_secret,
            self.hmac_client.resource_owner_secret))

    def test_sign_hmac_sha256_with_client(self):
        """
        Test sign and verify with HMAC-SHA256.
        """
        self.assertEqual(
            self.expected_signature_hmac_sha256,
            sign_hmac_sha256_with_client(self.eg_signature_base_string,
                                         self.hmac_client))
        self.assertTrue(verify_hmac_sha256(
            MockRequest('POST',
                        'http://example.com/request',
                        self.eg_params,
                        self.expected_signature_hmac_sha256),
            self.hmac_client.client_secret,
            self.hmac_client.resource_owner_secret))

    def test_sign_hmac_sha512_with_client(self):
        """
        Test sign and verify with HMAC-SHA512.
        """
        self.assertEqual(
            self.expected_signature_hmac_sha512,
            sign_hmac_sha512_with_client(self.eg_signature_base_string,
                                         self.hmac_client))
        self.assertTrue(verify_hmac_sha512(
            MockRequest('POST',
                        'http://example.com/request',
                        self.eg_params,
                        self.expected_signature_hmac_sha512),
            self.hmac_client.client_secret,
            self.hmac_client.resource_owner_secret))

    def test_hmac_false_positives(self):
        """
        Test verify_hmac-* functions will correctly detect invalid signatures.
        """

        _ros = self.hmac_client.resource_owner_secret

        for functions in [
            (sign_hmac_sha1_with_client, verify_hmac_sha1),
            (sign_hmac_sha256_with_client, verify_hmac_sha256),
            (sign_hmac_sha512_with_client, verify_hmac_sha512),
        ]:
            signing_function = functions[0]
            verify_function = functions[1]

            good_signature = \
                signing_function(
                    self.eg_signature_base_string,
                    self.hmac_client)

            bad_signature_on_different_value = \
                signing_function(
                    'not the signature base string',
                    self.hmac_client)

            bad_signature_produced_by_different_client_secret = \
                signing_function(
                    self.eg_signature_base_string,
                    MockClient(client_secret='wrong-secret',
                               resource_owner_secret=_ros))
            bad_signature_produced_by_different_resource_owner_secret = \
                signing_function(
                    self.eg_signature_base_string,
                    MockClient(client_secret=self.hmac_client.client_secret,
                               resource_owner_secret='wrong-secret'))

            bad_signature_produced_with_no_resource_owner_secret = \
                signing_function(
                    self.eg_signature_base_string,
                    MockClient(client_secret=self.hmac_client.client_secret))
            bad_signature_produced_with_no_client_secret = \
                signing_function(
                    self.eg_signature_base_string,
                    MockClient(resource_owner_secret=_ros))

            self.assertTrue(verify_function(
                MockRequest('POST',
                            'http://example.com/request',
                            self.eg_params,
                            good_signature),
                self.hmac_client.client_secret,
                self.hmac_client.resource_owner_secret))

            for bad_signature in [
                '',
                'ZG9uJ3QgdHJ1c3QgbWUK',  # random base64 encoded value
                'altérer',  # value with a non-ASCII character in it
                bad_signature_on_different_value,
                bad_signature_produced_by_different_client_secret,
                bad_signature_produced_by_different_resource_owner_secret,
                bad_signature_produced_with_no_resource_owner_secret,
                bad_signature_produced_with_no_client_secret,
            ]:
                self.assertFalse(verify_function(
                    MockRequest('POST',
                                'http://example.com/request',
                                self.eg_params,
                                bad_signature),
                    self.hmac_client.client_secret,
                    self.hmac_client.resource_owner_secret))

    # ==== RSA-based signature methods tests =========================

    rsa_private_client = MockClient(rsa_key='''
-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQDk1/bxyS8Q8jiheHeYYp/4rEKJopeQRRKKpZI4s5i+UPwVpupG
AlwXWfzXwSMaKPAoKJNdu7tqKRniqst5uoHXw98gj0x7zamu0Ck1LtQ4c7pFMVah
5IYGhBi2E9ycNS329W27nJPWNCbESTu7snVlG8V8mfvGGg3xNjTMO7IdrwIDAQAB
AoGBAOQ2KuH8S5+OrsL4K+wfjoCi6MfxCUyqVU9GxocdM1m30WyWRFMEz2nKJ8fR
p3vTD4w8yplTOhcoXdQZl0kRoaDzrcYkm2VvJtQRrX7dKFT8dR8D/Tr7dNQLOXfC
DY6xveQczE7qt7Vk7lp4FqmxBsaaEuokt78pOOjywZoInjZhAkEA9wz3zoZNT0/i
rf6qv2qTIeieUB035N3dyw6f1BGSWYaXSuerDCD/J1qZbAPKKhyHZbVawFt3UMhe
542UftBaxQJBAO0iJy1I8GQjGnS7B3yvyH3CcLYGy296+XO/2xKp/d/ty1OIeovx
C60pLNwuFNF3z9d2GVQAdoQ89hUkOtjZLeMCQQD0JO6oPHUeUjYT+T7ImAv7UKVT
Suy30sKjLzqoGw1kR+wv7C5PeDRvscs4wa4CW9s6mjSrMDkDrmCLuJDtmf55AkEA
kmaMg2PNrjUR51F0zOEFycaaqXbGcFwe1/xx9zLmHzMDXd4bsnwt9kk+fe0hQzVS
JzatanQit3+feev1PN3QewJAWv4RZeavEUhKv+kLe95Yd0su7lTLVduVgh4v5yLT
Ga6FHdjGPcfajt+nrpB1n8UQBEH9ZxniokR/IPvdMlxqXA==
-----END RSA PRIVATE KEY-----
''')

    rsa_public_client = MockClient(rsa_key='''
-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAOTX9vHJLxDyOKF4d5hin/isQomil5BFEoqlkjizmL5Q/BWm6kYCXBdZ
/NfBIxoo8Cgok127u2opGeKqy3m6gdfD3yCPTHvNqa7QKTUu1DhzukUxVqHkhgaE
GLYT3Jw1Lfb1bbuck9Y0JsRJO7uydWUbxXyZ+8YaDfE2NMw7sh2vAgMBAAE=
-----END RSA PUBLIC KEY-----
''')

    # The above private key was generated using:
    #     $ openssl genrsa -out example.pvt 1024
    #     $ chmod 600 example.pvt
    # Public key was extract from it using:
    #     $ ssh-keygen -e -m pem -f example.pvt
    # PEM encoding requires the key to be concatenated with linebreaks.

    # The following expected signatures were calculated by putting the private
    # key in a file (test.pvt) and the value of sig_base_str_rsa in another file
    # ("base-str.txt") and running:
    #
    # echo -n `cat base-str.txt` | openssl dgst -sha1 -sign test.pvt| base64
    #
    # Note: the "echo -n" is needed to remove the last newline character, which
    # most text editors will add.

    expected_signature_rsa_sha1 = \
        'mFY2KOEnlYWsTvUA+5kxuBIcvBYXu+ljw9ttVJQxKduMueGSVPCB1tK1PlqVLK738' \
        'HK0t19ecBJfb6rMxUwrriw+MlBO+jpojkZIWccw1J4cAb4qu4M81DbpUAq4j/1w/Q' \
        'yTR4TWCODlEfN7Zfgy8+pf+TjiXfIwRC1jEWbuL1E='

    expected_signature_rsa_sha256 = \
        'jqKl6m0WS69tiVJV8ZQ6aQEfJqISoZkiPBXRv6Al2+iFSaDpfeXjYm+Hbx6m1azR' \
        'drZ/35PM3cvuid3LwW/siAkzb0xQcGnTyAPH8YcGWzmnKGY7LsB7fkqThchNxvRK' \
        '/N7s9M1WMnfZZ+1dQbbwtTs1TG1+iexUcV7r3M7Heec='

    expected_signature_rsa_sha512 = \
        'jL1CnjlsNd25qoZVHZ2oJft47IRYTjpF5CvCUjL3LY0NTnbEeVhE4amWXUFBe9GL' \
        'DWdUh/79ZWNOrCirBFIP26cHLApjYdt4ZG7EVK0/GubS2v8wT1QPRsog8zyiMZkm' \
        'g4JXdWCGXG8YRvRJTg+QKhXuXwS6TcMNakrgzgFIVhA='

    def test_sign_rsa_sha1_with_client(self):
        """
        Test sign and verify with RSA-SHA1.
        """
        self.assertEqual(
            self.expected_signature_rsa_sha1,
            sign_rsa_sha1_with_client(self.eg_signature_base_string,
                                      self.rsa_private_client))
        self.assertTrue(verify_rsa_sha1(
            MockRequest('POST',
                        'http://example.com/request',
                        self.eg_params,
                        self.expected_signature_rsa_sha1),
            self.rsa_public_client.rsa_key))

    def test_sign_rsa_sha256_with_client(self):
        """
        Test sign and verify with RSA-SHA256.
        """
        self.assertEqual(
            self.expected_signature_rsa_sha256,
            sign_rsa_sha256_with_client(self.eg_signature_base_string,
                                        self.rsa_private_client))
        self.assertTrue(verify_rsa_sha256(
            MockRequest('POST',
                        'http://example.com/request',
                        self.eg_params,
                        self.expected_signature_rsa_sha256),
            self.rsa_public_client.rsa_key))

    def test_sign_rsa_sha512_with_client(self):
        """
        Test sign and verify with RSA-SHA512.
        """
        self.assertEqual(
            self.expected_signature_rsa_sha512,
            sign_rsa_sha512_with_client(self.eg_signature_base_string,
                                        self.rsa_private_client))
        self.assertTrue(verify_rsa_sha512(
            MockRequest('POST',
                        'http://example.com/request',
                        self.eg_params,
                        self.expected_signature_rsa_sha512),
            self.rsa_public_client.rsa_key))

    def test_rsa_false_positives(self):
        """
        Test verify_rsa-* functions will correctly detect invalid signatures.
        """

        another_client = MockClient(rsa_key='''
-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQDZcD/1OZNJJ6Y3QZM16Z+O7fkD9kTIQuT2BfpAOUvDfxzYhVC9
TNmSDHCQhr+ClutyolBk5jTE1/FXFUuHoPsTrkI7KQFXPP834D4gnSY9jrAiUJHe
DVF6wXNuS7H4Ueh16YPjUxgLLRh/nn/JSEj98gsw+7DP01OWMfWS99S7eQIDAQAB
AoGBALsQZRXVyK7BG7CiC8HwEcNnXDpaXmZjlpNKJTenk1THQMvONd4GBZAuf5D3
PD9fE4R1u/ByVKecmBaxTV+L0TRQfD8K/nbQe0SKRQIkLI2ymLJKC/eyw5iTKT0E
+BS6wYpVd+mfcqgvpHOYpUmz9X8k/eOa7uslFmvt+sDb5ZcBAkEA+++SRqqUxFEG
s/ZWAKw9p5YgkeVUOYVUwyAeZ97heySrjVzg1nZ6v6kv7iOPi9KOEpaIGPW7x1K/
uQuSt4YEqQJBANzyNqZTTPpv7b/R8ABFy0YMwPVNt3b1GOU1Xxl6iuhH2WcHuueo
UB13JHoZCMZ7hsEqieEz6uteUjdRzRPKclECQFNhVK4iop3emzNQYeJTHwyp+RmQ
JrHq2MTDioyiDUouNsDQbnFMQQ/RtNVB265Q/0hTnbN1ELLFRkK9+87VghECQQC9
hacLFPk6+TffCp3sHfI3rEj4Iin1iFhKhHWGzW7JwJfjoOXaQK44GDLZ6Q918g+t
MmgDHR2tt8KeYTSgfU+BAkBcaVF91EQ7VXhvyABNYjeYP7lU7orOgdWMa/zbLXSU
4vLsK1WOmwPY9zsXpPkilqszqcru4gzlG462cSbEdAW9
-----END RSA PRIVATE KEY-----
''')

        for functions in [
            (sign_rsa_sha1_with_client, verify_rsa_sha1),
            (sign_rsa_sha256_with_client, verify_rsa_sha256),
            (sign_rsa_sha512_with_client, verify_rsa_sha512),
        ]:
            signing_function = functions[0]
            verify_function = functions[1]

            good_signature = \
                signing_function(self.eg_signature_base_string,
                                 self.rsa_private_client)

            bad_signature_on_different_value = \
                signing_function('wrong value signed', self.rsa_private_client)

            bad_signature_produced_by_different_private_key = \
                signing_function(self.eg_signature_base_string, another_client)

            self.assertTrue(verify_function(
                MockRequest('POST',
                            'http://example.com/request',
                            self.eg_params,
                            good_signature),
                self.rsa_public_client.rsa_key))

            for bad_signature in [
                '',
                'ZG9uJ3QgdHJ1c3QgbWUK',  # random base64 encoded value
                'altérer',  # value with a non-ASCII character in it
                bad_signature_on_different_value,
                bad_signature_produced_by_different_private_key,
            ]:
                self.assertFalse(verify_function(
                    MockRequest('POST',
                                'http://example.com/request',
                                self.eg_params,
                                bad_signature),
                    self.rsa_public_client.rsa_key))

    def test_rsa_bad_keys(self):
        """
        Testing RSA sign and verify with bad key values produces errors.

        This test is useful for coverage tests, since it runs the code branches
        that deal with error situations.
        """

        # Signing needs a private key

        for bad_value in [None, '', 'foobar']:
            self.assertRaises(ValueError,
                              sign_rsa_sha1_with_client,
                              self.eg_signature_base_string,
                              MockClient(rsa_key=bad_value))

        self.assertRaises(AttributeError,
                          sign_rsa_sha1_with_client,
                          self.eg_signature_base_string,
                          self.rsa_public_client)  # public key doesn't sign

        # Verify needs a public key

        for bad_value in [None, '', 'foobar', self.rsa_private_client.rsa_key]:
            self.assertRaises(TypeError,
                              verify_rsa_sha1,
                              MockRequest('POST',
                                          'http://example.com/request',
                                          self.eg_params,
                                          self.expected_signature_rsa_sha1),
                              MockClient(rsa_key=bad_value))

        # For completeness, this text could repeat the above for RSA-SHA256 and
        # RSA-SHA512 signing and verification functions.

    def test_rsa_jwt_algorithm_cache(self):
        # Tests cache of RSAAlgorithm objects is implemented correctly.

        # This is difficult to test, since the cache is internal.
        #
        # Running this test with coverage will show the cache-hit branch of code
        # being executed by two signing operations with the same hash algorithm.

        self.test_sign_rsa_sha1_with_client()  # creates cache entry
        self.test_sign_rsa_sha1_with_client()  # reuses cache entry

        # Some possible bugs will be detected if multiple signing operations
        # with different hash algorithms produce the wrong results (e.g. if the
        # cache incorrectly returned the previously used algorithm, instead
        # of the one that is needed).

        self.test_sign_rsa_sha256_with_client()
        self.test_sign_rsa_sha256_with_client()
        self.test_sign_rsa_sha1_with_client()
        self.test_sign_rsa_sha256_with_client()
        self.test_sign_rsa_sha512_with_client()

    # ==== PLAINTEXT signature method tests ==========================

    plaintext_client = hmac_client  # for convenience, use the same HMAC secrets

    expected_signature_plaintext = (
        'ECrDNoq1VYzzzzzzzzzyAK7TwZNtPnkqatqZZZZ'
        '&'
        'just-a-string%20%20%20%20asdasd')

    def test_sign_plaintext_with_client(self):
        # With PLAINTEXT, the "signature" is always the same: regardless of the
        # contents of the request. It is the concatenation of the encoded
        # client_secret, an ampersand, and the encoded resource_owner_secret.
        #
        # That is why the spaces in the resource owner secret are "%20".

        self.assertEqual(self.expected_signature_plaintext,
                         sign_plaintext_with_client(None,  # request is ignored
                                                    self.plaintext_client))
        self.assertTrue(verify_plaintext(
            MockRequest('PUT',
                        'http://example.com/some-other-path',
                        [('description', 'request is ignored in PLAINTEXT')],
                        self.expected_signature_plaintext),
            self.plaintext_client.client_secret,
            self.plaintext_client.resource_owner_secret))

    def test_plaintext_false_positives(self):
        """
        Test verify_plaintext function will correctly detect invalid signatures.
        """

        _ros = self.plaintext_client.resource_owner_secret

        good_signature = \
            sign_plaintext_with_client(
                self.eg_signature_base_string,
                self.plaintext_client)

        bad_signature_produced_by_different_client_secret = \
            sign_plaintext_with_client(
                self.eg_signature_base_string,
                MockClient(client_secret='wrong-secret',
                           resource_owner_secret=_ros))
        bad_signature_produced_by_different_resource_owner_secret = \
            sign_plaintext_with_client(
                self.eg_signature_base_string,
                MockClient(client_secret=self.plaintext_client.client_secret,
                           resource_owner_secret='wrong-secret'))

        bad_signature_produced_with_no_resource_owner_secret = \
            sign_plaintext_with_client(
                self.eg_signature_base_string,
                MockClient(client_secret=self.plaintext_client.client_secret))
        bad_signature_produced_with_no_client_secret = \
            sign_plaintext_with_client(
                self.eg_signature_base_string,
                MockClient(resource_owner_secret=_ros))

        self.assertTrue(verify_plaintext(
            MockRequest('POST',
                        'http://example.com/request',
                        self.eg_params,
                        good_signature),
            self.plaintext_client.client_secret,
            self.plaintext_client.resource_owner_secret))

        for bad_signature in [
            '',
            'ZG9uJ3QgdHJ1c3QgbWUK',  # random base64 encoded value
            'altérer',  # value with a non-ASCII character in it
            bad_signature_produced_by_different_client_secret,
            bad_signature_produced_by_different_resource_owner_secret,
            bad_signature_produced_with_no_resource_owner_secret,
            bad_signature_produced_with_no_client_secret,
        ]:
            self.assertFalse(verify_plaintext(
                MockRequest('POST',
                            'http://example.com/request',
                            self.eg_params,
                            bad_signature),
                self.plaintext_client.client_secret,
                self.plaintext_client.resource_owner_secret))
