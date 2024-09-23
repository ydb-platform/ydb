import unittest

from oauthlib.oauth2 import WebApplicationClient, MobileApplicationClient
from oauthlib.oauth2 import LegacyApplicationClient, BackendApplicationClient
from requests import Request
from requests_oauthlib import OAuth2


class OAuth2AuthTest(unittest.TestCase):
    def setUp(self):
        self.token = {
            "token_type": "Bearer",
            "access_token": "asdfoiw37850234lkjsdfsdf",
            "expires_in": "3600",
        }
        self.client_id = "foo"
        self.clients = [
            WebApplicationClient(self.client_id),
            MobileApplicationClient(self.client_id),
            LegacyApplicationClient(self.client_id),
            BackendApplicationClient(self.client_id),
        ]

    def test_add_token_to_url(self):
        url = "https://example.com/resource?foo=bar"
        new_url = url + "&access_token=" + self.token["access_token"]
        for client in self.clients:
            client.default_token_placement = "query"
            auth = OAuth2(client=client, token=self.token)
            r = Request("GET", url, auth=auth).prepare()
            self.assertEqual(r.url, new_url)

    def test_add_token_to_headers(self):
        token = "Bearer " + self.token["access_token"]
        for client in self.clients:
            auth = OAuth2(client=client, token=self.token)
            r = Request("GET", "https://i.b", auth=auth).prepare()
            self.assertEqual(r.headers["Authorization"], token)

    def test_add_token_to_body(self):
        body = "foo=bar"
        new_body = body + "&access_token=" + self.token["access_token"]
        for client in self.clients:
            client.default_token_placement = "body"
            auth = OAuth2(client=client, token=self.token)
            r = Request("GET", "https://i.b", data=body, auth=auth).prepare()
            self.assertEqual(r.body, new_body)

    def test_add_nonexisting_token(self):
        for client in self.clients:
            auth = OAuth2(client=client)
            r = Request("GET", "https://i.b", auth=auth)
            self.assertRaises(ValueError, r.prepare)
