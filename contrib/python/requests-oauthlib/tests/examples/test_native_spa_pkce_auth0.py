import os
import unittest

from . import base

class TestNativeAuth0Test(base.Sample, base.Browser, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.client_id = os.environ.get("AUTH0_PKCE_CLIENT_ID")
        self.idp_domain = os.environ.get("AUTH0_DOMAIN")

        if not self.client_id or not self.idp_domain:
            self.skipTest("native auth0 is not configured properly")

    def test_login(self):
        # redirect_uri is http://
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = "1"

        self.run_sample(
            "native_spa_pkce_auth0.py", {
                "OAUTH_CLIENT_ID": self.client_id,
                "OAUTH_IDP_DOMAIN": self.idp_domain,
            }
        )
        authorize_url = self.wait_for_pattern("https://")
        redirect_uri = self.authorize_auth0(authorize_url, "http://")
        self.write(redirect_uri)
        last_line = self.wait_for_end()

        import ast
        response = ast.literal_eval(last_line)
        self.assertIn("access_token", response)
        self.assertIn("id_token", response)
        self.assertIn("scope", response)
        self.assertIn("openid", response["scope"])
        self.assertIn("expires_in", response)
        self.assertIn("expires_at", response)
        self.assertIn("token_type", response)
        self.assertEqual("Bearer", response["token_type"])
