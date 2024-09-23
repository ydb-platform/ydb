from unittest import TestCase

import requests
import requests_mock
import time

from urllib.parse import urlparse, parse_qs

from oauthlib.oauth2.rfc6749.errors import InvalidGrantError
from requests_oauthlib import OAuth2Session
from requests_oauthlib.compliance_fixes import facebook_compliance_fix
from requests_oauthlib.compliance_fixes import fitbit_compliance_fix
from requests_oauthlib.compliance_fixes import mailchimp_compliance_fix
from requests_oauthlib.compliance_fixes import weibo_compliance_fix
from requests_oauthlib.compliance_fixes import slack_compliance_fix
from requests_oauthlib.compliance_fixes import instagram_compliance_fix
from requests_oauthlib.compliance_fixes import plentymarkets_compliance_fix
from requests_oauthlib.compliance_fixes import ebay_compliance_fix


class FacebookComplianceFixTest(TestCase):
    def setUp(self):
        mocker = requests_mock.Mocker()
        mocker.post(
            "https://graph.facebook.com/oauth/access_token",
            text="access_token=urlencoded",
            headers={"Content-Type": "text/plain"},
        )
        mocker.start()
        self.addCleanup(mocker.stop)

        facebook = OAuth2Session("someclientid", redirect_uri="https://i.b")
        self.session = facebook_compliance_fix(facebook)

    def test_fetch_access_token(self):
        token = self.session.fetch_token(
            "https://graph.facebook.com/oauth/access_token",
            client_secret="someclientsecret",
            authorization_response="https://i.b/?code=hello",
        )
        self.assertEqual(token, {"access_token": "urlencoded", "token_type": "Bearer"})


class FitbitComplianceFixTest(TestCase):
    def setUp(self):
        self.mocker = requests_mock.Mocker()
        self.mocker.post(
            "https://api.fitbit.com/oauth2/token",
            json={"errors": [{"errorType": "invalid_grant"}]},
        )
        self.mocker.start()
        self.addCleanup(self.mocker.stop)

        fitbit = OAuth2Session("someclientid", redirect_uri="https://i.b")
        self.session = fitbit_compliance_fix(fitbit)

    def test_fetch_access_token(self):
        self.assertRaises(
            InvalidGrantError,
            self.session.fetch_token,
            "https://api.fitbit.com/oauth2/token",
            client_secret="someclientsecret",
            authorization_response="https://i.b/?code=hello",
        )

        self.mocker.post(
            "https://api.fitbit.com/oauth2/token", json={"access_token": "fitbit"}
        )

        token = self.session.fetch_token(
            "https://api.fitbit.com/oauth2/token", client_secret="good"
        )

        self.assertEqual(token, {"access_token": "fitbit"})

    def test_refresh_token(self):
        self.assertRaises(
            InvalidGrantError,
            self.session.refresh_token,
            "https://api.fitbit.com/oauth2/token",
            auth=requests.auth.HTTPBasicAuth("someclientid", "someclientsecret"),
        )

        self.mocker.post(
            "https://api.fitbit.com/oauth2/token",
            json={"access_token": "access", "refresh_token": "refresh"},
        )

        token = self.session.refresh_token(
            "https://api.fitbit.com/oauth2/token",
            auth=requests.auth.HTTPBasicAuth("someclientid", "someclientsecret"),
        )

        self.assertEqual(token["access_token"], "access")
        self.assertEqual(token["refresh_token"], "refresh")


class MailChimpComplianceFixTest(TestCase):
    def setUp(self):
        mocker = requests_mock.Mocker()
        mocker.post(
            "https://login.mailchimp.com/oauth2/token",
            json={"access_token": "mailchimp", "expires_in": 0, "scope": None},
        )
        mocker.start()
        self.addCleanup(mocker.stop)

        mailchimp = OAuth2Session("someclientid", redirect_uri="https://i.b")
        self.session = mailchimp_compliance_fix(mailchimp)

    def test_fetch_access_token(self):
        token = self.session.fetch_token(
            "https://login.mailchimp.com/oauth2/token",
            client_secret="someclientsecret",
            authorization_response="https://i.b/?code=hello",
        )
        # Times should be close
        approx_expires_at = time.time() + 3600
        actual_expires_at = token.pop("expires_at")
        self.assertAlmostEqual(actual_expires_at, approx_expires_at, places=2)

        # Other token values exact
        self.assertEqual(token, {"access_token": "mailchimp", "expires_in": 3600})

        # And no scope at all
        self.assertNotIn("scope", token)


class WeiboComplianceFixTest(TestCase):
    def setUp(self):
        mocker = requests_mock.Mocker()
        mocker.post(
            "https://api.weibo.com/oauth2/access_token", json={"access_token": "weibo"}
        )
        mocker.start()
        self.addCleanup(mocker.stop)

        weibo = OAuth2Session("someclientid", redirect_uri="https://i.b")
        self.session = weibo_compliance_fix(weibo)

    def test_fetch_access_token(self):
        token = self.session.fetch_token(
            "https://api.weibo.com/oauth2/access_token",
            client_secret="someclientsecret",
            authorization_response="https://i.b/?code=hello",
        )
        self.assertEqual(token, {"access_token": "weibo", "token_type": "Bearer"})


class SlackComplianceFixTest(TestCase):
    def setUp(self):
        mocker = requests_mock.Mocker()
        mocker.post(
            "https://slack.com/api/oauth.access",
            json={"access_token": "xoxt-23984754863-2348975623103", "scope": "read"},
        )
        for method in ("GET", "POST"):
            mocker.request(
                method=method,
                url="https://slack.com/api/auth.test",
                json={
                    "ok": True,
                    "url": "https://myteam.slack.com/",
                    "team": "My Team",
                    "user": "cal",
                    "team_id": "T12345",
                    "user_id": "U12345",
                },
            )
        mocker.start()
        self.addCleanup(mocker.stop)

        slack = OAuth2Session("someclientid", redirect_uri="https://i.b")
        self.session = slack_compliance_fix(slack)

    def test_protected_request(self):
        self.session.token = {"access_token": "dummy-access-token"}
        response = self.session.get("https://slack.com/api/auth.test")
        url = response.request.url
        query = parse_qs(urlparse(url).query)
        self.assertNotIn("token", query)
        body = response.request.body
        data = parse_qs(body)
        self.assertEqual(data["token"], ["dummy-access-token"])

    def test_protected_request_override_token_get(self):
        self.session.token = {"access_token": "dummy-access-token"}
        response = self.session.get(
            "https://slack.com/api/auth.test", data={"token": "different-token"}
        )
        url = response.request.url
        query = parse_qs(urlparse(url).query)
        self.assertNotIn("token", query)
        body = response.request.body
        data = parse_qs(body)
        self.assertEqual(data["token"], ["different-token"])

    def test_protected_request_override_token_post(self):
        self.session.token = {"access_token": "dummy-access-token"}
        response = self.session.post(
            "https://slack.com/api/auth.test", data={"token": "different-token"}
        )
        url = response.request.url
        query = parse_qs(urlparse(url).query)
        self.assertNotIn("token", query)
        body = response.request.body
        data = parse_qs(body)
        self.assertEqual(data["token"], ["different-token"])

    def test_protected_request_override_token_url(self):
        self.session.token = {"access_token": "dummy-access-token"}
        response = self.session.get(
            "https://slack.com/api/auth.test?token=different-token"
        )
        url = response.request.url
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["token"], ["different-token"])
        self.assertIsNone(response.request.body)


class InstagramComplianceFixTest(TestCase):
    def setUp(self):
        mocker = requests_mock.Mocker()
        mocker.request(
            method="GET",
            url="https://api.instagram.com/v1/users/self",
            json={
                "data": {
                    "id": "1574083",
                    "username": "snoopdogg",
                    "full_name": "Snoop Dogg",
                    "profile_picture": "http://distillery.s3.amazonaws.com/profiles/profile_1574083_75sq_1295469061.jpg",
                    "bio": "This is my bio",
                    "website": "http://snoopdogg.com",
                    "is_business": False,
                    "counts": {"media": 1320, "follows": 420, "followed_by": 3410},
                }
            },
        )
        mocker.start()
        self.addCleanup(mocker.stop)

        instagram = OAuth2Session("someclientid", redirect_uri="https://i.b")
        self.session = instagram_compliance_fix(instagram)

    def test_protected_request(self):
        self.session.token = {"access_token": "dummy-access-token"}
        response = self.session.get("https://api.instagram.com/v1/users/self")
        url = response.request.url
        query = parse_qs(urlparse(url).query)
        self.assertIn("access_token", query)
        self.assertEqual(query["access_token"], ["dummy-access-token"])

    def test_protected_request_dont_override(self):
        """check that if the access_token param
        already exist we don't override it"""
        self.session.token = {"access_token": "dummy-access-token"}
        response = self.session.get(
            "https://api.instagram.com/v1/users/self?access_token=correct-access-token"
        )
        url = response.request.url
        query = parse_qs(urlparse(url).query)
        self.assertIn("access_token", query)
        self.assertEqual(query["access_token"], ["correct-access-token"])


class PlentymarketsComplianceFixTest(TestCase):
    def setUp(self):
        mocker = requests_mock.Mocker()
        mocker.post(
            "https://shop.plentymarkets-cloud02.com",
            json={
                "accessToken": "ecUN1r8KhJewMCdLAmpHOdZ4O0ofXKB9zf6CXK61",
                "tokenType": "Bearer",
                "expiresIn": 86400,
                "refreshToken": "iG2kBGIjcXaRE4xmTVUnv7xwxX7XMcWCHqJmFaSX",
            },
            headers={"Content-Type": "application/json"},
        )
        mocker.start()
        self.addCleanup(mocker.stop)

        plentymarkets = OAuth2Session("someclientid", redirect_uri="https://i.b")
        self.session = plentymarkets_compliance_fix(plentymarkets)

    def test_fetch_access_token(self):
        token = self.session.fetch_token(
            "https://shop.plentymarkets-cloud02.com",
            authorization_response="https://i.b/?code=hello",
        )

        approx_expires_at = time.time() + 86400
        actual_expires_at = token.pop("expires_at")
        self.assertAlmostEqual(actual_expires_at, approx_expires_at, places=2)

        self.assertEqual(
            token,
            {
                "access_token": "ecUN1r8KhJewMCdLAmpHOdZ4O0ofXKB9zf6CXK61",
                "expires_in": 86400,
                "token_type": "Bearer",
                "refresh_token": "iG2kBGIjcXaRE4xmTVUnv7xwxX7XMcWCHqJmFaSX",
            },
        )


class EbayComplianceFixTest(TestCase):
    def setUp(self):
        mocker = requests_mock.Mocker()
        mocker.post(
            "https://api.ebay.com/identity/v1/oauth2/token",
            json={
                "access_token": "this is the access token",
                "expires_in": 7200,
                "token_type": "Application Access Token",
            },
            headers={"Content-Type": "application/json"},
        )
        mocker.start()
        self.addCleanup(mocker.stop)

        session = OAuth2Session()
        self.fixed_session = ebay_compliance_fix(session)

    def test_fetch_access_token(self):
        token = self.fixed_session.fetch_token(
            "https://api.ebay.com/identity/v1/oauth2/token",
            authorization_response="https://i.b/?code=hello",
        )
        assert token["token_type"] == "Bearer"


def access_and_refresh_token_request_compliance_fix_test(session, client_secret):
    def _non_compliant_header(url, headers, body):
        headers["X-Client-Secret"] = client_secret
        return url, headers, body

    session.register_compliance_hook("access_token_request", _non_compliant_header)
    session.register_compliance_hook("refresh_token_request", _non_compliant_header)
    return session


class RefreshTokenRequestComplianceFixTest(TestCase):
    value_to_test_for = "value_to_test_for"

    def setUp(self):
        mocker = requests_mock.Mocker()
        mocker.post(
            "https://example.com/token",
            request_headers={"X-Client-Secret": self.value_to_test_for},
            json={
                "access_token": "this is the access token",
                "expires_in": 7200,
                "token_type": "Bearer",
            },
            headers={"Content-Type": "application/json"},
        )
        mocker.post(
            "https://example.com/refresh",
            request_headers={"X-Client-Secret": self.value_to_test_for},
            json={
                "access_token": "this is the access token",
                "expires_in": 7200,
                "token_type": "Bearer",
            },
            headers={"Content-Type": "application/json"},
        )
        mocker.start()
        self.addCleanup(mocker.stop)

        session = OAuth2Session()
        self.fixed_session = access_and_refresh_token_request_compliance_fix_test(
            session, self.value_to_test_for
        )

    def test_access_token(self):
        token = self.fixed_session.fetch_token(
            "https://example.com/token",
            authorization_response="https://i.b/?code=hello",
        )
        assert token["token_type"] == "Bearer"

    def test_refresh_token(self):
        token = self.fixed_session.refresh_token(
            "https://example.com/refresh",
        )
        assert token["token_type"] == "Bearer"
