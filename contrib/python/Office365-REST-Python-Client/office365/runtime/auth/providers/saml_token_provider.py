import os
import uuid
from datetime import datetime, timezone
from typing import Optional
from xml.dom import minidom
from xml.etree import ElementTree

import requests
import requests.utils

import office365.logger
from office365.runtime.auth.auth_cookies import AuthCookies
from office365.runtime.auth.authentication_provider import AuthenticationProvider
from office365.runtime.auth.sts_profile import STSProfile
from office365.runtime.auth.user_realm_info import UserRealmInfo

office365.logger.ensure_debug_secrets()


def string_escape(value):
    value = value.replace("&", "&amp;")
    value = value.replace("<", "&lt;")
    value = value.replace(">", "&gt;")
    value = value.replace('"', "&quot;")
    value = value.replace("'", "&apos;")
    return value


def datetime_escape(value):
    return value.isoformat("T")[:-9] + "Z"


class SamlTokenProvider(AuthenticationProvider, office365.logger.LoggerContext):
    def __init__(self, url, username, password, browser_mode, environment=None):
        """
        SAML Security Token Service provider (claims-based authentication)

        :param str url: Site or Web absolute url
        :param str username: Typically a UPN in the form of an email address
        :param str password: The password
        :param bool browser_mode:
        :param str environment: The Office 365 Cloud Environment endpoint used for authentication.
        """
        # Security Token Service info
        self._sts_profile = STSProfile(url, environment)
        # Obtain authentication cookies, using the browser mode
        self._browser_mode = browser_mode
        # Last occurred error
        self.error = ""
        self._username = username
        self._password = password
        self._cached_auth_cookies = None  # type: Optional[AuthCookies]
        self.__ns_prefixes = {
            "S": "{http://www.w3.org/2003/05/soap-envelope}",
            "s": "{http://www.w3.org/2003/05/soap-envelope}",
            "psf": "{http://schemas.microsoft.com/Passport/SoapServices/SOAPFault}",
            "wst": "{http://schemas.xmlsoap.org/ws/2005/02/trust}",
            "wsse": "{http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd}",
            "saml": "{urn:oasis:names:tc:SAML:1.0:assertion}",
            "u": "{http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd}",
            "wsa": "{http://www.w3.org/2005/08/addressing}",
            "wsp": "{http://schemas.xmlsoap.org/ws/2004/09/policy}",
            "ps": "{http://schemas.microsoft.com/LiveID/SoapServices/v1}",
            "ds": "{http://www.w3.org/2000/09/xmldsig#}",
        }
        for key in self.__ns_prefixes.keys():
            ElementTree.register_namespace(key, self.__ns_prefixes[key][1:-1])

    def authenticate_request(self, request):
        """
        Authenticate request handler
        """
        logger = self.logger(self.authenticate_request.__name__)

        request_time = datetime.now(timezone.utc)
        if (
            self._cached_auth_cookies is None
            or request_time >= self._sts_profile.expires
        ):
            self._sts_profile.reset()
            self._cached_auth_cookies = self.get_authentication_cookie()
        logger.debug_secrets(self._cached_auth_cookies)
        request.set_header("Cookie", self._cached_auth_cookies.cookie_header)

    def get_authentication_cookie(self):
        """Acquire authentication cookie"""
        logger = self.logger(self.get_authentication_cookie.__name__)
        logger.debug("get_authentication_cookie called")

        try:
            logger.debug("Acquiring Access Token..")
            user_realm = self._get_user_realm()
            if user_realm.IsFederated:
                token = self._acquire_service_token_from_adfs(user_realm.STSAuthUrl)
            else:
                token = self._acquire_service_token()
            return self._get_authentication_cookie(token, user_realm.IsFederated)
        except requests.exceptions.RequestException as e:
            logger.error(e.response.text)
            self.error = "Error: {}".format(e)
            raise ValueError(e.response.text)

    def _get_user_realm(self):
        """Get User Realm"""
        resp = requests.post(
            self._sts_profile.user_realm_service_url,
            data="login={0}&xml=1".format(self._username),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        xml = ElementTree.fromstring(resp.content)
        node = xml.find("NameSpaceType")
        if node is not None:
            if node.text == "Federated":
                info = UserRealmInfo(xml.find("STSAuthURL").text, True)
            else:
                info = UserRealmInfo(None, False)
            return info
        return None

    def _acquire_service_token_from_adfs(self, adfs_url):
        logger = self.logger(self._acquire_service_token_from_adfs.__name__)

        payload = self._prepare_request_from_template(
            "FederatedSAML.xml",
            {
                "auth_url": adfs_url,
                "message_id": str(uuid.uuid4()),
                "username": string_escape(self._username),
                "password": string_escape(self._password),
                "created": datetime_escape(self._sts_profile.created),
                "expires": datetime_escape(self._sts_profile.expires),
                "issuer": self._sts_profile.token_issuer,
            },
        )

        response = requests.post(
            adfs_url,
            data=payload,
            headers={"Content-Type": "application/soap+xml; charset=utf-8"},
        )
        dom = minidom.parseString(response.content.decode())
        assertion_node = dom.getElementsByTagNameNS(
            "urn:oasis:names:tc:SAML:1.0:assertion", "Assertion"
        )[0].toxml()

        try:
            payload = self._prepare_request_from_template(
                "RST2.xml",
                {
                    "auth_url": self._sts_profile.tenant,
                    "serviceTokenUrl": self._sts_profile.security_token_service_url,
                    "assertion_node": assertion_node,
                },
            )

            # 3. get security token
            response = requests.post(
                self._sts_profile.security_token_service_url,
                data=payload,
                headers={"Content-Type": "application/soap+xml"},
            )
            token = self._process_service_token_response(response)
            logger.debug_secrets("security token: %s", token)
            return token
        except ElementTree.ParseError as e:
            self.error = (
                "An error occurred while parsing the server response: {}".format(e)
            )
            logger.error(self.error)
            return None

    def _acquire_service_token(self):
        """Retrieve service token"""
        logger = self.logger(self._acquire_service_token.__name__)

        payload = self._prepare_request_from_template(
            "SAML.xml",
            {
                "auth_url": self._sts_profile.site_url,
                "username": string_escape(self._username),
                "password": string_escape(self._password),
                "message_id": str(uuid.uuid4()),
                "created": datetime_escape(self._sts_profile.created),
                "expires": datetime_escape(self._sts_profile.expires),
                "issuer": self._sts_profile.token_issuer,
            },
        )
        logger.debug_secrets("options: %s", payload)
        response = requests.post(
            self._sts_profile.security_token_service_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = self._process_service_token_response(response)
        logger.debug_secrets("security token: %s", token)
        return token

    def _process_service_token_response(self, response):
        logger = self.logger(self._process_service_token_response.__name__)
        logger.debug_secrets(
            "response: %s\nresponse.content: %s", response, response.content
        )

        try:
            xml = ElementTree.fromstring(response.content)
        except ElementTree.ParseError as e:
            self.error = (
                "An error occurred while parsing the server response: {}".format(e)
            )
            logger.error(self.error)
            return None

        # check for errors
        if xml.find("{0}Body/{0}Fault".format(self.__ns_prefixes["s"])) is not None:
            error = xml.find(
                "{0}Body/{0}Fault/{0}Detail/{1}error/{1}internalerror/{1}text".format(
                    self.__ns_prefixes["s"], self.__ns_prefixes["psf"]
                )
            )
            if error is None:
                self.error = (
                    "An error occurred while retrieving token from XML response."
                )
            else:
                self.error = "An error occurred while retrieving token from XML response: {0}".format(
                    error.text
                )
            logger.error(self.error)
            raise ValueError(self.error)

        # extract token
        token = xml.find(
            "{0}Body/{1}RequestSecurityTokenResponse/{1}RequestedSecurityToken/{2}BinarySecurityToken".format(
                self.__ns_prefixes["s"],
                self.__ns_prefixes["wst"],
                self.__ns_prefixes["wsse"],
            )
        )
        if token is None:
            self.error = "Cannot get binary security token for from {0}".format(
                self._sts_profile.security_token_service_url
            )
            logger.error(self.error)
            raise ValueError(self.error)
        logger.debug_secrets("token: %s", token)
        return token.text

    def _get_authentication_cookie(self, security_token, federated=False):
        """Retrieve auth cookie from STS

        :type federated: bool
        :type security_token: str
        """
        logger = self.logger(self._get_authentication_cookie.__name__)

        session = requests.session()
        logger.debug_secrets(
            "session: %s\nsession.post(%s, data=%s)",
            session,
            self._sts_profile.signin_page_url,
            security_token,
        )
        if not federated or self._browser_mode:
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            if self._browser_mode:
                headers["User-Agent"] = (
                    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)"
                )
            session.post(
                self._sts_profile.signin_page_url, data=security_token, headers=headers
            )
        else:
            idcrl_endpoint = "https://{}/_vti_bin/idcrl.svc/".format(
                self._sts_profile.tenant
            )
            session.get(
                idcrl_endpoint,
                headers={
                    "User-Agent": "Office365 Python Client",
                    "X-IDCRL_ACCEPTED": "t",
                    "Authorization": "BPOSIDCRL {0}".format(security_token),
                },
            )
        logger.debug_secrets("session.cookies: %s", session.cookies)
        cookies = AuthCookies(requests.utils.dict_from_cookiejar(session.cookies))
        logger.debug_secrets("cookies: %s", cookies)
        if not cookies.is_valid:
            self.error = (
                "An error occurred while retrieving auth cookies from {0}".format(
                    self._sts_profile.signin_page_url
                )
            )
            logger.error(self.error)
            raise ValueError(self.error)
        return cookies

    def _prepare_request_from_template(self, template_name, params):
        """Construct the request body to acquire security token from STS endpoint"""
        logger = self.logger(self._prepare_request_from_template.__name__)
        logger.debug_secrets("params: %s", params)

        import importlib.resources
        template_path = importlib.resources.files(__package__) / "templates" / template_name

        with template_path.open(encoding="utf8") as f:
            data = f.read()

        return data.format(**params)
