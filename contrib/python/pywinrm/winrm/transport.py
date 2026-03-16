from __future__ import annotations

import os
import typing as t
import warnings

import requests
import requests.auth

from winrm.encryption import Encryption
from winrm.exceptions import InvalidCredentialsError, WinRMError, WinRMTransportError

DISPLAYED_PROXY_WARNING = False
DISPLAYED_CA_TRUST_WARNING = False


HAVE_KERBEROS = False
try:
    from winrm.vendor.requests_kerberos import REQUIRED, HTTPKerberosAuth

    HAVE_KERBEROS = True
except ImportError:
    pass

HAVE_NTLM = False
try:
    from requests_ntlm import HttpNtlmAuth

    HAVE_NTLM = True
except ImportError as ie:
    pass

HAVE_CREDSSP = False
try:
    from requests_credssp import HttpCredSSPAuth

    HAVE_CREDSSP = True
except ImportError as ie:
    pass

__all__ = ["Transport"]


def strtobool(value: str) -> bool:
    value = value.lower()
    if value in ("true", "t", "yes", "y", "on", "1"):
        return True

    elif value in ("false", "f", "no", "n", "off", "0"):
        return False

    else:
        raise ValueError("invalid truth value '%s'" % value)


class UnsupportedAuthArgument(Warning):
    pass


class Transport(object):
    def __init__(
        self,
        endpoint: str,
        username: str | None = None,
        password: str | None = None,
        realm: None = None,
        service: str | None = None,
        keytab: None = None,
        ca_trust_path: t.Literal["legacy_requests"] | str = "legacy_requests",
        cert_pem: str | None = None,
        cert_key_pem: str | None = None,
        read_timeout_sec: int | None = None,
        server_cert_validation: t.Literal["validate", "ignore"] | None = "validate",
        kerberos_delegation: bool | str = False,
        kerberos_hostname_override: str | None = None,
        auth_method: t.Literal["auto", "basic", "certificate", "ntlm", "kerberos", "credssp", "plaintext", "ssl"] = "auto",
        message_encryption: t.Literal["auto", "always", "never"] = "auto",
        credssp_disable_tlsv1_2: bool = False,
        credssp_auth_mechanism: t.Literal["auto", "ntlm", "kerberos"] = "auto",
        credssp_minimum_version: int = 2,
        send_cbt: bool = True,
        proxy: t.Literal["legacy_requests"] | str | None = "legacy_requests",
    ) -> None:
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.realm = realm
        self.service = service
        self.keytab = keytab
        self.ca_trust_path = ca_trust_path
        self.cert_pem = cert_pem
        self.cert_key_pem = cert_key_pem
        self.read_timeout_sec = read_timeout_sec
        self.server_cert_validation = server_cert_validation
        self.kerberos_hostname_override = kerberos_hostname_override
        self.message_encryption = message_encryption
        self.credssp_disable_tlsv1_2 = credssp_disable_tlsv1_2
        self.credssp_auth_mechanism = credssp_auth_mechanism
        self.credssp_minimum_version = credssp_minimum_version
        self.send_cbt = send_cbt
        self.proxy = proxy

        if self.server_cert_validation not in [None, "validate", "ignore"]:
            raise WinRMError("invalid server_cert_validation mode: %s" % self.server_cert_validation)

        # defensively parse this to a bool
        if isinstance(kerberos_delegation, bool):
            self.kerberos_delegation = kerberos_delegation
        else:
            self.kerberos_delegation = bool(strtobool(str(kerberos_delegation)))

        self.auth_method = auth_method
        self.default_headers = {
            "Content-Type": "application/soap+xml;charset=UTF-8",
            "User-Agent": "Python WinRM client",
        }

        # try to suppress user-unfriendly warnings from requests' vendored urllib3
        try:
            from requests.packages.urllib3.exceptions import InsecurePlatformWarning

            warnings.simplefilter("ignore", category=InsecurePlatformWarning)
        except Exception:
            pass  # oh well, we tried...

        try:
            from requests.packages.urllib3.exceptions import SNIMissingWarning

            warnings.simplefilter("ignore", category=SNIMissingWarning)
        except Exception:
            pass  # oh well, we tried...

        # if we're explicitly ignoring validation, try to suppress InsecureRequestWarning, since the user opted-in
        if self.server_cert_validation == "ignore":
            try:
                from requests.packages.urllib3.exceptions import InsecureRequestWarning

                warnings.simplefilter("ignore", category=InsecureRequestWarning)
            except Exception:
                pass  # oh well, we tried...

            try:
                from urllib3.exceptions import InsecureRequestWarning

                warnings.simplefilter("ignore", category=InsecureRequestWarning)
            except Exception:
                pass  # oh well, we tried...

        # validate credential requirements for various auth types
        if self.auth_method != "kerberos":
            if self.auth_method == "certificate" or (self.auth_method == "ssl" and (self.cert_pem or self.cert_key_pem)):
                if not self.cert_pem or not self.cert_key_pem:
                    raise InvalidCredentialsError("both cert_pem and cert_key_pem must be specified for cert auth")
                if not os.path.exists(self.cert_pem):
                    raise InvalidCredentialsError("cert_pem file not found (%s)" % self.cert_pem)
                if not os.path.exists(self.cert_key_pem):
                    raise InvalidCredentialsError("cert_key_pem file not found (%s)" % self.cert_key_pem)

            else:
                if not self.username:
                    raise InvalidCredentialsError("auth method %s requires a username" % self.auth_method)
                if self.password is None:
                    raise InvalidCredentialsError("auth method %s requires a password" % self.auth_method)

        self.session: requests.Session | None = None

        # Used for encrypting messages
        self.encryption: Encryption | None = None  # The Pywinrm Encryption class used to encrypt/decrypt messages
        if self.message_encryption not in ["auto", "always", "never"]:
            raise WinRMError("invalid message_encryption arg: %s. Should be 'auto', 'always', or 'never'" % self.message_encryption)

    def build_session(self) -> requests.Session:
        if self.session:
            return self.session

        session = requests.Session()
        proxies = dict()

        if self.proxy is None:
            proxies["no_proxy"] = "*"
        elif self.proxy != "legacy_requests":
            # If there was a proxy specified then use it
            proxies["http"] = self.proxy
            proxies["https"] = self.proxy

        # Merge proxy environment variables
        settings = session.merge_environment_settings(url=self.endpoint, proxies=proxies, stream=None, verify=None, cert=None)

        global DISPLAYED_PROXY_WARNING

        # We want to eventually stop reading proxy information from the environment.
        # Also only display the warning once. This method can be called many times during an application's runtime.
        if not DISPLAYED_PROXY_WARNING and self.proxy == "legacy_requests" and ("http" in settings["proxies"] or "https" in settings["proxies"]):
            message = "'pywinrm' will use an environment defined proxy. This feature will be disabled in " "the future, please specify it explicitly."
            if "http" in settings["proxies"]:
                message += " HTTP proxy {proxy} discovered.".format(proxy=settings["proxies"]["http"])
            if "https" in settings["proxies"]:
                message += " HTTPS proxy {proxy} discovered.".format(proxy=settings["proxies"]["https"])

            DISPLAYED_PROXY_WARNING = True
            warnings.warn(message, DeprecationWarning)

        session.proxies = settings["proxies"]

        # specified validation mode takes precedence
        session.verify = self.server_cert_validation == "validate"

        # patch in CA path override if one was specified in init or env
        if session.verify:
            if self.ca_trust_path == "legacy_requests" and settings["verify"] is not None:
                # We will
                session.verify = settings["verify"]

                global DISPLAYED_CA_TRUST_WARNING

                # We want to eventually stop reading proxy information from the environment.
                # Also only display the warning once. This method can be called many times during an application's runtime.
                if not DISPLAYED_CA_TRUST_WARNING and session.verify is not True:
                    message = (
                        "'pywinrm' will use an environment variable defined CA Trust. This feature will be disabled in "
                        "the future, please specify it explicitly."
                    )
                    if os.environ.get("REQUESTS_CA_BUNDLE") is not None:
                        message += " REQUESTS_CA_BUNDLE contains {ca_path}".format(ca_path=os.environ.get("REQUESTS_CA_BUNDLE"))
                    elif os.environ.get("CURL_CA_BUNDLE") is not None:
                        message += " CURL_CA_BUNDLE contains {ca_path}".format(ca_path=os.environ.get("CURL_CA_BUNDLE"))

                    DISPLAYED_CA_TRUST_WARNING = True
                    warnings.warn(message, DeprecationWarning)

            elif session.verify and self.ca_trust_path is not None:
                # session.verify can be either a bool or path to a CA store; prefer passed-in value over env if both are present
                session.verify = self.ca_trust_path

        encryption_available = False

        if self.auth_method == "kerberos":
            if not HAVE_KERBEROS:
                raise WinRMError("requested auth method is kerberos, but pykerberos is not installed")

            kerb_auth = session.auth = HTTPKerberosAuth(
                mutual_authentication=REQUIRED,
                delegate=self.kerberos_delegation,
                force_preemptive=True,
                principal=self.username,
                hostname_override=self.kerberos_hostname_override,
                sanitize_mutual_error_response=False,
                service=self.service,
                send_cbt=self.send_cbt,
            )
            encryption_available = hasattr(session.auth, "winrm_encryption_available") and kerb_auth.winrm_encryption_available
        elif self.auth_method in ["certificate", "ssl"]:
            if self.auth_method == "ssl" and not self.cert_pem and not self.cert_key_pem:
                # 'ssl' was overloaded for HTTPS with optional certificate auth,
                # fall back to basic auth if no cert specified
                session.auth = requests.auth.HTTPBasicAuth(
                    username=self.username or "",
                    password=self.password or "",
                )
            else:
                session.cert = (self.cert_pem or "", self.cert_key_pem or "")
                session.headers["Authorization"] = "http://schemas.dmtf.org/wbem/wsman/1/wsman/secprofile/https/mutual"
        elif self.auth_method == "ntlm":
            if not HAVE_NTLM:
                raise WinRMError("requested auth method is ntlm, but requests_ntlm is not installed")

            session.auth = HttpNtlmAuth(
                username=self.username,
                password=self.password,
                send_cbt=self.send_cbt,
            )
            # check if requests_ntlm has the session_security attribute available for encryption
            encryption_available = hasattr(session.auth, "session_security")
        # TODO: ssl is not exactly right here- should really be client_cert
        elif self.auth_method in ["basic", "plaintext"]:
            session.auth = requests.auth.HTTPBasicAuth(
                username=self.username or "",
                password=self.password or "",
            )
        elif self.auth_method == "credssp":
            if not HAVE_CREDSSP:
                raise WinRMError("requests auth method is credssp, but requests-credssp is not installed")

            session.auth = HttpCredSSPAuth(
                username=self.username,
                password=self.password,
                disable_tlsv1_2=self.credssp_disable_tlsv1_2,
                auth_mechanism=self.credssp_auth_mechanism,
                minimum_version=self.credssp_minimum_version,
            )
            encryption_available = True
        else:
            raise WinRMError("unsupported auth method: %s" % self.auth_method)

        session.headers.update(self.default_headers)
        self.session = session

        # Will check the current config and see if we need to setup message encryption
        if self.message_encryption == "always" and not encryption_available:
            raise WinRMError("message encryption is set to 'always' but the selected auth method %s does not support it" % self.auth_method)
        elif encryption_available:
            if self.message_encryption == "always":
                self.setup_encryption(session)
            elif self.message_encryption == "auto" and not self.endpoint.lower().startswith("https"):
                self.setup_encryption(session)

        return session

    def setup_encryption(self, session: requests.Session) -> None:
        # Security context doesn't exist, sending blank message to initialise context
        request = requests.Request("POST", self.endpoint, data=None)
        prepared_request = session.prepare_request(request)
        self._send_message_request(session, prepared_request)
        self.encryption = Encryption(session, self.auth_method)

    def close_session(self) -> None:
        if not self.session:
            return
        self.session.close()
        self.session = None

    def send_message(self, message: str | bytes) -> bytes:
        session = self.build_session()

        # urllib3 fails on SSL retries with unicode buffers- must send it a byte string
        # see https://github.com/shazow/urllib3/issues/717
        if isinstance(message, str):
            message = message.encode("utf-8")

        if self.encryption:
            prepared_request = self.encryption.prepare_encrypted_request(session, self.endpoint, message)
        else:
            request = requests.Request("POST", self.endpoint, data=message)
            prepared_request = session.prepare_request(request)

        response = self._send_message_request(session, prepared_request)
        return self._get_message_response_text(response)

    def _send_message_request(self, session: requests.Session, prepared_request: requests.PreparedRequest) -> requests.Response:
        try:
            response = session.send(prepared_request, timeout=self.read_timeout_sec)
            response.raise_for_status()
            return response
        except requests.HTTPError as ex:
            if ex.response.status_code == 401:
                raise InvalidCredentialsError("the specified credentials were rejected by the server")
            if ex.response.content:
                response_text = self._get_message_response_text(ex.response)
            else:
                response_text = b""

            raise WinRMTransportError("http", ex.response.status_code, response_text.decode())

    def _get_message_response_text(self, response: requests.Response) -> bytes:
        if self.encryption:
            response_text = self.encryption.parse_encrypted_response(response)
        else:
            response_text = response.content
        return response_text
