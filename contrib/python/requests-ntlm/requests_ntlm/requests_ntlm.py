from __future__ import annotations

import base64
import typing as t
import warnings
from urllib.parse import urlparse

import requests
import spnego
from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from requests.auth import AuthBase
from requests.packages.urllib3.response import HTTPResponse


class ShimSessionSecurity:
    """Shim used for backwards compatibility with ntlm-auth."""

    def __init__(self, context: spnego.ContextProxy) -> None:
        self._context = context

    def wrap(self, message: bytes) -> tuple[bytes, bytes]:
        wrap_res = self._context.wrap(message, encrypt=True)
        signature = wrap_res.data[:16]
        data = wrap_res.data[16:]

        return data, signature

    def unwrap(self, message: bytes, signature: bytes) -> bytes:
        data = signature + message
        return self._context.unwrap(data).data

    def get_signature(self, message: bytes) -> bytes:
        return self._context.sign(message)

    def verify_signature(self, message: bytes, signature: bytes) -> None:
        self._context.verify(message, signature)


class HttpNtlmAuth(AuthBase):
    """
    HTTP NTLM Authentication Handler for Requests.

    Supports pass-the-hash.
    """

    def __init__(
        self,
        username: str | None,
        password: str | None,
        session: None = None,
        send_cbt: bool = True,
    ) -> None:
        """Create an authentication handler for NTLM over HTTP.

        :param str username: Username in 'domain\\username' format
        :param str password: Password
        :param str session: Unused. Kept for backwards-compatibility.
        :param bool send_cbt: Will send the channel bindings over a HTTPS channel (Default: True)
        """
        self.username = username
        self.password = password
        self.send_cbt = send_cbt

        # This exposes the encrypt/decrypt methods used to encrypt and decrypt messages
        # sent after ntlm authentication. These methods are utilised by libraries that
        # call requests_ntlm to encrypt and decrypt the messages sent after authentication
        self.session_security: ShimSessionSecurity | None = None

    def retry_using_http_NTLM_auth(
        self,
        auth_header_field: str,
        auth_header: str,
        response: requests.Response,
        auth_type: str,
        args: t.Any,
    ) -> requests.Response:
        # Get the certificate of the server if using HTTPS for CBT
        server_certificate_hash = self._get_server_cert(response)
        cbt = None
        if server_certificate_hash:
            cbt = spnego.channel_bindings.GssChannelBindings(
                application_data=b"tls-server-end-point:" + server_certificate_hash
            )

        """Attempt to authenticate using HTTP NTLM challenge/response."""
        if auth_header in response.request.headers:
            return response

        content_length = int(
            response.request.headers.get("Content-Length", "0"), base=10
        )
        if response.request.body and hasattr(response.request.body, "seek"):
            if content_length > 0:
                response.request.body.seek(-content_length, 1)
            else:
                response.request.body.seek(0, 0)

        # Consume content and release the original connection
        # to allow our new request to reuse the same one.
        response.content
        response.raw.release_conn()
        request = response.request.copy()

        target_hostname = t.cast(str, urlparse(response.url).hostname)
        spnego_options = spnego.NegotiateOptions.none
        if self.username and self.password:
            # If a username and password are specified force spnego to use the
            # pure NTLM code. This is for backwards compatibility with older
            # requests-ntlm versions which never used SSPI. If no username and
            # password are specified we need to rely on the cached SSPI
            # behaviour.
            # https://github.com/requests/requests-ntlm/issues/136#issuecomment-1751677055
            spnego_options = spnego.NegotiateOptions.use_ntlm

        client = spnego.client(
            self.username,
            self.password,
            protocol="ntlm",
            channel_bindings=cbt,
            hostname=target_hostname,
            service="http",
            options=spnego_options,
        )
        # Perform the first step of the NTLM authentication
        negotiate_message = base64.b64encode(client.step() or b"").decode()
        auth = "%s %s" % (auth_type, negotiate_message)

        request.headers[auth_header] = auth

        # A streaming response breaks authentication.
        # This can be fixed by not streaming this request, which is safe
        # because the returned response3 will still have stream=True set if
        # specified in args. In addition, we expect this request to give us a
        # challenge and not the real content, so the content will be short
        # anyway.
        args_nostream = dict(args, stream=False)
        response2 = response.connection.send(request, **args_nostream)  # type: ignore[attr-defined]

        # needed to make NTLM auth compatible with requests-2.3.0

        # Consume content and release the original connection
        # to allow our new request to reuse the same one.
        response2.content
        response2.raw.release_conn()
        request = response2.request.copy()

        # this is important for some web applications that store
        # authentication-related info in cookies (it took a long time to
        # figure out)
        if response2.headers.get("set-cookie"):
            request.headers["Cookie"] = response2.headers.get("set-cookie")

        # get the challenge
        auth_header_value = response2.headers[auth_header_field]

        auth_strip = auth_type + " "

        ntlm_header_value = next(
            (
                s.strip()
                for s in (val.lstrip() for val in auth_header_value.split(","))
                if s.startswith(auth_strip)
            ),
            None,
        )

        if not ntlm_header_value:
            raise PermissionError(
                "Access denied: Server did not respond with NTLM challenge token"
            )

        # Parse the challenge in the ntlm context and perform
        # the second step of authentication
        val = base64.b64decode(ntlm_header_value[len(auth_strip) :].encode())
        authenticate_message = base64.b64encode(client.step(val) or b"").decode()

        auth = "%s %s" % (auth_type, authenticate_message)
        request.headers[auth_header] = auth

        response3 = response2.connection.send(request, **args)
        # Update the history.
        response3.history.append(response)
        response3.history.append(response2)

        self.session_security = ShimSessionSecurity(client)

        return response3

    def response_hook(self, r: requests.Response, **kwargs: t.Any) -> requests.Response:
        """The actual hook handler."""
        if r.status_code == 401:
            # Handle server auth.
            www_authenticate = r.headers.get("www-authenticate", "").lower()
            auth_type = _auth_type_from_header(www_authenticate)

            if auth_type is not None:
                return self.retry_using_http_NTLM_auth(
                    "www-authenticate",
                    "Authorization",
                    r,
                    auth_type,
                    kwargs,
                )
        elif r.status_code == 407:
            # If we didn't have server auth, do proxy auth.
            proxy_authenticate = r.headers.get("proxy-authenticate", "").lower()
            auth_type = _auth_type_from_header(proxy_authenticate)
            if auth_type is not None:
                return self.retry_using_http_NTLM_auth(
                    "proxy-authenticate",
                    "Proxy-authorization",
                    r,
                    auth_type,
                    kwargs,
                )

        return r

    def _get_server_cert(self, response: requests.Response) -> bytes | None:
        """
        Get the certificate at the request_url and return it as a hash. Will get the raw socket from the
        original response from the server. This socket is then checked if it is an SSL socket and then used to
        get the hash of the certificate. The certificate hash is then used with NTLMv2 authentication for
        Channel Binding Tokens support. If the raw object is not a urllib3 HTTPReponse (default with requests)
        then no certificate will be returned.

        :param response: The original 401 response from the server
        :return: The hash of the DER encoded certificate at the request_url or None if not a HTTPS endpoint
        """
        if self.send_cbt:
            raw_response = response.raw

            if isinstance(raw_response, HTTPResponse):
                socket = raw_response._fp.fp.raw._sock

                try:
                    server_certificate = socket.getpeercert(True)
                except AttributeError:
                    pass
                else:
                    return _get_certificate_hash(server_certificate)
            else:
                warnings.warn(
                    "Requests is running with a non urllib3 backend, cannot retrieve server certificate for CBT",
                    NoCertificateRetrievedWarning,
                )

        return None

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        # we must keep the connection because NTLM authenticates the
        # connection, not single requests
        r.headers["Connection"] = "Keep-Alive"

        r.register_hook("response", self.response_hook)
        return r


def _auth_type_from_header(header: str) -> str | None:
    """
    Given a WWW-Authenticate or Proxy-Authenticate header, returns the
    authentication type to use. We prefer NTLM over Negotiate if the server
    suppports it.
    """
    if "ntlm" in header:
        return "NTLM"
    elif "negotiate" in header:
        return "Negotiate"

    return None


def _get_certificate_hash(certificate_der: bytes) -> bytes | None:
    # https://tools.ietf.org/html/rfc5929#section-4.1
    cert = x509.load_der_x509_certificate(certificate_der, default_backend())

    try:
        hash_algorithm = cert.signature_hash_algorithm
    except UnsupportedAlgorithm as ex:
        warnings.warn(
            "Failed to get signature algorithm from certificate, "
            "unable to pass channel bindings: %s" % str(ex),
            UnknownSignatureAlgorithmOID,
        )
        return None

    # if the cert signature algorithm is either md5 or sha1 then use sha256
    # otherwise use the signature algorithm
    if not hash_algorithm or hash_algorithm.name in ["md5", "sha1"]:
        digest = hashes.Hash(hashes.SHA256(), default_backend())
    else:
        digest = hashes.Hash(hash_algorithm, default_backend())

    digest.update(certificate_der)
    certificate_hash_bytes = digest.finalize()

    return certificate_hash_bytes


class NoCertificateRetrievedWarning(Warning):
    pass


class UnknownSignatureAlgorithmOID(Warning):
    pass
