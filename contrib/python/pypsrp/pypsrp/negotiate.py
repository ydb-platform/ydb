# Copyright: (c) 2018, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import base64
import logging
import re
import typing
import warnings

import spnego
import spnego.channel_bindings
from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes
from requests.auth import AuthBase
from requests.packages.urllib3.response import HTTPResponse

from pypsrp._utils import get_hostname, to_bytes
from pypsrp.exceptions import AuthenticationError

log = logging.getLogger(__name__)


class NoCertificateRetrievedWarning(Warning):
    pass


class UnknownSignatureAlgorithmOID(Warning):
    pass


class HTTPNegotiateAuth(AuthBase):
    def __init__(
        self,
        username: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
        auth_provider: str = "negotiate",
        send_cbt: bool = True,
        service: str = "host",
        delegate: bool = False,
        hostname_override: typing.Optional[str] = None,
        wrap_required: bool = False,
    ) -> None:
        """
        Creates a HTTP auth context that uses Microsoft's Negotiate protocol
        to complete the auth process. This currently only supports the NTLM
        and Kerberos providers in the Negotiate protocol.

        :param username: The username to authenticate with, if not specified
            this will be with the user currently logged in (Windows only) or
            the default Kerberos ticket in the cache
        :param password: The password for username, if not specified this will
            try to use implicit credentials available to the user
        :param auth_provider: The authentication provider to use
            'negotiate': Will try to use Kerberos if available and fallback to
                NTLM if that fails
            'ntlm': Will only use NTLM
            'kerberos': Will only use Kerberos and will fail if this is not
                available
        :param send_cbt: Try to bind the channel token (HTTPS only) to the auth
            process, default is True
        :param service: The service part of the SPN to authenticate with,
            defaults to host
        :param delegate: Whether to get an auth token that allows the token to
            be delegated to other servers, this is only used with Kerberos and
            defaults to False
        :param hostname_override: Override the hostname used as part of the
            SPN, by default the hostname is based on the URL of the request
        :param wrap_required: Whether message encryption (wrapping) is
            required (controls what auth context is used)
        """
        self.username = username
        self.password = password
        self.auth_provider = auth_provider
        self.send_cbt = send_cbt
        self.service = service
        self.delegate = delegate
        self.hostname_override = hostname_override
        self.wrap_required = wrap_required
        self.contexts: typing.Dict[str, typing.Any] = {}

        self._regex = re.compile(r"(Kerberos|Negotiate|NTLM)\s*([^,]*),?", re.I)

    def __call__(self, request):
        request.headers["Connection"] = "Keep-Alive"
        request.register_hook("response", self.response_hook)

        return request

    def response_hook(self, response, **kwargs):
        if response.status_code == 401:
            matched_provider = self._check_auth_supported(response, ["Negotiate", "Kerberos", "NTLM"])
            kwargs["_pypsrp_auth_provider"] = matched_provider

            response = self.handle_401(response, **kwargs)

        return response

    def handle_401(self, response, **kwargs):
        response_auth_header = kwargs.pop("_pypsrp_auth_provider")
        response_auth_header_l = response_auth_header.lower()
        auth_provider = self.auth_provider

        if response_auth_header_l != self.auth_provider:
            if self.auth_provider == "negotiate":
                auth_provider = response_auth_header_l

            elif response_auth_header_l != "negotiate":
                raise ValueError(
                    "Server responded with the auth protocol '%s' which is incompatible with the "
                    "specified auth_provider '%s'" % (response_auth_header, auth_provider)
                )

        host = get_hostname(response.url)
        auth_hostname = self.hostname_override or host

        cbt = None
        if self.send_cbt:
            cbt_app_data = HTTPNegotiateAuth._get_cbt_data(response)
            if cbt_app_data:
                cbt = spnego.channel_bindings.GssChannelBindings(application_data=cbt_app_data)

        context_req = spnego.ContextReq.default
        if self.delegate:
            context_req |= spnego.ContextReq.delegate

        spnego_options = spnego.NegotiateOptions.wrapping_winrm if self.wrap_required else 0
        context = spnego.client(
            self.username,
            self.password,
            hostname=auth_hostname,
            service=self.service,
            channel_bindings=cbt,
            context_req=context_req,
            protocol=auth_provider,
            options=spnego_options,
        )
        self.contexts[host] = context

        out_token = context.step()
        while not context.complete or out_token is not None:
            # consume content and release the original connection to allow the
            # new request to reuse the same one.
            response.content
            response.raw.release_conn()

            # create a request with the Negotiate token present
            request = response.request.copy()
            log.debug("Sending http request with new auth token")
            self._set_auth_token(request, out_token, response_auth_header)

            # send the request with the auth token and get the response
            response = response.connection.send(request, **kwargs)

            # attempt to retrieve the auth token response
            in_token = self._get_auth_token(response, self._regex)

            # break if there was no token received from the host and return the
            # last response
            if in_token in [None, b""]:
                log.debug("Did not receive a http response with an auth response, stopping authentication process")
                break

            out_token = context.step(in_token)

        # This is used by the message encryption to decide the MIME protocol.
        setattr(context, "response_auth_header", response_auth_header_l)

        return response

    @staticmethod
    def _check_auth_supported(response, auth_providers):
        auth_supported = response.headers.get("www-authenticate", "")
        matched_providers = [p for p in auth_providers if p.upper() in auth_supported.upper()]
        if not matched_providers:
            raise AuthenticationError(
                "The server did not response with one of the following authentication methods "
                "%s - actual: '%s'" % (", ".join(auth_providers), auth_supported)
            )

        return matched_providers[0]

    @staticmethod
    def _set_auth_token(request, token, auth_provider):
        encoded_token = base64.b64encode(token)
        auth_header = to_bytes("%s " % auth_provider) + encoded_token
        request.headers["Authorization"] = auth_header

    @staticmethod
    def _get_auth_token(response, pattern):
        auth_header = response.headers.get("www-authenticate", "")
        token_match = pattern.search(auth_header)

        if not token_match:
            return None

        token = token_match.group(2)
        return base64.b64decode(token)

    @staticmethod
    def _get_cbt_data(response):
        """
        Tries to get the channel binding token as specified in RFC 5929 to pass
        along to the authentication provider. This is usually the SHA256
        hash of the certificate of the HTTPS endpoint appended onto the string
        'tls-server-end-point'.

        If the socket is not an SSL socker or the raw HTTP object is not a
        urllib3 HTTPResponse, then None will be returned and no channel binding
        data is passed onto the auth context

        :param response: The server's response which is used to sniff out the
            server's certificate
        :return: A byte string containing the CBT prefix and cert hash to pass
            onto the auth context
        """
        app_data = None
        raw_response = response.raw

        if isinstance(raw_response, HTTPResponse):
            try:
                socket = raw_response._fp.fp.raw._sock
            except AttributeError as err:
                warning = "Failed to get raw socket for CBT from urllib3 resp: %s" % str(err)
                warnings.warn(warning, NoCertificateRetrievedWarning)

            else:
                try:
                    cert = socket.getpeercert(True)
                except AttributeError:
                    pass
                else:
                    cert_hash = HTTPNegotiateAuth._get_certificate_hash(cert)
                    app_data = b"tls-server-end-point:" + cert_hash
        else:
            warning = (
                "Requests is running with a non urllib3 backend, cannot retrieve server cert for CBT. Raw "
                "type: %s" % type(response).__name__
            )
            warnings.warn(warning, NoCertificateRetrievedWarning)

        return app_data

    @staticmethod
    def _get_certificate_hash(certificate_der):
        """
        Get's the server's certificate hash for the tls-server-end-point
        channel binding.

        According to https://tools.ietf.org/html/rfc5929#section-4.1, this is
        calculated by
            Using the SHA256 is the signatureAlgorithm is MD5 or SHA1
            The signatureAlgorithm if the hash function is neither MD5 or SHA1

        :param certificate_der: The byte string of the server's certificate
        :return: The byte string containing the hash of the server's
            certificate
        """
        cert = x509.load_der_x509_certificate(certificate_der)

        hash_algorithm = None
        try:
            hash_algorithm = cert.signature_hash_algorithm
        except UnsupportedAlgorithm as ex:
            warnings.warn(
                "Failed to get the signature algorithm from the certificate due to: %s" % str(ex),
                UnknownSignatureAlgorithmOID,
            )

        # If the cert signature algorithm is unknown, md5, or sha1 then use sha256 otherwise use the signature
        # algorithm of the cert itself.
        if not hash_algorithm or hash_algorithm.name in ["md5", "sha1"]:
            digest = hashes.Hash(hashes.SHA256())
        else:
            digest = hashes.Hash(hash_algorithm)

        digest.update(certificate_der)
        certificate_hash = digest.finalize()

        return certificate_hash
