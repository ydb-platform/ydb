"""

.. versionadded:: 0.10.0

Asynchronous :class:`~pyhanko.sign.signers.pdf_cms.Signer` implementation for
interacting with a remote signing service using the Cloud Signature Consortium
(CSC) API.

This implementation is based on version 1.0.4.0 (2019-06) of the CSC API
specification.


Usage notes
-----------

This module's :class:`.CSCSigner` class supplies an implementation of the
:class:`~pyhanko.sign.signers.pdf_cms.Signer` class in pyHanko.
As such, it is flexible enough to be used either through pyHanko's high-level
API (:func:`~pyhanko.sign.signers.functions.sign_pdf` et al.), or through
the :ref:`interrupted signing API <interrupted-signing>`.

:class:`.CSCSigner` overview
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:class:`.CSCSigner` is only directly responsible for calling the
``signatures/signHash`` endpoint in the CSC API. Other than that, it only
handles batch control. This means that the following tasks require further
action on the API user's part:

 * authenticating to the signing service (typically using OAuth2);
 * obtaining Signature Activation Data (SAD) from the signing service;
 * provisioning the certificates to embed into the document (usually
   those are supplied by the signing service as well).

The first two involve a degree of implementation/vendor dependence that is
difficult to cater to in full generality, and the third is out of scope
for :class:`~pyhanko.sign.signers.pdf_cms.Signer` subclasses in general.

However, this module still provides a number of convenient hooks and guardrails
that should allow you to fill in these blanks with relative ease. We briefly
discuss these below.

Throughout, the particulars of how pyHanko should connect to a signing
service are supplied in a :class:`.CSCServiceSessionInfo` object.
This object contains the base CSC API URL, the CSC credential ID to use,
and authentication data.


Authenticating to the signing service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

While the authentication process itself is the API user's responsibility,
:class:`.CSCServiceSessionInfo` includes an
:attr:`~.CSCServiceSessionInfo.oauth_token` field that will (by default)
be used to populate the HTTP ``Authorization`` header for every request.

To handle OAuth-specific tasks, you might want to use a library like
`OAuthLib <https://oauthlib.readthedocs.io/en/latest/>`_.


Obtaining SAD from the signing service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is done by subclassing :class:`.CSCAuthorizationInfo` and passing
an instance to the :class:`.CSCSigner`. The :class:`.CSCAuthorizationInfo`
instance should call the signer's ``credentials/authorize`` endpoint with
the proper parameters required by the service.
See the documentation for :class:`.CSCAuthorizationInfo` for details and=
information about helper functions.


Certificate provisioning
^^^^^^^^^^^^^^^^^^^^^^^^

In pyHanko's API, :class:`~pyhanko.sign.signers.pdf_cms.Signer` instances
need to be initialised with the signer's certificate, preferably together
with other relevant CA certificates.
In a CSC context, these are typically retrieved from the signing service by
calling the ``credentials/info`` endpoint.

This module offers a helper function to handle that task, see
:func:`.fetch_certs_in_csc_credential`.
"""

import abc
import asyncio
import base64
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, FrozenSet, List, Optional

import tzlocal
from asn1crypto import algos, x509
from cryptography.hazmat.primitives import hashes
from pyhanko_certvalidator.registry import (
    CertificateStore,
    SimpleCertificateStore,
)

from pyhanko.sign import Signer
from pyhanko.sign.general import SigningError, get_pyca_cryptography_hash

try:
    import aiohttp
except ImportError:  # pragma: nocover
    raise ImportError("Install pyHanko with [async_http]")

__all__ = [
    'CSCSigner',
    'CSCServiceSessionInfo',
    'CSCCredentialInfo',
    'fetch_certs_in_csc_credential',
    'CSCAuthorizationInfo',
    'CSCAuthorizationManager',
    'PrefetchedSADAuthorizationManager',
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CSCServiceSessionInfo:
    """
    Information about the CSC service, together with the required authentication
    data.
    """

    service_url: str
    """
    Base URL of the CSC service. This is the part that precedes
    ``/csc/<version>/...`` in the API endpoint URLs.
    """

    credential_id: str
    """
    The identifier of the CSC credential to use when signing.
    The format is vendor-dependent.
    """

    oauth_token: Optional[str] = None
    """
    OAuth token to use when making requests to the CSC service.
    """

    api_ver: str = 'v1'
    """
    CSC API version.

    .. note::
        This section does not affect any of the internal logic, it only changes
        how the URLs are formatted.
    """

    def endpoint_url(self, endpoint_name):
        """
        Complete an endpoint name to a full URL.

        :param endpoint_name:
            Name of the endpoint (e.g. ``credentials/info``).
        :return:
            A URL.
        """
        return f"{self.service_url}/csc/{self.api_ver}/{endpoint_name}"

    @property
    def auth_headers(self):
        """
        HTTP Header(s) necessary for authentication, to be passed with every
        request.

        .. note::
            By default, this supplies the ``Authorization`` header
            with the value of :attr:`oauth_token` as the ``Bearer`` value.

        :return:
            A ``dict`` of headers.
        """

        tok = self.oauth_token
        return {'Authorization': f'Bearer {tok}'} if tok else {}


@dataclass(frozen=True)
class CSCCredentialInfo:
    """
    Information about a CSC credential, typically fetched using a
    ``credentials/info`` call. See also :func:`.fetch_certs_in_csc_credential`.
    """

    signing_cert: x509.Certificate
    """
    The signer's certificate.
    """

    chain: List[x509.Certificate]
    """
    Other relevant CA certificates.
    """

    supported_mechanisms: FrozenSet[str]
    """
    Signature mechanisms supported by the credential.
    """

    max_batch_size: int
    """
    The maximal batch size that can be used with this credential.
    """

    hash_pinning_required: bool
    """
    Flag controlling whether SAD must be tied to specific hashes.
    """

    response_data: dict
    """
    The JSON response data from the server as an otherwise unparsed ``dict``.
    """

    def as_cert_store(self) -> CertificateStore:
        """
        Register the relevant certificates into a :class:`.CertificateStore`
        and return it.

        :return:
            A :class:`.CertificateStore`.
        """

        scs = SimpleCertificateStore()
        scs.register(self.signing_cert)
        scs.register_multiple(self.chain)
        return scs


async def fetch_certs_in_csc_credential(
    session: aiohttp.ClientSession,
    csc_session_info: CSCServiceSessionInfo,
    timeout: int = 30,
) -> CSCCredentialInfo:
    """
    Call the ``credentials/info`` endpoint of the CSC service for a specific
    credential, and encode the result into a :class:`.CSCCredentialInfo`
    object.

    :param session:
        The ``aiohttp`` session to use when performing queries.
    :param csc_session_info:
        General information about the CSC service and the credential.
    :param timeout:
        How many seconds to allow before time-out.
    :return:
        A :class:`.CSCCredentialInfo` object with the processed response.
    """
    url = csc_session_info.endpoint_url("credentials/info")
    req_data = {
        "credentialID": csc_session_info.credential_id,
        "certificates": "chain",
        "certInfo": False,
    }

    try:
        async with session.post(
            url,
            headers=csc_session_info.auth_headers,
            json=req_data,
            raise_for_status=True,
            timeout=timeout,
        ) as response:
            response_data = await response.json()
    except aiohttp.ClientError as e:
        raise SigningError("Credential info request failed") from e

    return _process_certificate_info_response(response_data)


def _process_certificate_info_response(response_data) -> CSCCredentialInfo:
    try:
        b64_certs = response_data['cert']['certificates']
    except KeyError as e:
        raise SigningError(
            "Could not retrieve certificates from response"
        ) from e
    try:
        certs = [
            x509.Certificate.load(base64.b64decode(cert)) for cert in b64_certs
        ]
    except ValueError as e:
        raise SigningError("Could not decode certificates in response") from e
    try:
        algo_oids = response_data["key"]["algo"]
        if not isinstance(algo_oids, list):
            raise TypeError
        supported_algos = frozenset(
            algos.SignedDigestAlgorithmId(oid).native for oid in algo_oids
        )
    except (KeyError, ValueError, TypeError) as e:
        raise SigningError(
            "Could not retrieve supported signing mechanisms from response"
        ) from e

    try:
        max_batch_size = int(response_data['multisign'])
    except (KeyError, ValueError) as e:
        raise SigningError(
            "Could not retrieve max batch size from response"
        ) from e

    scal_value = response_data.get("SCAL", 1)
    try:
        scal_value = int(scal_value)
        if scal_value not in (1, 2):
            raise ValueError
    except ValueError:
        raise SigningError("SCAL value must be \"1\" or \"2\".")
    hash_pinning_required = scal_value == 2

    return CSCCredentialInfo(
        # The CSC spec requires the signer's certificate to be first
        # in the 'certs' array. The order for the others is unspecified,
        # but that doesn't matter.
        signing_cert=certs[0],
        chain=certs[1:],
        supported_mechanisms=supported_algos,
        max_batch_size=max_batch_size,
        hash_pinning_required=hash_pinning_required,
        response_data=response_data,
    )


def base64_digest(data: bytes, digest_algorithm: str) -> str:
    """
    Digest some bytes and base64-encode the result.

    :param data:
        Data to digest.
    :param digest_algorithm:
        Name of the digest algorihtm to use.
    :return:
        A base64-encoded hash.
    """

    hash_spec = get_pyca_cryptography_hash(digest_algorithm)
    md = hashes.Hash(hash_spec)
    md.update(data)
    return base64.b64encode(md.finalize()).decode('ascii')


@dataclass(frozen=True)
class CSCAuthorizationInfo:
    """
    Authorization data to make a signing request.
    This is the result of a call to ``credentials/authorize``.
    """

    sad: str
    """
    Signature activation data; opaque to the client.
    """

    expires_at: Optional[datetime] = None
    """
    Expiry date of the signature activation data.
    """


class CSCAuthorizationManager(abc.ABC):
    """
    Abstract class that handles authorisation requests for the CSC signing
    client.

    .. note::
        Implementations may wish to make use of the
        :meth:`format_csc_auth_request` convenience method to format
        requests to the ``credentials/authorize`` endpoint.

    :param csc_session_info:
        General information about the CSC service and the credential.
    :param credential_info:
        Details about the credential.
    """

    def __init__(
        self,
        csc_session_info: CSCServiceSessionInfo,
        credential_info: CSCCredentialInfo,
    ):
        self.csc_session_info = csc_session_info
        self.credential_info = credential_info

    async def authorize_signature(
        self, hash_b64s: List[str]
    ) -> CSCAuthorizationInfo:
        """
        Request a SAD token from the signing service, either freshly or to
        extend the current transaction.

        Depending on the lifecycle of this object, pre-fetched SAD values
        may be used. All authorization transaction management is left to
        implementing subclasses.

        :param hash_b64s:
            Base64-encoded hash values about to be signed.
        :return:
            Authorization data.
        """
        raise NotImplementedError

    def format_csc_auth_request(
        self,
        num_signatures: int = 1,
        pin: Optional[str] = None,
        otp: Optional[str] = None,
        hash_b64s: Optional[List[str]] = None,
        description: Optional[str] = None,
        client_data: Optional[str] = None,
    ) -> dict:
        """
        Format the parameters for a call to ``credentials/authorize``.

        :param num_signatures:
            The number of signatures to request authorisation for.
        :param pin:
            The user's PIN (if applicable).
        :param otp:
            The current value of an OTP token, provided by the user
            (if applicable).
        :param hash_b64s:
            An explicit list of base64-encoded hashes to be tied to the SAD.
            Is optional if the service's SCAL value is 1, i.e.
            when :attr:`~.CSCCredentialInfo.hash_pinning_required` is false.
        :param description:
            A free-form description of the authorisation request
            (optional).
        :param client_data:
            Custom vendor-specific data (if applicable).
        :return:
            A dict that, when encoded as a JSON object, be used as the request
            body for a call to ``credentials/authorize``.
        """
        result: Dict[str, Any] = {
            'credentialID': self.csc_session_info.credential_id
        }

        if hash_b64s is not None:
            # make num_signatures congruent with the number of hashes passed in
            # (this is a SHOULD in the spec, but we enforce it here)
            num_signatures = len(hash_b64s)
            result['hash'] = hash_b64s

        result['numSignatures'] = num_signatures

        if pin is not None:
            result['PIN'] = pin
        if otp is not None:
            result['OTP'] = otp
        if description is not None:
            result['description'] = description
        if client_data is not None:
            result['clientData'] = client_data

        return result

    @staticmethod
    def parse_csc_auth_response(response_data: dict) -> CSCAuthorizationInfo:
        """
        Parse the response from a ``credentials/authorize`` call into
        a :class:`.CSCAuthorizationInfo` object.

        :param response_data:
            The decoded response JSON.
        :return:
            A :class:`.CSCAuthorizationInfo` object.
        """

        try:
            sad = str(response_data["SAD"])
        except KeyError:
            raise SigningError("Could not extract SAD value from auth response")

        try:
            lifetime_seconds = int(response_data.get('expiresIn', 3600))
            now = datetime.now(tz=tzlocal.get_localzone())
            expires_at = now + timedelta(seconds=lifetime_seconds)
        except ValueError as e:
            raise SigningError(
                "Could not process expiresIn value in auth response"
            ) from e
        return CSCAuthorizationInfo(sad=sad, expires_at=expires_at)

    @property
    def auth_headers(self):
        """
        HTTP Header(s) necessary for authentication, to be passed with every
        request. By default, this delegates to
        :attr:`.CSCServiceSessionInfo.auth_headers`.

        :return:
            A ``dict`` of headers.
        """
        return self.csc_session_info.auth_headers


class PrefetchedSADAuthorizationManager(CSCAuthorizationManager):
    """
    Simplistic :class:`.CSCAuthorizationManager` for use with pre-fetched
    signature activation data.

    This class is effectively only useful for CSC services that do not require
    SAD to be pinned to specific document hashes. It allows you to use a SAD
    that was fetched before starting the signing process, for a one-shot
    signature.

    This can simplify resource management in cases where obtaining a
    SAD is time-consuming, but the caller still wants the conveniences of
    pyHanko's high-level API without having to keep too many pyHanko objects
    in memory while waiting for a ``credentials/authorize`` call to go through.

    Legitimate uses are limited, but the implementation is trivial, so we
    provide it here.

    :param csc_session_info:
        General information about the CSC service and the credential.
    :param credential_info:
        Details about the credential.
    :param csc_auth_info:
        The pre-fetched signature activation data.
    """

    def __init__(
        self,
        csc_session_info: CSCServiceSessionInfo,
        credential_info: CSCCredentialInfo,
        csc_auth_info: CSCAuthorizationInfo,
    ):
        super().__init__(csc_session_info, credential_info)
        self.csc_auth_info = csc_auth_info
        self._used = False

    async def authorize_signature(
        self, hash_b64s: List[str]
    ) -> CSCAuthorizationInfo:
        """
        Return the prefetched SAD, or raise an error if called twice.

        :param hash_b64s:
            List of hashes to be signed; ignored.
        :return:
            The prefetched authorisation data.
        """
        if self._used:
            raise SigningError("Prefetched SAD token is stale")
        self._used = True
        return self.csc_auth_info


@dataclass
class _CSCBatchInfo:
    """
    Internal value type to track batch control data.
    """

    notifier: asyncio.Event
    """
    Event object used to notify waiting coroutines that the batch has
    been committed.
    """

    md_algorithm: str
    """
    The digest algorithm that was used for the entire batch.
    """

    b64_hashes: List[str] = field(default_factory=list)
    """
    List of base64-encoded hashes to sign.
    """

    initiated: bool = False
    """
    Flag indicating whether the commit process has been started.
    """

    results: Optional[List[bytes]] = None
    """
    Signature(s) returned by the API.
    """

    def add(self, b64_hash: str) -> int:
        ix = len(self.b64_hashes)
        self.b64_hashes.append(b64_hash)
        return ix


class CSCSigner(Signer):
    """
    Implements the :class:`~pyhanko.sign.signers.pdf_cms.Signer` interface
    for a remote CSC signing service.
    Requests are made asynchronously, using ``aiohttp``.

    :param session:
        The ``aiohttp`` session to use when performing queries.
    :param auth_manager:
        A :class:`.CSCAuthorizationManager` instance capable of procuring
        signature activation data from the signing service.
    :param sign_timeout:
        Timeout for signing operations, in seconds.
        Defaults to 300 seconds (5 minutes).
    :param prefer_pss:
        When signing using an RSA key, prefer PSS padding to legacy PKCS#1 v1.5
        padding. Default is ``False``. This option has no effect on non-RSA
        signatures.
    :param embed_roots:
        Option that controls whether or not additional self-signed certificates
        should be embedded into the CMS payload. The default is ``True``.
    :param client_data:
        CSC client data to add to any signing request(s), if applicable.
    :param batch_autocommit:
        Whether to automatically commit a signing transaction as soon as a
        batch is full. The default is ``True``.
        If ``False``, the caller has to trigger :meth:`commit` manually.
    :param batch_size:
        The number of signatures to sign in one transaction.
        This defaults to 1 (i.e. a separate ``signatures/signHash`` call is made
        for every signature).
    :param est_raw_signature_size:
        Estimated raw signature size (in bytes). Defaults to 512 bytes, which,
        combined with other built-in safety margins, should provide a generous
        overestimate.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        auth_manager: CSCAuthorizationManager,
        sign_timeout: int = 300,
        prefer_pss: bool = False,
        embed_roots: bool = True,
        client_data: Optional[str] = None,
        batch_autocommit: bool = True,
        batch_size: Optional[int] = None,
        est_raw_signature_size=512,
    ):
        credential_info = auth_manager.credential_info
        self.auth_manager = auth_manager
        self.session = session
        self.est_raw_signature_size = est_raw_signature_size
        self.sign_timeout = sign_timeout
        self.client_data = client_data
        self.batch_autocommit = batch_autocommit
        self._current_batch: Optional[_CSCBatchInfo] = None
        self.batch_size = batch_size or 1
        super().__init__(
            prefer_pss=prefer_pss,
            embed_roots=embed_roots,
            signing_cert=credential_info.signing_cert,
            cert_registry=credential_info.as_cert_store(),
        )

    def get_signature_mechanism_for_digest(self, digest_algorithm):
        if self.signature_mechanism is not None:
            return self.signature_mechanism
        result = super().get_signature_mechanism_for_digest(digest_algorithm)
        result_algo = result['algorithm']
        supported = self.auth_manager.credential_info.supported_mechanisms
        if result_algo.native not in supported:
            raise SigningError(
                f"Signature mechanism {result_algo.native} is not supported, "
                f"must be one of {', '.join(alg for alg in supported)}."
            )
        return result

    async def format_csc_signing_req(
        self, tbs_hashes: List[str], digest_algorithm: str
    ) -> dict:
        """
        Populate the request data for a CSC signing request

        :param tbs_hashes:
            Base64-encoded hashes that require signing.
        :param digest_algorithm:
            The digest algorithm to use.
        :return:
            A dict that, when encoded as a JSON object, be used as the request
            body for a call to ``signatures/signHash``.
        """

        mechanism = self.get_signature_mechanism_for_digest(digest_algorithm)
        session_info = self.auth_manager.csc_session_info
        # SAD can be bound to specific hashes, but the authorization
        # process typically takes more wall clock time (esp. when
        # authorization requires a human user to perform an action).
        # Putting get_activation_data in a separate coroutine
        # allows API users to choose whether they want to provide
        # the credentials at init time, or just-in-time tied to specific
        # hashes.
        # The latter might not scale as easily within this architecture;
        # if you want both optimal security _and_ optimal performance,
        # you'll have to use this signer in the interrupted signing workflow.
        auth_info: CSCAuthorizationInfo = (
            await self.auth_manager.authorize_signature(tbs_hashes)
        )

        req_data = {
            'credentialID': session_info.credential_id,
            'SAD': auth_info.sad,
            'hashAlgo': algos.DigestAlgorithmId(digest_algorithm).dotted,
            'signAlgo': mechanism['algorithm'].dotted,
            'hash': tbs_hashes,
        }
        if mechanism['parameters'].native is not None:
            params_der = mechanism['parameters'].dump()
            req_data['signAlgoParams'] = base64.b64encode(params_der).decode(
                'ascii'
            )
        if self.client_data is not None:
            req_data['clientData'] = self.client_data

        return req_data

    async def _ensure_batch(self, digest_algorithm) -> _CSCBatchInfo:
        while self._current_batch is not None and self._current_batch.initiated:
            logger.debug("Commit ongoing... Waiting for it to finish")
            # There's a commit going on, wait for it to finish
            await self._current_batch.notifier.wait()
            logger.debug(
                f"Done waiting for commit: "
                f"new batch: {repr(self._current_batch)})"
            )
            # ...and start a new batch right after (unless someone else
            # already did, or the new batch is somehow already full/already
            # committing, in which case we have to keep queueing)
        if self._current_batch is not None:
            batch = self._current_batch
            if batch.md_algorithm != digest_algorithm:
                raise SigningError(
                    f"All signatures in the same batch must use the same "
                    f"digest function; encountered both {batch.md_algorithm} "
                    f"and {digest_algorithm}."
                )
            return batch
        self._current_batch = batch = _CSCBatchInfo(
            notifier=asyncio.Event(),
            md_algorithm=digest_algorithm,
        )
        return batch

    async def async_sign_raw(
        self, data: bytes, digest_algorithm: str, dry_run=False
    ) -> bytes:
        if dry_run:
            return bytes(self.est_raw_signature_size)

        tbs_hash = base64_digest(data, digest_algorithm)
        # ensure that there's a batch that we can hitch a ride on
        batch = await self._ensure_batch(digest_algorithm)
        ix = batch.add(tbs_hash)
        # autocommit if the batch is full
        if self.batch_autocommit and ix == self.batch_size - 1:
            try:
                await self.commit()
            except SigningError as e:
                # log and move on, we'll throw a regular exception later
                logger.error("Failed to commit signatures", exc_info=e)

        # Sleep until a commit goes through
        await batch.notifier.wait()
        if not batch.results:
            raise SigningError("No signing results available")
        return batch.results[ix]

    async def commit(self):
        """
        Commit the current batch by calling the ``signatures/signHash`` endpoint
        on the CSC service.

        This coroutine does not return anything; instead, it notifies all
        waiting signing coroutines that their signature has been fetched.
        """

        batch = self._current_batch
        if batch is None or batch.results is not None:
            return
        elif batch.initiated:
            # just wait for the commit to finish together with
            # all the signers in the queue
            await batch.notifier.wait()
            if not batch.results:
                raise SigningError("Commit failed")
        else:
            batch.initiated = True
            await self._do_commit(batch)

    async def _do_commit(self, batch: _CSCBatchInfo):
        """
        Internal commit routine that skips error handling and concurrency
        checks.
        """

        try:
            req_data = await self.format_csc_signing_req(
                batch.b64_hashes, batch.md_algorithm
            )
            session_info = self.auth_manager.csc_session_info
            url = session_info.endpoint_url("signatures/signHash")
            session = self.session
            async with session.post(
                url,
                headers=self.auth_manager.auth_headers,
                json=req_data,
                raise_for_status=True,
                timeout=self.sign_timeout,
            ) as response:
                response_data = await response.json()
            sig_b64s = response_data['signatures']
            actual_len = len(sig_b64s)
            expected_len = len(batch.b64_hashes)
            if actual_len != expected_len:
                raise SigningError(
                    f"Expected {expected_len} signatures, got {actual_len}"
                )
            signatures = [base64.b64decode(sig) for sig in sig_b64s]
            batch.results = signatures
        except SigningError:
            raise
        except (ValueError, KeyError, TypeError) as e:
            raise SigningError(
                "Expected response with b64-encoded signature values"
            ) from e
        except aiohttp.ClientError as e:
            raise SigningError("Signature request failed") from e
        finally:
            self._current_batch = None
            batch.notifier.set()
