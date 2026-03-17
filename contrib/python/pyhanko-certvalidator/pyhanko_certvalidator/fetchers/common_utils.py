"""
Internal backend-agnostic utilities to help process fetched certificates, CRLs
and OCSP responses.
"""

import asyncio
import logging
import os
from typing import Awaitable, Callable, Dict, Iterable, Optional, TypeVar, Union

from asn1crypto import algos, cms, core, ocsp, pem, x509
from asn1crypto.x509 import DistributionPoint

from .. import errors
from ..authority import Authority
from ..util import get_ac_extension_value

__all__ = [
    'unpack_cert_content',
    'format_ocsp_request',
    'process_ocsp_response_data',
    'queue_fetch_task',
    'crl_job_results_as_completed',
    'ocsp_job_get_earliest',
    'complete_certificate_fetch_jobs',
    'gather_aia_issuer_urls',
    'ACCEPTABLE_STRICT_CERT_CONTENT_TYPES',
    'ACCEPTABLE_CERT_PEM_ALIASES',
    'ACCEPTABLE_PKCS7_DER_ALIASES',
    'ACCEPTABLE_CERT_DER_ALIASES',
]

logger = logging.getLogger(__name__)


ACCEPTABLE_STRICT_CERT_CONTENT_TYPES = frozenset(
    [
        'application/pkix-cert',
        'application/pkcs7-mime',
        'application/x-x509-ca-cert',
        'application/x-pkcs7-certificates',
    ]
)

ACCEPTABLE_CERT_PEM_ALIASES = frozenset(
    [
        'application/x-pem-file',
        'text/plain',
        'application/octet-stream',
        'binary/octet-stream',
    ]
)

ACCEPTABLE_CERT_DER_ALIASES = frozenset(
    [
        'application/pkix-cert',
        'application/x-x509-ca-cert',
        'application/octet-stream',
        'binary/octet-stream',
    ]
)


ACCEPTABLE_PKCS7_DER_ALIASES = frozenset(
    [
        'application/pkcs7-mime',
        'application/x-pkcs7-certificates',
        'binary/octet-stream',
    ]
)


def unpack_cert_content(
    response_data: bytes,
    content_type: Optional[str],
    url: str,
    permit_pem: bool,
):
    is_pem = pem.detect(response_data)
    if (
        content_type is None or content_type in ACCEPTABLE_CERT_DER_ALIASES
    ) and not is_pem:
        # sometimes we get DER over octet-stream
        if content_type is None:
            logger.warning(
                f"Response to certificate fetch request to {url} did not "
                f"include a content type, verifying it's sequence length to "
                f"check if it is a certificate or pkcs7."
            )
        der_sequence_length = len(core.Sequence.load(response_data))
        if der_sequence_length == 2:
            yield from _unpack_der_pkcs7(response_data, url)
        elif der_sequence_length == 3:
            yield x509.Certificate.load(response_data)
    elif (content_type in ACCEPTABLE_PKCS7_DER_ALIASES) and not is_pem:
        yield from _unpack_der_pkcs7(response_data, url)
    elif permit_pem and is_pem:
        # technically, PEM is not allowed here, but of course some people don't
        # bother following the rules
        for type_name, _, data in pem.unarmor(response_data, multiple=True):
            if type_name == 'PKCS7':
                yield from _unpack_der_pkcs7(data, url)
            else:
                yield x509.Certificate.load(data)
    else:  # pragma: nocover
        raise ValueError(
            f"Failed to extract certs from {content_type} payload. "
            f"Source URL: {url}."
        )


def _unpack_der_pkcs7(pkcs7_data: bytes, pkcs7_url: str):
    content_info: cms.ContentInfo = cms.ContentInfo.load(pkcs7_data)
    cms_ct = content_info['content_type'].native
    if cms_ct != 'signed_data':
        raise ValueError(
            "Expected CMS SignedData when extracting certs from "
            "application/pkcs7-mime payload, but content type was "
            f"'{cms_ct}'. Source URL: {pkcs7_url}."
        )
    signed_data = content_info['content']
    if isinstance(signed_data['certificates'], cms.CertificateSet):
        for cert_choice in signed_data['certificates']:
            if cert_choice.name == 'certificate':
                yield cert_choice.chosen


def get_certid(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    authority: Authority,
    *,
    certid_hash_algo,
) -> ocsp.CertId:
    if isinstance(cert, x509.Certificate):
        serial_number = cert.serial_number
    else:
        serial_number = cert['ac_info']['serial_number'].native

    iss_name_hash = getattr(authority.name, certid_hash_algo)
    cert_id = ocsp.CertId(
        {
            'hash_algorithm': algos.DigestAlgorithm(
                {'algorithm': certid_hash_algo}
            ),
            'issuer_name_hash': iss_name_hash,
            'issuer_key_hash': getattr(authority.public_key, certid_hash_algo),
            'serial_number': serial_number,
        }
    )
    return cert_id


def format_ocsp_request(
    cert: x509.Certificate,
    authority: Authority,
    *,
    certid_hash_algo: str,
    request_nonces: bool,
):
    cert_id = get_certid(cert, authority, certid_hash_algo=certid_hash_algo)

    request = ocsp.Request(
        {
            'req_cert': cert_id,
        }
    )
    tbs_request = ocsp.TBSRequest(
        {
            'request_list': ocsp.Requests([request]),
        }
    )

    if request_nonces:
        nonce_extension = ocsp.TBSRequestExtension(
            {
                'extn_id': 'nonce',
                'critical': False,
                'extn_value': core.OctetString(os.urandom(16)),
            }
        )
        tbs_request['request_extensions'] = ocsp.TBSRequestExtensions(
            [nonce_extension]
        )

    return ocsp.OCSPRequest({'tbs_request': tbs_request})


def process_ocsp_response_data(
    response_data: bytes, *, ocsp_request: ocsp.OCSPRequest, ocsp_url: str
):
    try:
        ocsp_response = ocsp.OCSPResponse.load(response_data)
    except ValueError:
        raise errors.OCSPFetchError('Failed to parse response from OCSP server')
    status = ocsp_response['response_status'].native
    if status != 'successful':
        raise errors.OCSPValidationError(
            'OCSP server at %s returned an error. Status was \'%s\'.'
            % (ocsp_url, status)
        )

    request_nonce = ocsp_request.nonce_value
    if request_nonce:
        response_nonce = ocsp_response.nonce_value
        # if the response did not contain the nonce extension, there's no
        # point in trying to enforce it, that's the CA's problem.
        #  (I suppose we could give callers the option to mark the nonce
        #  extension as critical in the request, but that's discouraged by the
        #  specification)
        if response_nonce and (request_nonce.native != response_nonce.native):
            raise errors.OCSPValidationError(
                'Unable to verify OCSP response since the request and '
                'response nonces do not match'
            )
    return ocsp_response


T = TypeVar('T')
R = TypeVar('R')


async def queue_fetch_task(
    results: Dict[T, Union[R, Exception]],
    running_jobs: Dict[T, asyncio.Event],
    tag: T,
    async_fun: Callable[[], Awaitable[R]],
) -> Union[R, Exception]:
    # use an asyncio events to make sure that we don't attempt to re-fetch
    # the same tag while the job is running
    # Note: this uses asyncio locking, so we only transfer control
    # on 'await'.
    # We use events instead of locks because we don't care about fairness,
    # and events are easier to reason about.
    try:
        result = results[tag]
        logger.debug(
            f"Result for fetch job with tag {repr(tag)} was available in cache."
        )
        return _return_or_raise(result)
    except KeyError:
        pass
    try:
        wait_event: asyncio.Event = running_jobs[tag]
        logger.debug(f"Waiting for fetch job with tag {repr(tag)} to return...")
        # there's a fetch job running, wait for it to finish and then
        # return the result
        await wait_event.wait()
        logger.debug(
            f"Received completion signal for job with tag {repr(tag)}."
        )
        return _return_or_raise(results[tag])
    except KeyError:
        logger.debug(f"Starting new fetch job with tag {repr(tag)}...")
        # no fetch job running, run the task and store the result
        running_jobs[tag] = wait_event = asyncio.Event()
        try:
            result = await async_fun()
        except Exception as e:
            logger.debug(
                f"New fetch job with tag {repr(tag)} threw an exception: {e}"
            )
            result = e
        results[tag] = result
        logger.debug(f"New fetch job with tag {repr(tag)} returned.")
        # deregister event, notify waiters
        del running_jobs[tag]
        wait_event.set()
        return _return_or_raise(result)


def _return_or_raise(result):
    if isinstance(result, Exception):
        raise result
    return result


async def crl_job_results_as_completed(jobs):
    last_e = None
    at_least_one_success = False
    for crl_job in asyncio.as_completed(list(jobs)):
        try:
            fetched_crl = await crl_job
            yield fetched_crl
        except errors.CRLFetchError as e:
            last_e = e

    if last_e is not None and not at_least_one_success:
        raise last_e


async def cancel_all(pending_tasks):
    pending = asyncio.gather(*pending_tasks)
    pending.cancel()
    try:
        await pending
    except asyncio.CancelledError:
        pass


async def ocsp_job_get_earliest(jobs):
    queue = [asyncio.create_task(coro) for coro in jobs]
    ocsp_resp = last_e = None
    while queue:
        done, queue = await asyncio.wait(
            queue, return_when=asyncio.FIRST_COMPLETED
        )
        for ocsp_job in done:
            try:
                ocsp_resp = await ocsp_job
                break
            except errors.OCSPFetchError as e:
                last_e = e
    if ocsp_resp is not None:
        # cancel remaining fetch tasks
        await cancel_all(queue)
        return ocsp_resp
    raise last_e or errors.OCSPFetchError("No OCSP results")


def gather_aia_issuer_urls(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
):
    if isinstance(cert, x509.Certificate):
        aia_value = cert.authority_information_access_value
    else:
        aia_value = get_ac_extension_value(cert, 'authority_information_access')
    if aia_value is None:
        return
    for entry in aia_value:
        if entry['access_method'].native == 'ca_issuers':
            location = entry['access_location']
            if location.name != 'uniform_resource_identifier':
                continue
            url = location.native
            if url.startswith('http'):
                yield url


async def complete_certificate_fetch_jobs(fetch_jobs):
    for fetch_job in asyncio.as_completed(fetch_jobs):
        try:
            certs_fetched = await fetch_job
        except errors.CertificateFetchError as e:
            logger.warning(
                f'Error during certificate fetch job, skipping... '
                f'(Error: {e})',
            )
            continue
        for cert in certs_fetched:
            yield cert


def enumerate_delivery_point_urls(distribution_point: DistributionPoint):
    name = distribution_point['distribution_point']
    if name.name != 'full_name':
        # We don't support relative DPs
        #  (esp. since we don't support directory-based lookups at all)
        return

    for general_name in name.chosen:
        if general_name.name == 'uniform_resource_identifier':
            url = general_name.native
            # Only fetch CRLs over http
            #  (or https, but that doesn't really happen all that often)
            # In particular, don't attempt to grab CRLs over LDAP
            if url.lower().startswith(('http://', 'https://')):
                yield url
