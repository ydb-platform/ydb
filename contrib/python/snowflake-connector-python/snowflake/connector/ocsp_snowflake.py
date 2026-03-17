#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import codecs
import json
import os
import platform
import re
import sys
import tempfile
import time
import traceback
from base64 import b64decode, b64encode
from datetime import datetime
from logging import getLogger
from os import environ, path
from os.path import expanduser
from threading import Lock, RLock
from time import gmtime, strftime
from typing import Any, NamedTuple

# We use regular requests and urlib3 when we reach out to do OCSP checks, basically in this very narrow
# part of the code where we want to call out to check for revoked certificates,
# we don't want to use our hardened version of requests.
import requests as generic_requests
from asn1crypto.ocsp import CertId, OCSPRequest
from asn1crypto.x509 import Certificate
from OpenSSL.SSL import Connection

from snowflake.connector.compat import OK, urlsplit, urlunparse
from snowflake.connector.constants import HTTP_HEADER_USER_AGENT
from snowflake.connector.errorcode import (
    ER_INVALID_OCSP_RESPONSE_SSD,
    ER_INVALID_SSD,
    ER_OCSP_FAILED_TO_CONNECT_CACHE_SERVER,
    ER_OCSP_RESPONSE_ATTACHED_CERT_EXPIRED,
    ER_OCSP_RESPONSE_ATTACHED_CERT_INVALID,
    ER_OCSP_RESPONSE_CACHE_DECODE_FAILED,
    ER_OCSP_RESPONSE_CACHE_DOWNLOAD_FAILED,
    ER_OCSP_RESPONSE_CERT_STATUS_INVALID,
    ER_OCSP_RESPONSE_CERT_STATUS_REVOKED,
    ER_OCSP_RESPONSE_CERT_STATUS_UNKNOWN,
    ER_OCSP_RESPONSE_EXPIRED,
    ER_OCSP_RESPONSE_FETCH_EXCEPTION,
    ER_OCSP_RESPONSE_FETCH_FAILURE,
    ER_OCSP_RESPONSE_INVALID_EXPIRY_INFO_MISSING,
    ER_OCSP_RESPONSE_INVALID_SIGNATURE,
    ER_OCSP_RESPONSE_LOAD_FAILURE,
    ER_OCSP_RESPONSE_STATUS_UNSUCCESSFUL,
    ER_OCSP_RESPONSE_UNAVAILABLE,
    ER_OCSP_URL_INFO_MISSING,
)
from snowflake.connector.errors import RevocationCheckError
from snowflake.connector.network import PYTHON_CONNECTOR_USER_AGENT
from snowflake.connector.telemetry_oob import TelemetryService
from snowflake.connector.time_util import DecorrelateJitterBackoff

from snowflake.connector.vendored.requests import certs

from . import constants
from .cache import SFDictCache, SFDictFileCache
from .telemetry import TelemetryField, generate_telemetry_data_dict


class OCSPResponseValidationResult(NamedTuple):
    exception: Exception | None = None
    issuer: Certificate | None = None
    subject: Certificate | None = None
    cert_id: CertId | None = None
    ocsp_response: bytes | None = None
    ts: int | None = None
    validated: bool = False


try:
    OCSP_RESPONSE_VALIDATION_CACHE: SFDictFileCache[
        tuple[bytes, bytes, bytes],
        OCSPResponseValidationResult,
    ] = SFDictFileCache(
        entry_lifetime=constants.DAY_IN_SECONDS,
        file_path={
            "linux": os.path.join(
                "~", ".cache", "snowflake", "ocsp_response_validation_cache"
            ),
            "darwin": os.path.join(
                "~", "Library", "Caches", "Snowflake", "ocsp_response_validation_cache"
            ),
            "windows": os.path.join(
                "~",
                "AppData",
                "Local",
                "Snowflake",
                "Caches",
                "ocsp_response_validation_cache",
            ),
        },
    )
except OSError:
    # In case we run into some read/write permission error fall back onto
    #  in memory caching
    OCSP_RESPONSE_VALIDATION_CACHE: SFDictCache[
        tuple[bytes, bytes, bytes],
        OCSPResponseValidationResult,
    ] = SFDictCache(
        entry_lifetime=constants.DAY_IN_SECONDS,
    )

logger = getLogger(__name__)


def generate_cache_key(
    cert_id: CertId,
) -> tuple[bytes, bytes, bytes]:
    return (
        cert_id["issuer_name_hash"].dump(),
        cert_id["issuer_key_hash"].dump(),
        cert_id["serial_number"].dump(),
    )


class OCSPTelemetryData:

    CERTIFICATE_EXTRACTION_FAILED = "CertificateExtractionFailed"
    OCSP_URL_MISSING = "OCSPURLMissing"
    OCSP_RESPONSE_UNAVAILABLE = "OCSPResponseUnavailable"
    OCSP_RESPONSE_FETCH_EXCEPTION = "OCSPResponseFetchException"
    OCSP_RESPONSE_FAILED_TO_CONNECT_CACHE_SERVER = (
        "OCSPResponseFailedToConnectCacheServer"
    )
    OCSP_RESPONSE_CERT_STATUS_INVALID = "OCSPResponseCertStatusInvalid"
    OCSP_RESPONSE_CERT_STATUS_REVOKED = "OCSPResponseCertStatusRevoked"
    OCSP_RESPONSE_CERT_STATUS_UNKNOWN = "OCSPResponseCertStatusUnknown"
    OCSP_RESPONSE_STATUS_UNSUCCESSFUL = "OCSPResponseStatusUnsuccessful"
    OCSP_RESPONSE_ATTACHED_CERT_INVALID = "OCSPResponseAttachedCertInvalid"
    OCSP_RESPONSE_ATTACHED_CERT_EXPIRED = "OCSPResponseAttachedCertExpired"
    OCSP_RESPONSE_INVALID_SIGNATURE = "OCSPResponseSignatureInvalid"
    OCSP_RESPONSE_EXPIRY_INFO_MISSING = "OCSPResponseExpiryInfoMissing"
    OCSP_RESPONSE_EXPIRED = "OCSPResponseExpired"
    OCSP_RESPONSE_FETCH_FAILURE = "OCSPResponseFetchFailure"
    OCSP_RESPONSE_CACHE_DOWNLOAD_FAILED = "OCSPResponseCacheDownloadFailed"
    OCSP_RESPONSE_CACHE_DECODE_FAILED = "OCSPResponseCacheDecodeFailed"
    OCSP_RESPONSE_LOAD_FAILURE = "OCSPResponseLoadFailure"
    OCSP_RESPONSE_INVALID_SSD = "OCSPResponseInvalidSSD"

    ERROR_CODE_MAP = {
        ER_OCSP_URL_INFO_MISSING: OCSP_URL_MISSING,
        ER_OCSP_RESPONSE_UNAVAILABLE: OCSP_RESPONSE_UNAVAILABLE,
        ER_OCSP_RESPONSE_FETCH_EXCEPTION: OCSP_RESPONSE_FETCH_EXCEPTION,
        ER_OCSP_FAILED_TO_CONNECT_CACHE_SERVER: OCSP_RESPONSE_FAILED_TO_CONNECT_CACHE_SERVER,
        ER_OCSP_RESPONSE_CERT_STATUS_INVALID: OCSP_RESPONSE_CERT_STATUS_INVALID,
        ER_OCSP_RESPONSE_CERT_STATUS_REVOKED: OCSP_RESPONSE_CERT_STATUS_REVOKED,
        ER_OCSP_RESPONSE_CERT_STATUS_UNKNOWN: OCSP_RESPONSE_CERT_STATUS_UNKNOWN,
        ER_OCSP_RESPONSE_STATUS_UNSUCCESSFUL: OCSP_RESPONSE_STATUS_UNSUCCESSFUL,
        ER_OCSP_RESPONSE_ATTACHED_CERT_INVALID: OCSP_RESPONSE_ATTACHED_CERT_INVALID,
        ER_OCSP_RESPONSE_ATTACHED_CERT_EXPIRED: OCSP_RESPONSE_ATTACHED_CERT_EXPIRED,
        ER_OCSP_RESPONSE_INVALID_SIGNATURE: OCSP_RESPONSE_INVALID_SIGNATURE,
        ER_OCSP_RESPONSE_INVALID_EXPIRY_INFO_MISSING: OCSP_RESPONSE_EXPIRY_INFO_MISSING,
        ER_OCSP_RESPONSE_EXPIRED: OCSP_RESPONSE_EXPIRED,
        ER_OCSP_RESPONSE_FETCH_FAILURE: OCSP_RESPONSE_FETCH_FAILURE,
        ER_OCSP_RESPONSE_LOAD_FAILURE: OCSP_RESPONSE_LOAD_FAILURE,
        ER_OCSP_RESPONSE_CACHE_DOWNLOAD_FAILED: OCSP_RESPONSE_CACHE_DOWNLOAD_FAILED,
        ER_OCSP_RESPONSE_CACHE_DECODE_FAILED: OCSP_RESPONSE_CACHE_DECODE_FAILED,
        ER_INVALID_OCSP_RESPONSE_SSD: OCSP_RESPONSE_INVALID_SSD,
        ER_INVALID_SSD: OCSP_RESPONSE_INVALID_SSD,
    }

    def __init__(self):
        self.event_sub_type = None
        self.ocsp_connection_method = None
        self.cert_id = None
        self.sfc_peer_host = None
        self.ocsp_url = None
        self.ocsp_req = None
        self.error_msg = None
        self.cache_enabled = False
        self.cache_hit = False
        self.fail_open = False
        self.insecure_mode = False

    def set_event_sub_type(self, event_sub_type: str) -> None:
        """
        Sets sub type for OCSP Telemetry Event.

        There can be multiple event_sub_type that could have happened
        during a single connection establishment. Ensure that all of them
        are captured.
        :param event_sub_type:
        :return:
        """
        if self.event_sub_type is not None:
            self.event_sub_type = f"{self.event_sub_type}|{event_sub_type}"
        else:
            self.event_sub_type = event_sub_type

    def set_ocsp_connection_method(self, ocsp_conn_method: str) -> None:
        self.ocsp_connection_method = ocsp_conn_method

    def set_cert_id(self, cert_id):
        self.cert_id = cert_id

    def set_sfc_peer_host(self, sfc_peer_host):
        self.sfc_peer_host = sfc_peer_host

    def set_ocsp_url(self, ocsp_url):
        self.ocsp_url = ocsp_url

    def set_ocsp_req(self, ocsp_req):
        self.ocsp_req = ocsp_req

    def set_error_msg(self, error_msg):
        self.error_msg = error_msg

    def set_cache_enabled(self, cache_enabled):
        self.cache_enabled = cache_enabled
        if not cache_enabled:
            self.cache_hit = False

    def set_cache_hit(self, cache_hit):
        if not self.cache_enabled:
            self.cache_hit = False
        else:
            self.cache_hit = cache_hit

    def set_fail_open(self, fail_open):
        self.fail_open = fail_open

    def set_insecure_mode(self, insecure_mode):
        self.insecure_mode = insecure_mode

    def generate_telemetry_data(
        self, event_type: str, urgent: bool = False
    ) -> dict[str, Any]:
        _, exception, _ = sys.exc_info()
        telemetry_data = generate_telemetry_data_dict(
            from_dict={
                TelemetryField.KEY_OOB_EVENT_TYPE.value: event_type,
                TelemetryField.KEY_OOB_EVENT_SUB_TYPE.value: self.event_sub_type,
                TelemetryField.KEY_OOB_SFC_PEER_HOST.value: self.sfc_peer_host,
                TelemetryField.KEY_OOB_CERT_ID.value: self.cert_id,
                TelemetryField.KEY_OOB_OCSP_REQUEST_BASE64.value: self.ocsp_req,
                TelemetryField.KEY_OOB_OCSP_RESPONDER_URL.value: self.ocsp_url,
                TelemetryField.KEY_OOB_ERROR_MESSAGE.value: self.error_msg,
                TelemetryField.KEY_OOB_INSECURE_MODE.value: self.insecure_mode,
                TelemetryField.KEY_OOB_FAIL_OPEN.value: self.fail_open,
                TelemetryField.KEY_OOB_CACHE_ENABLED.value: self.cache_enabled,
                TelemetryField.KEY_OOB_CACHE_HIT.value: self.cache_hit,
            },
            is_oob_telemetry=True,
        )

        telemetry_client = TelemetryService.get_instance()
        telemetry_client.log_ocsp_exception(
            event_type,
            telemetry_data,
            exception=str(exception),
            stack_trace=traceback.format_exc(),
            urgent=urgent,
        )

        return telemetry_data
        # To be updated once Python Driver has out of band telemetry.
        # telemetry_client = TelemetryClient()
        # telemetry_client.add_log_to_batch(TelemetryData(telemetry_data, datetime.utcnow()))


class OCSPServer:
    MAX_RETRY = int(os.getenv("OCSP_MAX_RETRY", "3"))

    def __init__(self):
        self.DEFAULT_CACHE_SERVER_URL = "http://ocsp.snowflakecomputing.com"
        """
        The following will change to something like
        http://ocspssd.snowflakecomputing.com/ocsp/
        once the endpoint is up in the backend
        """
        self.NEW_DEFAULT_CACHE_SERVER_BASE_URL = (
            "https://ocspssd.snowflakecomputing.com/ocsp/"
        )
        if not OCSPServer.is_enabled_new_ocsp_endpoint():
            self.CACHE_SERVER_URL = os.getenv(
                "SF_OCSP_RESPONSE_CACHE_SERVER_URL",
                "{}/{}".format(
                    self.DEFAULT_CACHE_SERVER_URL,
                    OCSPCache.OCSP_RESPONSE_CACHE_FILE_NAME,
                ),
            )
        else:
            self.CACHE_SERVER_URL = os.getenv("SF_OCSP_RESPONSE_CACHE_SERVER_URL")

        self.CACHE_SERVER_ENABLED = (
            os.getenv("SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED", "true") != "false"
        )
        # OCSP dynamic cache server URL pattern
        self.OCSP_RETRY_URL = None

    @staticmethod
    def is_enabled_new_ocsp_endpoint():
        """Checks if new OCSP Endpoint has been enabled."""
        return os.getenv("SF_OCSP_ACTIVATE_NEW_ENDPOINT", "false").lower() == "true"

    def reset_ocsp_endpoint(self, hname):
        """Resets current object members CACHE_SERVER_URL and RETRY_URL_PATTERN.

        They will point at the new OCSP Fetch and Retry endpoints respectively. The new OCSP Endpoint address is based
        on the hostname the customer is trying to connect to. The deployment or in case of client failover, the
        replication ID is copied from the hostname.
        """
        if hname.endswith("privatelink.snowflakecomputing.com"):
            temp_ocsp_endpoint = "".join(["https://ocspssd.", hname, "/ocsp/"])
        elif hname.endswith("global.snowflakecomputing.com"):
            rep_id_begin = hname[hname.find("-") :]
            temp_ocsp_endpoint = "".join(["https://ocspssd", rep_id_begin, "/ocsp/"])
        elif not hname.endswith("snowflakecomputing.com"):
            temp_ocsp_endpoint = self.NEW_DEFAULT_CACHE_SERVER_BASE_URL
        else:
            hname_wo_acc = hname[hname.find(".") :]
            temp_ocsp_endpoint = "".join(["https://ocspssd", hname_wo_acc, "/ocsp/"])

        self.CACHE_SERVER_URL = "".join([temp_ocsp_endpoint, "fetch"])
        self.OCSP_RETRY_URL = "".join([temp_ocsp_endpoint, "retry"])

    def reset_ocsp_dynamic_cache_server_url(self, use_ocsp_cache_server):
        """Resets OCSP dynamic cache server url pattern.

        This is used only when OCSP cache server is updated.
        """
        if use_ocsp_cache_server is not None:
            self.CACHE_SERVER_ENABLED = use_ocsp_cache_server

        if self.CACHE_SERVER_ENABLED:
            logger.debug(
                "OCSP response cache server is enabled: %s", self.CACHE_SERVER_URL
            )
        else:
            logger.debug("OCSP response cache server is disabled")

        if self.OCSP_RETRY_URL is None:
            if self.CACHE_SERVER_URL is not None and (
                not self.CACHE_SERVER_URL.startswith(self.DEFAULT_CACHE_SERVER_URL)
            ):
                # only if custom OCSP cache server is used.
                parsed_url = urlsplit(self.CACHE_SERVER_URL)
                self.OCSP_RETRY_URL = f"{urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', ''))}/retry/{{0}}/{{1}}"
        logger.debug("OCSP dynamic cache server RETRY URL: %s", self.OCSP_RETRY_URL)

    def download_cache_from_server(self, ocsp):
        if self.CACHE_SERVER_ENABLED:
            # if any of them is not cache, download the cache file from
            # OCSP response cache server.
            try:
                retval = OCSPServer._download_ocsp_response_cache(
                    ocsp, self.CACHE_SERVER_URL
                )
                if not retval:
                    raise RevocationCheckError(
                        msg="OCSP Cache Server Unavailable.",
                        errno=ER_OCSP_RESPONSE_CACHE_DOWNLOAD_FAILED,
                    )
                logger.debug(
                    "downloaded OCSP response cache file from %s", self.CACHE_SERVER_URL
                )
                # len(OCSP_RESPONSE_VALIDATION_CACHE) is thread-safe, however, we do not want to
                # block for logging purpose, thus using len(OCSP_RESPONSE_VALIDATION_CACHE._cache) here.
                logger.debug(
                    "# of certificates: %s", len(OCSP_RESPONSE_VALIDATION_CACHE._cache)
                )
            except RevocationCheckError as rce:
                logger.debug(
                    "OCSP Response cache download failed. The client"
                    "will reach out to the OCSP Responder directly for"
                    "any missing OCSP responses %s\n" % rce.msg
                )
                raise

    @staticmethod
    def _download_ocsp_response_cache(ocsp, url, do_retry=True):
        """Downloads OCSP response cache from the cache server."""
        headers = {HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT}
        sf_timeout = SnowflakeOCSP.OCSP_CACHE_SERVER_CONNECTION_TIMEOUT

        try:
            start_time = time.time()
            logger.debug("started downloading OCSP response cache file: %s", url)

            if ocsp.test_mode is not None:
                test_timeout = os.getenv(
                    "SF_TEST_OCSP_CACHE_SERVER_CONNECTION_TIMEOUT", None
                )
                sf_cache_server_url = os.getenv("SF_TEST_OCSP_CACHE_SERVER_URL", None)
                if test_timeout is not None:
                    sf_timeout = int(test_timeout)
                if sf_cache_server_url is not None:
                    url = sf_cache_server_url

            with generic_requests.Session() as session:
                max_retry = SnowflakeOCSP.OCSP_CACHE_SERVER_MAX_RETRY if do_retry else 1
                sleep_time = 1
                backoff = DecorrelateJitterBackoff(sleep_time, 16)
                for _ in range(max_retry):
                    response = session.get(
                        url,
                        timeout=sf_timeout,  # socket timeout
                        headers=headers,
                    )
                    if response.status_code == OK:
                        ocsp.decode_ocsp_response_cache(response.json())
                        elapsed_time = time.time() - start_time
                        logger.debug(
                            "ended downloading OCSP response cache file. "
                            "elapsed time: %ss",
                            elapsed_time,
                        )
                        break
                    elif max_retry > 1:
                        sleep_time = backoff.next_sleep(1, sleep_time)
                        logger.debug(
                            "OCSP server returned %s. Retrying in %s(s)",
                            response.status_code,
                            sleep_time,
                        )
                    time.sleep(sleep_time)
                else:
                    logger.error(
                        "Failed to get OCSP response after %s attempt.", max_retry
                    )
                    return False
                return True
        except Exception as e:
            logger.debug("Failed to get OCSP response cache from %s: %s", url, e)
            raise RevocationCheckError(
                msg=f"Failed to get OCSP Response Cache from {url}: {e}",
                errno=ER_OCSP_FAILED_TO_CONNECT_CACHE_SERVER,
            )

    def generate_get_url(self, ocsp_url, b64data):
        parsed_url = urlsplit(ocsp_url)
        if self.OCSP_RETRY_URL is None:
            target_url = f"{ocsp_url}/{b64data}"
        else:
            target_url = self.OCSP_RETRY_URL.format(parsed_url.hostname, b64data)

        logger.debug("OCSP Retry URL is - %s", target_url)
        return target_url


class OCSPCache:

    # OCSP cache lock
    CACHE_LOCK = Lock()

    # OCSP cache update flag
    CACHE_UPDATED = False

    # Cache Expiration in seconds (120 hours). OCSP validation cache is
    # invalidated every 120 hours (5 days)
    CACHE_EXPIRATION = 432000

    # OCSP Response Cache URI
    OCSP_RESPONSE_CACHE_URI = None

    # OCSP response cache file name
    OCSP_RESPONSE_CACHE_FILE_NAME = "ocsp_response_cache.json"

    # Cache directory
    CACHE_DIR = None

    @staticmethod
    def reset_cache_dir():
        # Cache directory
        OCSPCache.CACHE_DIR = os.getenv("SF_OCSP_RESPONSE_CACHE_DIR")
        if OCSPCache.CACHE_DIR is None:
            cache_root_dir = expanduser("~") or tempfile.gettempdir()
            if platform.system() == "Windows":
                OCSPCache.CACHE_DIR = path.join(
                    cache_root_dir, "AppData", "Local", "Snowflake", "Caches"
                )
            elif platform.system() == "Darwin":
                OCSPCache.CACHE_DIR = path.join(
                    cache_root_dir, "Library", "Caches", "Snowflake"
                )
            else:
                OCSPCache.CACHE_DIR = path.join(cache_root_dir, ".cache", "snowflake")
        logger.debug("cache directory: %s", OCSPCache.CACHE_DIR)

        if not path.exists(OCSPCache.CACHE_DIR):
            try:
                os.makedirs(OCSPCache.CACHE_DIR, mode=0o700)
            except Exception as ex:
                logger.debug(
                    "cannot create a cache directory: [%s], err=[%s]",
                    OCSPCache.CACHE_DIR,
                    ex,
                )
                OCSPCache.CACHE_DIR = None

    @staticmethod
    def del_cache_file():
        """Deletes the OCSP response cache file if exists."""
        cache_file = path.join(
            OCSPCache.CACHE_DIR, OCSPCache.OCSP_RESPONSE_CACHE_FILE_NAME
        )
        if path.exists(cache_file):
            logger.debug(f"deleting cache file {cache_file}")
            os.unlink(cache_file)

    @staticmethod
    def reset_ocsp_response_cache_uri(ocsp_response_cache_uri):
        if ocsp_response_cache_uri is None and OCSPCache.CACHE_DIR is not None:
            OCSPCache.OCSP_RESPONSE_CACHE_URI = "file://" + path.join(
                OCSPCache.CACHE_DIR, OCSPCache.OCSP_RESPONSE_CACHE_FILE_NAME
            )
        else:
            OCSPCache.OCSP_RESPONSE_CACHE_URI = ocsp_response_cache_uri

        if OCSPCache.OCSP_RESPONSE_CACHE_URI is not None:
            # normalize URI for Windows
            OCSPCache.OCSP_RESPONSE_CACHE_URI = (
                OCSPCache.OCSP_RESPONSE_CACHE_URI.replace("\\", "/")
            )

        logger.debug("ocsp_response_cache_uri: %s", OCSPCache.OCSP_RESPONSE_CACHE_URI)
        # len(OCSP_RESPONSE_VALIDATION_CACHE) is thread-safe, however, we do not want to
        # block for logging purpose, thus using len(OCSP_RESPONSE_VALIDATION_CACHE._cache) here.
        logger.debug(
            "OCSP_VALIDATION_CACHE size: %s", len(OCSP_RESPONSE_VALIDATION_CACHE._cache)
        )

    @staticmethod
    def read_file(ocsp):
        """Reads OCSP Response cache data from the URI, which is very likely a file."""
        try:
            parsed_url = urlsplit(OCSPCache.OCSP_RESPONSE_CACHE_URI)
            if parsed_url.scheme == "file":
                OCSPCache.read_ocsp_response_cache_file(
                    ocsp, path.join(parsed_url.netloc, parsed_url.path)
                )
            else:
                msg = "Unsupported OCSP URI: {}".format(
                    OCSPCache.OCSP_RESPONSE_CACHE_URI
                )
                raise Exception(msg)
        except (RevocationCheckError, Exception) as rce:
            logger.debug(
                "Failed to read OCSP response cache file %s: %s, "
                "No worry. It will validate with OCSP server. "
                "Ignoring...",
                OCSPCache.OCSP_RESPONSE_CACHE_URI,
                rce,
                exc_info=True,
            )

    @staticmethod
    def read_ocsp_response_cache_file(ocsp, filename):
        """Reads OCSP Response cache."""
        try:
            if OCSPCache.check_ocsp_response_cache_lock_dir(filename) and path.exists(
                filename
            ):
                with codecs.open(filename, "r", encoding="utf-8", errors="ignore") as f:
                    ocsp.decode_ocsp_response_cache(json.load(f))
                # len(OCSP_RESPONSE_VALIDATION_CACHE) is thread-safe, however, we do not want to
                # block for logging purpose, thus using len(OCSP_RESPONSE_VALIDATION_CACHE._cache) here.
                logger.debug(
                    "Read OCSP response cache file: %s, count=%s",
                    filename,
                    len(OCSP_RESPONSE_VALIDATION_CACHE._cache),
                )
            else:
                logger.debug(
                    "Failed to locate OCSP response cache file. "
                    "No worry. It will validate with OCSP server: %s",
                    filename,
                )
        except Exception as ex:
            logger.debug("Caught - %s", ex)
            raise ex

    @staticmethod
    def update_file(ocsp):
        """
        Updates OCSP Response Cache file.
        Two file shall be updated/saved:
            1. file for OCSP_RESPONSE_VALIDATION_CACHE which keeps ocsp response validation result
            2. ocsp_response_cache.json, the file in the same format as the one downloaded from snowflake cache service
        """
        if OCSPCache.CACHE_UPDATED:
            if isinstance(OCSP_RESPONSE_VALIDATION_CACHE, SFDictFileCache):
                OCSP_RESPONSE_VALIDATION_CACHE._save()
            OCSPCache.update_ocsp_response_cache_file(
                ocsp, OCSPCache.OCSP_RESPONSE_CACHE_URI
            )
            OCSPCache.CACHE_UPDATED = False

    @staticmethod
    def update_ocsp_response_cache_file(ocsp, ocsp_response_cache_uri):
        """Updates OCSP Response Cache."""
        if ocsp_response_cache_uri is not None:
            try:
                parsed_url = urlsplit(ocsp_response_cache_uri)
                if parsed_url.scheme == "file":
                    filename = path.join(parsed_url.netloc, parsed_url.path)
                    lock_dir = filename + ".lck"
                    for _ in range(100):
                        # wait until the lck file has been removed
                        # or up to 1 second (0.01 x 100)
                        if OCSPCache.lock_cache_file(lock_dir):
                            break
                        time.sleep(0.01)
                    try:
                        OCSPCache.write_ocsp_response_cache_file(ocsp, filename)
                    finally:
                        OCSPCache.unlock_cache_file(lock_dir)
                else:
                    logger.debug(
                        "No OCSP response cache file is written, because the "
                        "given URI is not a file: %s. Ignoring...",
                        ocsp_response_cache_uri,
                    )
            except Exception as e:
                logger.debug(
                    "Failed to write OCSP response cache "
                    "file. file: %s, error: %s, Ignoring...",
                    ocsp_response_cache_uri,
                    e,
                    exc_info=True,
                )

    @staticmethod
    def write_ocsp_response_cache_file(ocsp, filename):
        """Writes OCSP Response Cache."""
        logger.debug(f"writing OCSP response cache file to {filename}")
        file_cache_data = {}
        ocsp.encode_ocsp_response_cache(file_cache_data)
        with codecs.open(filename, "w", encoding="utf-8", errors="ignore") as f:
            json.dump(file_cache_data, f)

    @staticmethod
    def check_ocsp_response_cache_lock_dir(filename):
        """Checks if the lock directory exists.

        Returns:
            True if it can update the cache file or False when some other process may be updating the cache file.
        """
        current_time = int(time.time())
        lock_dir = filename + ".lck"

        try:
            ts_cache_file = OCSPCache._file_timestamp(filename)
            if (
                not path.exists(lock_dir)
                and current_time - OCSPCache.CACHE_EXPIRATION <= ts_cache_file
            ):
                # use cache only if no lock directory exists and the cache file
                # was created last 24 hours
                return True

            if path.exists(lock_dir):
                # delete lock directory if older 60 seconds
                ts_lock_dir = OCSPCache._file_timestamp(lock_dir)
                if ts_lock_dir < current_time - 60:
                    OCSPCache.unlock_cache_file(lock_dir)
                    logger.debug(
                        "The lock directory is older than 60 seconds. "
                        "Deleted the lock directory and ignoring the cache: %s",
                        lock_dir,
                    )
                else:
                    logger.debug(
                        "The lock directory exists. Other process may be "
                        "updating the cache file: %s, %s",
                        filename,
                        lock_dir,
                    )
            else:
                os.unlink(filename)
                logger.debug(
                    "The cache is older than 1 day. " "Deleted the cache file: %s",
                    filename,
                )
        except Exception as e:
            logger.debug(
                "Failed to check OCSP response cache file. No worry. It will "
                "validate with OCSP server: file: %s, lock directory: %s, "
                "error: %s",
                filename,
                lock_dir,
                e,
            )
        return False

    @staticmethod
    def is_cache_fresh(current_time, ts):
        return current_time - OCSPCache.CACHE_EXPIRATION <= ts

    @staticmethod
    def find_cache(
        ocsp: SnowflakeOCSP, cert_id: CertId, subject: Certificate | None, **kwargs: Any
    ) -> tuple[bool, bytes | None]:
        subject_name = ocsp.subject_name(subject) if subject else None
        current_time = int(time.time())
        cache_key: tuple[bytes, bytes, bytes] = kwargs.get(
            "cache_key", ocsp.decode_cert_id_key(cert_id)
        )
        lock_cache: bool = kwargs.get("lock_cache", True)
        try:
            ocsp_response_validation_result = (
                OCSP_RESPONSE_VALIDATION_CACHE[cache_key]
                if lock_cache
                else OCSP_RESPONSE_VALIDATION_CACHE._getitem_non_locking(cache_key)
            )
            try:
                # is_valid_time can raise exception if the cache
                # entry is a SSD.
                if OCSPCache.is_cache_fresh(
                    current_time, ocsp_response_validation_result.ts
                ) and ocsp.is_valid_time(
                    cert_id, ocsp_response_validation_result.ocsp_response
                ):
                    if subject_name:
                        logger.debug("hit cache for subject: %s", subject_name)
                    return True, ocsp_response_validation_result.ocsp_response
                else:
                    OCSPCache.delete_cache(
                        ocsp, cert_id, cache_key=cache_key, lock_cache=lock_cache
                    )
            except Exception as ex:
                logger.debug(f"Could not validate cache entry {cert_id} {ex}")
            OCSPCache.CACHE_UPDATED = True
        except KeyError:
            if subject_name:
                logger.debug(f"cache miss for subject: '{subject_name}'")
        return False, None

    @staticmethod
    def delete_cache(ocsp: SnowflakeOCSP, cert_id: CertId, **kwargs: Any):
        cache_key: tuple[bytes, bytes, bytes] = kwargs.get(
            "cache_key", ocsp.decode_cert_id_key(cert_id)
        )
        lock_cache: bool = kwargs.get("lock_cache", True)
        try:
            if lock_cache:
                del OCSP_RESPONSE_VALIDATION_CACHE[cache_key]
            else:
                OCSP_RESPONSE_VALIDATION_CACHE._delitem(cache_key)
            OCSPCache.CACHE_UPDATED = True
        except KeyError:
            pass

    @staticmethod
    def _file_timestamp(filename):
        """Gets the last created timestamp of the file/dir."""
        if platform.system() == "Windows":
            ts = int(path.getctime(filename))
        else:
            stat = os.stat(filename)
            if hasattr(stat, "st_birthtime"):  # odx
                ts = int(stat.st_birthtime)
            else:
                ts = int(stat.st_mtime)  # linux
        return ts

    @staticmethod
    def lock_cache_file(fname):
        """Locks a cache file by creating a directory."""
        try:
            os.mkdir(fname)
            return True
        except OSError:
            return False

    @staticmethod
    def unlock_cache_file(fname):
        """Unlocks a cache file by deleting a directory."""
        try:
            os.rmdir(fname)
            return True
        except OSError:
            return False

    @staticmethod
    def delete_cache_file():
        """Deletes the cache file. Used by tests only."""
        parsed_url = urlsplit(OCSPCache.OCSP_RESPONSE_CACHE_URI)
        fname = path.join(parsed_url.netloc, parsed_url.path)
        OCSPCache.lock_cache_file(fname)
        try:
            logger.debug(f"deleting cache file, used by tests only {fname}")
            os.unlink(fname)
        finally:
            OCSPCache.unlock_cache_file(fname)

    @staticmethod
    def clear_cache():
        """Clears cache."""
        OCSP_RESPONSE_VALIDATION_CACHE.clear()

    @staticmethod
    def cache_size():
        """Returns the cache's size."""
        return len(OCSP_RESPONSE_VALIDATION_CACHE)


# Reset OCSP cache directory
OCSPCache.reset_cache_dir()


class SnowflakeOCSP:
    """OCSP validator using PyOpenSSL and asn1crypto/pyasn1."""

    # root certificate cache
    ROOT_CERTIFICATES_DICT = {}  # root certificates

    # root certificate cache lock
    ROOT_CERTIFICATES_DICT_LOCK = RLock()

    # cache object
    OCSP_CACHE = OCSPCache()

    OCSP_WHITELIST = re.compile(
        r"^"
        r"(.*\.snowflakecomputing\.com$"
        r"|(?:|.*\.)s3.*\.amazonaws\.com$"  # start with s3 or .s3 in the middle
        r"|.*\.okta\.com$"
        r"|(?:|.*\.)storage\.googleapis\.com$"
        r"|.*\.blob\.core\.windows\.net$"
        r"|.*\.blob\.core\.usgovcloudapi\.net$)"
    )

    # Tolerable validity date range ratio. The OCSP response is valid up
    # to (next update timestamp) + (next update timestamp -
    # this update timestamp) * TOLERABLE_VALIDITY_RANGE_RATIO. This buffer
    # yields some time for Root CA to update intermediate CA's certificate
    # OCSP response. In fact, they don't update OCSP response in time. In Dec
    # 2016, they left OCSP response expires for 5 hours at least, and it
    # caused the connectivity issues in customers.
    # With this buffer, about 2 days are given for 180 days validity date.
    TOLERABLE_VALIDITY_RANGE_RATIO = 0.01

    # Maximum clock skew in seconds (15 minutes) allowed when checking
    # validity of OCSP responses
    MAX_CLOCK_SKEW = 900

    # Epoch time
    ZERO_EPOCH = datetime.utcfromtimestamp(0)

    # Timestamp format for logging
    OUTPUT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%SZ"

    # Connection timeout in seconds for CA OCSP Responder
    CA_OCSP_RESPONDER_CONNECTION_TIMEOUT = 10

    # Connection timeout in seconds for Cache Server
    OCSP_CACHE_SERVER_CONNECTION_TIMEOUT = 5

    # MAX number of connection retry attempts with Responder in Fail Open
    CA_OCSP_RESPONDER_MAX_RETRY_FO = 1

    # MAX number of connection retry attempts with Responder in Fail Close
    CA_OCSP_RESPONDER_MAX_RETRY_FC = 3

    # MAX number of connection retry attempts with Cache Server
    OCSP_CACHE_SERVER_MAX_RETRY = 1

    def __init__(
        self,
        ocsp_response_cache_uri=None,
        use_ocsp_cache_server=None,
        use_post_method=True,
        use_fail_open=True,
    ):

        self.test_mode = os.getenv("SF_OCSP_TEST_MODE", None)

        if self.test_mode == "true":
            logger.debug("WARNING - DRIVER CONFIGURED IN TEST MODE")

        self._use_post_method = use_post_method
        self.OCSP_CACHE_SERVER = OCSPServer()

        self.debug_ocsp_failure_url = None

        if os.getenv("SF_OCSP_FAIL_OPEN") is not None:
            # failOpen Env Variable is for internal usage/ testing only.
            # Using it in production is not advised and not supported.
            self.FAIL_OPEN = os.getenv("SF_OCSP_FAIL_OPEN").lower() == "true"
        else:
            self.FAIL_OPEN = use_fail_open

        SnowflakeOCSP.OCSP_CACHE.reset_ocsp_response_cache_uri(ocsp_response_cache_uri)

        if not OCSPServer.is_enabled_new_ocsp_endpoint():
            self.OCSP_CACHE_SERVER.reset_ocsp_dynamic_cache_server_url(
                use_ocsp_cache_server
            )

        """
        Here we have a two-layer cache design:

        The upper layer is the OCSP_RESPONSE_VALIDATION_CACHE which caches not only the ocsp responses but also
        the validation result of ocsp responses. This will be both in-memory and in-file (if program has the right
        to read and write files).

        The bottom layer is the ocsp responses in the form of a json file which are either
        retrieved from Snowflake cache service or locally maintained by writing OCSP_RESPONSE_VALIDATION_CACHE back
        to the json file for any updates (certificate revoked, cache expired, etc.). This will be in-file.

        The cache logic is as following:
        1. The OCSP_RESPONSE_VALIDATION_CACHE will be loaded from disk first during module loading period.
        2. If there's no content loaded either due to no cache file or all cache expired, then we try load ocsp
         response cache file. We will parse the content in the ocsp response cache json file, and
         then update OCSP_RESPONSE_VALIDATION_CACHE.
        3. When validating certs, we will first check OCSP_RESPONSE_VALIDATION_CACHE, if cache is not found,
         when we will validate against the OCSP servers and cache the results.
        4. After validating all the certs, we save OCSP_RESPONSE_VALIDATION_CACHE and ocsp response json
         onto disk.
        """
        if not OCSP_RESPONSE_VALIDATION_CACHE:
            SnowflakeOCSP.OCSP_CACHE.read_file(self)

    def validate_certfile(self, cert_filename, no_exception=False):
        """Validates that the certificate is NOT revoked."""
        cert_map = {}
        telemetry_data = OCSPTelemetryData()
        telemetry_data.set_cache_enabled(self.OCSP_CACHE_SERVER.CACHE_SERVER_ENABLED)
        telemetry_data.set_insecure_mode(False)
        telemetry_data.set_sfc_peer_host(cert_filename)
        telemetry_data.set_fail_open(self.is_enabled_fail_open())
        try:
            self.read_cert_bundle(cert_filename, cert_map)
            cert_data = self.create_pair_issuer_subject(cert_map)
        except Exception as ex:
            logger.debug("Caught exception while validating certfile %s", str(ex))
            raise ex

        return self._validate(
            None, cert_data, telemetry_data, do_retry=False, no_exception=no_exception
        )

    def validate(
        self,
        hostname: str | None,
        connection: Connection,
        no_exception: bool = False,
    ) -> list[
        tuple[
            Exception | None,
            Certificate,
            Certificate,
            CertId,
            str | bytes,
        ]
    ] | None:
        """Validates the certificate is not revoked using OCSP."""
        logger.debug("validating certificate: %s", hostname)

        do_retry = SnowflakeOCSP.get_ocsp_retry_choice()

        m = not SnowflakeOCSP.OCSP_WHITELIST.match(hostname)
        if m or hostname.startswith("ocspssd"):
            logger.debug("skipping OCSP check: %s", hostname)
            return [None, None, None, None, None]

        if OCSPServer.is_enabled_new_ocsp_endpoint():
            self.OCSP_CACHE_SERVER.reset_ocsp_endpoint(hostname)

        telemetry_data = OCSPTelemetryData()
        telemetry_data.set_cache_enabled(self.OCSP_CACHE_SERVER.CACHE_SERVER_ENABLED)
        telemetry_data.set_insecure_mode(False)
        telemetry_data.set_sfc_peer_host(hostname)
        telemetry_data.set_fail_open(self.is_enabled_fail_open())

        try:
            cert_data = self.extract_certificate_chain(connection)
        except RevocationCheckError:
            telemetry_data.set_event_sub_type(
                OCSPTelemetryData.CERTIFICATE_EXTRACTION_FAILED
            )
            logger.debug(
                telemetry_data.generate_telemetry_data("RevocationCheckFailure")
            )
            return None

        return self._validate(
            hostname, cert_data, telemetry_data, do_retry, no_exception
        )

    def _validate(
        self,
        hostname: str | None,
        cert_data: list[tuple[Certificate, Certificate]],
        telemetry_data: OCSPTelemetryData,
        do_retry: bool = True,
        no_exception: bool = False,
    ) -> list[tuple[Exception | None, Certificate, Certificate, CertId, bytes]]:
        """Validate certs sequentially if OCSP response cache server is used."""
        results = self._validate_certificates_sequential(
            cert_data, telemetry_data, hostname, do_retry=do_retry
        )

        SnowflakeOCSP.OCSP_CACHE.update_file(self)

        any_err = False
        for err, _, _, _, _ in results:
            if isinstance(err, RevocationCheckError):
                err.msg += f" for {hostname}"
            if not no_exception and err is not None:
                raise err
            elif err is not None:
                any_err = True

        logger.debug("ok" if not any_err else "failed")
        return results

    @staticmethod
    def get_ocsp_retry_choice():
        return os.getenv("SF_OCSP_DO_RETRY", "true") == "true"

    def is_cert_id_in_cache(
        self, cert_id: CertId, subject: Certificate | None, **kwargs: Any
    ):
        """Decides whether OCSP CertID is in cache.

        Args:
            cert_id: OCSP CertID.
            subject: Subject certificate.

        Returns:
            True if in cache otherwise False, followed by the cached OCSP Response.
        """
        found, cache = SnowflakeOCSP.OCSP_CACHE.find_cache(
            self, cert_id, subject, **kwargs
        )
        return found, cache

    def get_account_from_hostname(self, hostname: str) -> str:
        """Extracts the account name part from the hostname.

        Args:
            hostname: Hostname that account name is in.

        Returns:
            The extracted account name.
        """
        split_hname = hostname.split(".")
        if "global" in split_hname:
            acc_name = split_hname[0].split("-")[0]
        else:
            acc_name = split_hname[0]
        return acc_name

    def is_enabled_fail_open(self):
        return self.FAIL_OPEN

    @staticmethod
    def print_fail_open_warning(ocsp_log):
        static_warning = (
            "WARNING!!! Using fail-open to connect. Driver is connecting to an "
            "HTTPS endpoint without OCSP based Certificate Revocation checking "
            "as it could not obtain a valid OCSP Response to use from the CA OCSP "
            "responder. Details:"
        )
        ocsp_warning = f"{static_warning} \n {ocsp_log}"
        logger.error(ocsp_warning)

    def validate_by_direct_connection(
        self,
        issuer: Certificate,
        subject: Certificate,
        telemetry_data: OCSPTelemetryData,
        hostname: str = None,
        do_retry: bool = True,
        **kwargs: Any,
    ) -> tuple[Exception | None, Certificate, Certificate, CertId, bytes]:
        cert_id, req = self.create_ocsp_request(issuer, subject)
        cache_status, ocsp_response = self.is_cert_id_in_cache(
            cert_id, subject, **kwargs
        )

        try:
            if not cache_status:
                telemetry_data.set_cache_hit(False)
                logger.debug("getting OCSP response from CA's OCSP server")
                ocsp_response = self._fetch_ocsp_response(
                    req, subject, cert_id, telemetry_data, hostname, do_retry
                )
            else:
                ocsp_url = self.extract_ocsp_url(subject)
                cert_id_enc = self.encode_cert_id_base64(
                    self.decode_cert_id_key(cert_id)
                )
                telemetry_data.set_cache_hit(True)
                self.debug_ocsp_failure_url = SnowflakeOCSP.create_ocsp_debug_info(
                    self, req, ocsp_url
                )
                telemetry_data.set_ocsp_url(ocsp_url)
                telemetry_data.set_ocsp_req(req)
                telemetry_data.set_cert_id(cert_id_enc)
                logger.debug("using OCSP response cache")

            if not ocsp_response:
                telemetry_data.set_event_sub_type(
                    OCSPTelemetryData.OCSP_RESPONSE_UNAVAILABLE
                )
                raise RevocationCheckError(
                    msg="Could not retrieve OCSP Response. Cannot perform Revocation Check",
                    errno=ER_OCSP_RESPONSE_UNAVAILABLE,
                )
            try:
                self.process_ocsp_response(issuer, cert_id, ocsp_response)
                err = None
            except RevocationCheckError as op_er:
                telemetry_data.set_event_sub_type(
                    OCSPTelemetryData.ERROR_CODE_MAP[op_er.errno]
                )
                raise op_er

        except RevocationCheckError as rce:
            telemetry_data.set_error_msg(rce.msg)
            err = self.verify_fail_open(rce, telemetry_data)

        except Exception as ex:
            logger.debug("OCSP Validation failed %s", str(ex))
            telemetry_data.set_error_msg(str(ex))
            err = self.verify_fail_open(ex, telemetry_data)
            SnowflakeOCSP.OCSP_CACHE.delete_cache(self, cert_id)

        return err, issuer, subject, cert_id, ocsp_response

    def verify_fail_open(self, ex_obj, telemetry_data):
        if not self.is_enabled_fail_open():
            if ex_obj.errno is ER_OCSP_RESPONSE_CERT_STATUS_REVOKED:
                logger.debug(
                    telemetry_data.generate_telemetry_data(
                        "RevokedCertificateError", True
                    )
                )
            else:
                logger.debug(
                    telemetry_data.generate_telemetry_data("RevocationCheckFailure")
                )
            return ex_obj
        else:
            if ex_obj.errno is ER_OCSP_RESPONSE_CERT_STATUS_REVOKED:
                logger.debug(
                    telemetry_data.generate_telemetry_data(
                        "RevokedCertificateError", True
                    )
                )
                return ex_obj
            else:
                SnowflakeOCSP.print_fail_open_warning(
                    telemetry_data.generate_telemetry_data("RevocationCheckFailure")
                )
                return None

    def _validate_certificates_sequential(
        self,
        cert_data: list[tuple[Certificate, Certificate]],
        telemetry_data: OCSPTelemetryData,
        hostname: str | None = None,
        do_retry: bool = True,
    ) -> list[tuple[Exception | None, Certificate, Certificate, CertId, bytes]]:
        results = []
        try:
            self._check_ocsp_response_cache_server(cert_data)
        except RevocationCheckError as rce:
            telemetry_data.set_event_sub_type(
                OCSPTelemetryData.ERROR_CODE_MAP[rce.errno]
            )
        except Exception as ex:
            logger.debug(
                "Caught unknown exception - %s. Continue to validate by direct connection",
                str(ex),
            )

        for issuer, subject in cert_data:
            cert_id, _ = self.create_ocsp_request(issuer=issuer, subject=subject)
            cache_key = self.decode_cert_id_key(cert_id)
            ocsp_response_validation_result = OCSP_RESPONSE_VALIDATION_CACHE.get(
                cache_key
            )
            if (
                ocsp_response_validation_result is None
                or not ocsp_response_validation_result.validated
            ):
                r = self.validate_by_direct_connection(
                    issuer,
                    subject,
                    telemetry_data,
                    hostname,
                    do_retry=do_retry,
                    cache_key=cache_key,
                )
                OCSP_RESPONSE_VALIDATION_CACHE[
                    cache_key
                ] = OCSPResponseValidationResult(
                    *r,
                    ts=int(time.time()),
                    validated=True,
                )
                OCSPCache.CACHE_UPDATED = True
                results.append(r)
            else:
                results.append(
                    (
                        ocsp_response_validation_result.exception,
                        ocsp_response_validation_result.issuer,
                        ocsp_response_validation_result.subject,
                        ocsp_response_validation_result.cert_id,
                        ocsp_response_validation_result.ocsp_response,
                    )
                )
        return results

    def _check_ocsp_response_cache_server(
        self,
        cert_data: list[tuple[Certificate, Certificate]],
    ) -> None:
        """Checks if OCSP response is in cache, and if not it downloads the OCSP response cache from the server.

        Args:
          cert_data: Tuple of issuer and subject certificates.
        """
        in_cache = False
        for issuer, subject in cert_data:
            # check if any OCSP response is NOT in cache
            cert_id, _ = self.create_ocsp_request(issuer, subject)
            in_cache, _ = SnowflakeOCSP.OCSP_CACHE.find_cache(self, cert_id, subject)
            if not in_cache:
                # not found any
                break

        if not in_cache:
            self.OCSP_CACHE_SERVER.download_cache_from_server(self)

    def _lazy_read_ca_bundle(self):
        """Reads the local cabundle file and cache it in memory."""
        with SnowflakeOCSP.ROOT_CERTIFICATES_DICT_LOCK:
            if SnowflakeOCSP.ROOT_CERTIFICATES_DICT:
                # return if already loaded
                return

            try:
                ca_bundle = environ.get("REQUESTS_CA_BUNDLE") or environ.get(
                    "CURL_CA_BUNDLE"
                )
                if ca_bundle and path.exists(ca_bundle):
                    # if the user/application specifies cabundle.
                    self.read_cert_bundle(ca_bundle)
                else:
                    import sys

                    # This import that depends on these libraries is to import certificates from them,
                    # we would like to have these as up to date as possible.

                    # forcing YA specific wark around
                    # from requests import certs

                    if (
                        hasattr(certs, "__file__")
                        and path.exists(certs.__file__)
                        and path.exists(
                            path.join(path.dirname(certs.__file__), "cacert.pem")
                        )
                    ):
                        # if cacert.pem exists next to certs.py in request
                        # package.
                        ca_bundle = path.join(
                            path.dirname(certs.__file__), "cacert.pem"
                        )
                        self.read_cert_bundle(ca_bundle)
                    elif hasattr(sys, "_MEIPASS"):
                        # if pyinstaller includes cacert.pem
                        cabundle_candidates = [
                            ["botocore", "vendored", "requests", "cacert.pem"],
                            ["requests", "cacert.pem"],
                            ["cacert.pem"],
                        ]
                        for filename in cabundle_candidates:
                            ca_bundle = path.join(sys._MEIPASS, *filename)
                            if path.exists(ca_bundle):
                                self.read_cert_bundle(ca_bundle)
                                break
                        else:
                            logger.error("No cabundle file is found in _MEIPASS")
                    try:
                        import certifi

                        # self.read_cert_bundle(certifi.where())
                        self.read_cert_bundle(certs.where())
                    except Exception:
                        logger.debug("no certifi is installed. ignored.")

            except Exception as e:
                logger.error("Failed to read ca_bundle: %s", e)

            if not SnowflakeOCSP.ROOT_CERTIFICATES_DICT:
                logger.error(
                    "No CA bundle file is found in the system. "
                    "Set REQUESTS_CA_BUNDLE to the file."
                )

    @staticmethod
    def _calculate_tolerable_validity(this_update, next_update):
        return max(
            int(
                SnowflakeOCSP.TOLERABLE_VALIDITY_RANGE_RATIO
                * (next_update - this_update)
            ),
            SnowflakeOCSP.MAX_CLOCK_SKEW,
        )

    @staticmethod
    def _is_validaity_range(current_time, this_update, next_update, test_mode=None):
        if test_mode is not None:
            force_validity_fail = os.getenv("SF_TEST_OCSP_FORCE_BAD_RESPONSE_VALIDITY")
            if force_validity_fail is not None:
                return False

        tolerable_validity = SnowflakeOCSP._calculate_tolerable_validity(
            this_update, next_update
        )
        return (
            this_update - SnowflakeOCSP.MAX_CLOCK_SKEW
            <= current_time
            <= next_update + tolerable_validity
        )

    @staticmethod
    def _validity_error_message(current_time, this_update, next_update):
        tolerable_validity = SnowflakeOCSP._calculate_tolerable_validity(
            this_update, next_update
        )
        return (
            "Response is unreliable. Its validity "
            "date is out of range: current_time={}, "
            "this_update={}, next_update={}, "
            "tolerable next_update={}. A potential cause is "
            "client clock is skewed, CA fails to update OCSP "
            "response in time.".format(
                strftime(SnowflakeOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(current_time)),
                strftime(SnowflakeOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(this_update)),
                strftime(SnowflakeOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(next_update)),
                strftime(
                    SnowflakeOCSP.OUTPUT_TIMESTAMP_FORMAT,
                    gmtime(next_update + tolerable_validity),
                ),
            )
        )

    @staticmethod
    def clear_cache():
        SnowflakeOCSP.OCSP_CACHE.clear_cache()

    @staticmethod
    def cache_size():
        return SnowflakeOCSP.OCSP_CACHE.cache_size()

    @staticmethod
    def delete_cache_file():
        SnowflakeOCSP.OCSP_CACHE.delete_cache_file()

    @staticmethod
    def create_ocsp_debug_info(ocsp, ocsp_request, ocsp_url):
        b64data = ocsp.decode_ocsp_request_b64(ocsp_request)
        target_url = f"{ocsp_url}/{b64data}"
        return target_url

    def _fetch_ocsp_response(
        self,
        ocsp_request,
        subject,
        cert_id,
        telemetry_data,
        hostname=None,
        do_retry=True,
    ):
        """Fetches OCSP response using OCSPRequest."""
        sf_timeout = SnowflakeOCSP.CA_OCSP_RESPONDER_CONNECTION_TIMEOUT
        ocsp_url = self.extract_ocsp_url(subject)
        cert_id_enc = self.encode_cert_id_base64(self.decode_cert_id_key(cert_id))
        if not ocsp_url:
            telemetry_data.set_event_sub_type(OCSPTelemetryData.OCSP_URL_MISSING)
            raise RevocationCheckError(
                msg="No OCSP URL found in cert. Cannot perform Certificate Revocation check",
                errno=ER_OCSP_URL_INFO_MISSING,
            )
        headers = {HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT}

        if not OCSPServer.is_enabled_new_ocsp_endpoint():
            actual_method = "post" if self._use_post_method else "get"
            if self.OCSP_CACHE_SERVER.OCSP_RETRY_URL:
                # no POST is supported for Retry URL at the moment.
                actual_method = "get"

            if actual_method == "get":
                b64data = self.decode_ocsp_request_b64(ocsp_request)
                target_url = self.OCSP_CACHE_SERVER.generate_get_url(ocsp_url, b64data)
                payload = None
            else:
                target_url = ocsp_url
                payload = self.decode_ocsp_request(ocsp_request)
                headers["Content-Type"] = "application/ocsp-request"
        else:
            actual_method = "post"
            target_url = self.OCSP_CACHE_SERVER.OCSP_RETRY_URL
            ocsp_req_enc = self.decode_ocsp_request_b64(ocsp_request)

            payload = json.dumps(
                {
                    "hostname": hostname,
                    "ocsp_request": ocsp_req_enc,
                    "cert_id": cert_id_enc,
                    "ocsp_responder_url": ocsp_url,
                }
            )
            headers["Content-Type"] = "application/json"

        telemetry_data.set_ocsp_connection_method(actual_method)
        if self.test_mode is not None:
            logger.debug("WARNING - DRIVER IS CONFIGURED IN TESTMODE.")
            test_ocsp_url = os.getenv("SF_TEST_OCSP_URL", None)
            test_timeout = os.getenv(
                "SF_TEST_CA_OCSP_RESPONDER_CONNECTION_TIMEOUT", None
            )
            if test_timeout is not None:
                sf_timeout = int(test_timeout)
            if test_ocsp_url is not None:
                target_url = test_ocsp_url

        self.debug_ocsp_failure_url = SnowflakeOCSP.create_ocsp_debug_info(
            self, ocsp_request, ocsp_url
        )
        telemetry_data.set_ocsp_req(self.decode_ocsp_request_b64(ocsp_request))
        telemetry_data.set_ocsp_url(ocsp_url)
        telemetry_data.set_cert_id(cert_id_enc)

        ret = None
        logger.debug("url: %s", target_url)
        sf_max_retry = SnowflakeOCSP.CA_OCSP_RESPONDER_MAX_RETRY_FO
        if not self.is_enabled_fail_open():
            sf_max_retry = SnowflakeOCSP.CA_OCSP_RESPONDER_MAX_RETRY_FC

        with generic_requests.Session() as session:
            max_retry = sf_max_retry if do_retry else 1
            sleep_time = 1
            backoff = DecorrelateJitterBackoff(sleep_time, 16)
            for _ in range(max_retry):
                try:
                    response = session.request(
                        headers=headers,
                        method=actual_method,
                        url=target_url,
                        timeout=sf_timeout,
                        data=payload,
                    )
                    if response.status_code == OK:
                        logger.debug(
                            "OCSP response was successfully returned from OCSP "
                            "server."
                        )
                        ret = response.content
                        break
                    elif max_retry > 1:
                        sleep_time = backoff.next_sleep(1, sleep_time)
                        logger.debug(
                            "OCSP server returned %s. Retrying in %s(s)",
                            response.status_code,
                            sleep_time,
                        )
                    time.sleep(sleep_time)
                except Exception as ex:
                    if max_retry > 1:
                        sleep_time = backoff.next_sleep(1, sleep_time)
                        logger.debug(
                            "Could not fetch OCSP Response from server"
                            "Retrying in %s(s)",
                            sleep_time,
                        )
                        time.sleep(sleep_time)
                    else:
                        telemetry_data.set_event_sub_type(
                            OCSPTelemetryData.OCSP_RESPONSE_FETCH_EXCEPTION
                        )
                        raise RevocationCheckError(
                            msg="Could not fetch OCSP Response from server. Consider"
                            "checking your whitelists : Exception - {}".format(str(ex)),
                            errno=ER_OCSP_RESPONSE_FETCH_EXCEPTION,
                        )
            else:
                logger.error(
                    "Failed to get OCSP response after {} attempt. Consider checking "
                    "for OCSP URLs being blocked".format(max_retry)
                )
                telemetry_data.set_event_sub_type(
                    OCSPTelemetryData.OCSP_RESPONSE_FETCH_FAILURE
                )
                raise RevocationCheckError(
                    msg="Failed to get OCSP response after {} attempt.".format(
                        max_retry
                    ),
                    errno=ER_OCSP_RESPONSE_FETCH_FAILURE,
                )

        return ret

    def _process_good_status(self, single_response, cert_id, ocsp_response):
        """Processes GOOD status."""
        current_time = int(time.time())
        this_update_native, next_update_native = self.extract_good_status(
            single_response
        )

        if this_update_native is None or next_update_native is None:
            raise RevocationCheckError(
                msg="Either this update or next "
                "update is None. this_update: {}, next_update: {}".format(
                    this_update_native, next_update_native
                ),
                errno=ER_OCSP_RESPONSE_INVALID_EXPIRY_INFO_MISSING,
            )

        this_update = (
            this_update_native.replace(tzinfo=None) - SnowflakeOCSP.ZERO_EPOCH
        ).total_seconds()
        next_update = (
            next_update_native.replace(tzinfo=None) - SnowflakeOCSP.ZERO_EPOCH
        ).total_seconds()
        if not SnowflakeOCSP._is_validaity_range(
            current_time, this_update, next_update, self.test_mode
        ):
            raise RevocationCheckError(
                msg=SnowflakeOCSP._validity_error_message(
                    current_time, this_update, next_update
                ),
                errno=ER_OCSP_RESPONSE_EXPIRED,
            )

    def _process_revoked_status(self, single_response, cert_id):
        """Processes REVOKED status."""
        current_time = int(time.time())
        if self.test_mode is not None:
            test_cert_status = os.getenv("SF_TEST_OCSP_CERT_STATUS")
            if test_cert_status == "revoked":
                raise RevocationCheckError(
                    msg="The certificate has been revoked: current_time={}, "
                    "revocation_time={}, reason={}".format(
                        strftime(
                            SnowflakeOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(current_time)
                        ),
                        strftime(
                            SnowflakeOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(current_time)
                        ),
                        "Force Revoke",
                    ),
                    errno=ER_OCSP_RESPONSE_CERT_STATUS_REVOKED,
                )

        SnowflakeOCSP.OCSP_CACHE.delete_cache(self, cert_id)
        revocation_time, revocation_reason = self.extract_revoked_status(
            single_response
        )
        raise RevocationCheckError(
            msg="The certificate has been revoked: current_time={}, "
            "revocation_time={}, reason={}".format(
                strftime(SnowflakeOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(current_time)),
                revocation_time.strftime(SnowflakeOCSP.OUTPUT_TIMESTAMP_FORMAT),
                revocation_reason,
            ),
            errno=ER_OCSP_RESPONSE_CERT_STATUS_REVOKED,
        )

    def _process_unknown_status(self, cert_id):
        """Processes UNKNOWN status."""
        SnowflakeOCSP.OCSP_CACHE.delete_cache(self, cert_id)
        raise RevocationCheckError(
            msg="The certificate is in UNKNOWN revocation status.",
            errno=ER_OCSP_RESPONSE_CERT_STATUS_UNKNOWN,
        )

    def decode_ocsp_response_cache(self, ocsp_response_cache_json):
        """Decodes OCSP response cache from JSON."""
        try:
            with OCSP_RESPONSE_VALIDATION_CACHE._lock:
                new_cache_dict = {}
                for cert_id_base64, (
                    ts,
                    ocsp_response,
                ) in ocsp_response_cache_json.items():
                    cert_id = self.decode_cert_id_base64(cert_id_base64)
                    b64decoded_ocsp_response = b64decode(ocsp_response)
                    if not self.is_valid_time(cert_id, b64decoded_ocsp_response):
                        continue
                    current_time = int(time.time())
                    cache_key: tuple[bytes, bytes, bytes] = self.decode_cert_id_key(
                        cert_id
                    )
                    found, _ = OCSPCache.find_cache(
                        self, cert_id, None, cache_key=cache_key, lock_cache=False
                    )
                    if OCSPCache.is_cache_fresh(current_time, ts):
                        new_cache_dict[cache_key] = OCSPResponseValidationResult(
                            ocsp_response=b64decoded_ocsp_response,
                            ts=current_time,
                            validated=False,
                        )
                    elif found:
                        OCSPCache.delete_cache(
                            self, cert_id, cache_key=cache_key, lock_cache=False
                        )
            if new_cache_dict:
                OCSP_RESPONSE_VALIDATION_CACHE._update(new_cache_dict)
                OCSPCache.CACHE_UPDATED = True
        except Exception as ex:
            logger.debug("Caught here - %s", ex)
            ermsg = "Exception raised while decoding OCSP Response Cache {}".format(
                str(ex)
            )
            raise RevocationCheckError(
                msg=ermsg, errno=ER_OCSP_RESPONSE_CACHE_DECODE_FAILED
            )

    def encode_ocsp_response_cache(self, ocsp_response_cache_json):
        """Encodes OCSP response cache to JSON."""
        logger.debug("encoding OCSP response cache to JSON")
        for (
            cache_key,
            ocsp_response_validation_result,
        ) in OCSP_RESPONSE_VALIDATION_CACHE.items():
            k = self.encode_cert_id_base64(cache_key)
            v = b64encode(ocsp_response_validation_result.ocsp_response).decode("ascii")
            ocsp_response_cache_json[k] = (ocsp_response_validation_result.ts, v)

    def read_cert_bundle(self, ca_bundle_file, storage=None):
        """Reads a certificate file including certificates in PEM format."""
        raise NotImplementedError

    def encode_cert_id_key(self, _):
        """Encodes Cert ID key to native CertID."""
        raise NotImplementedError

    def decode_cert_id_key(self, _):
        """Decodes name CertID to Cert ID key."""
        raise NotImplementedError

    def encode_cert_id_base64(self, hkey):
        """Encodes native CertID to base64 Cert ID."""
        raise NotImplementedError

    def decode_cert_id_base64(self, cert_id_base64):
        """Decodes base64 Cert ID to native CertID."""
        raise NotImplementedError

    def create_ocsp_request(
        self,
        issuer: Certificate,
        subject: Certificate,
    ) -> tuple[CertId, OCSPRequest]:
        """Creates CertId and OCSPRequest."""
        raise NotImplementedError

    def extract_ocsp_url(self, cert):
        """Extracts OCSP URL from Certificate."""
        raise NotImplementedError

    def decode_ocsp_request(self, ocsp_request):
        """Decodes OCSP request to DER."""
        raise NotImplementedError

    def decode_ocsp_request_b64(self, ocsp_request):
        """Decodes OCSP Request object to b64."""
        raise NotImplementedError

    def extract_good_status(self, single_response):
        """Extracts Revocation Status GOOD."""
        raise NotImplementedError

    def extract_revoked_status(self, single_response):
        """Extracts Revocation Status REVOKED."""
        raise NotImplementedError

    def process_ocsp_response(self, issuer, cert_id, ocsp_response):
        """Processes OCSP response."""
        raise NotImplementedError

    def verify_signature(self, signature_algorithm, signature, cert, data):
        """Verifies signature."""
        raise NotImplementedError

    def extract_certificate_chain(self, connection):
        """Gets certificate chain and extract the key info from OpenSSL connection."""
        raise NotImplementedError

    def create_pair_issuer_subject(self, cert_map):
        """Creates pairs of issuer and subject certificates."""
        raise NotImplementedError

    def subject_name(self, subject):
        """Gets human readable Subject name."""
        raise NotImplementedError

    def is_valid_time(self, cert_id, ocsp_response):
        """Checks whether ocsp_response is in valid time range."""
        raise NotImplementedError
