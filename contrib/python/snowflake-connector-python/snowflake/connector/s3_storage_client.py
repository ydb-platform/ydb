#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import binascii
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from io import IOBase
from logging import getLogger
from operator import itemgetter
from typing import TYPE_CHECKING, Any, NamedTuple

from cryptography.hazmat.primitives import hashes, hmac

from .compat import quote, urlparse
from .constants import (
    HTTP_HEADER_CONTENT_TYPE,
    HTTP_HEADER_VALUE_OCTET_STREAM,
    FileHeader,
    ResultStatus,
)
from .encryption_util import EncryptionMetadata
from .storage_client import SnowflakeStorageClient, remove_content_encoding
from .vendored import requests

if TYPE_CHECKING:  # pragma: no cover
    from .file_transfer_agent import SnowflakeFileMeta, StorageCredential

logger = getLogger(__name__)

META_PREFIX = "x-amz-meta-"
SFC_DIGEST = "sfc-digest"

AMZ_MATDESC = "x-amz-matdesc"
AMZ_KEY = "x-amz-key"
AMZ_IV = "x-amz-iv"

ERRORNO_WSAECONNABORTED = 10053  # network connection was aborted

EXPIRED_TOKEN = "ExpiredToken"
ADDRESSING_STYLE = "virtual"  # explicit force to use virtual addressing style
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"

RE_MULTIPLE_SPACES = re.compile(r" +")


class S3Location(NamedTuple):
    bucket_name: str
    path: str


class SnowflakeS3RestClient(SnowflakeStorageClient):
    def __init__(
        self,
        meta: SnowflakeFileMeta,
        credentials: StorageCredential,
        stage_info: dict[str, Any],
        chunk_size: int,
        use_accelerate_endpoint: bool | None = None,
        use_s3_regional_url=False,
    ) -> None:
        """Rest client for S3 storage.

        Args:
            stage_info:
        """
        super().__init__(meta, stage_info, chunk_size, credentials=credentials)
        # Signature version V4
        # Addressing style Virtual Host
        self.region_name: str = stage_info["region"]
        # Multipart upload only
        self.upload_id: str | None = None
        self.etags: list[str] | None = None
        self.s3location: S3Location = (
            SnowflakeS3RestClient._extract_bucket_name_and_path(
                self.stage_info["location"]
            )
        )
        self.use_s3_regional_url = use_s3_regional_url

        # if GS sends us an endpoint, it's likely for FIPS. Use it.
        self.endpoint: str | None = None
        if stage_info["endPoint"]:
            self.endpoint = (
                f"https://{self.s3location.bucket_name}." + stage_info["endPoint"]
            )
        self.transfer_accelerate_config(use_accelerate_endpoint)

    def transfer_accelerate_config(
        self, use_accelerate_endpoint: bool | None = None
    ) -> bool:
        # if self.endpoint has been set, e.g. by metadata, no more config is needed.
        if self.endpoint is not None:
            return self.endpoint.find("s3-accelerate.amazonaws.com") >= 0
        if self.use_s3_regional_url:
            self.endpoint = (
                f"https://{self.s3location.bucket_name}."
                f"s3.{self.region_name}.amazonaws.com"
            )
            return False
        else:
            if use_accelerate_endpoint is None:
                use_accelerate_endpoint = self._get_bucket_accelerate_config(
                    self.s3location.bucket_name
                )

            if use_accelerate_endpoint:
                self.endpoint = (
                    f"https://{self.s3location.bucket_name}.s3-accelerate.amazonaws.com"
                )
            else:
                self.endpoint = (
                    f"https://{self.s3location.bucket_name}.s3.amazonaws.com"
                )
            return use_accelerate_endpoint

    @staticmethod
    def _sign_bytes(secret_key: bytes, _input: str) -> bytes:
        """Applies HMAC-SHA-256 to given string with secret_key."""
        h = hmac.HMAC(secret_key, hashes.SHA256())
        h.update(_input.encode("utf-8"))
        return h.finalize()

    @staticmethod
    def _sign_bytes_hex(secret_key: bytes, _input: str) -> bytes:
        """Convenience function, same as _sign_bytes, but returns result in hex form."""
        return binascii.hexlify(SnowflakeS3RestClient._sign_bytes(secret_key, _input))

    @staticmethod
    def _hash_bytes(_input: bytes) -> bytes:
        """Applies SHA-256 hash to given bytes."""
        digest = hashes.Hash(hashes.SHA256())
        digest.update(_input)
        return digest.finalize()

    @staticmethod
    def _hash_bytes_hex(_input: bytes) -> bytes:
        """Convenience function, same as _hash_bytes, but returns result in hex form."""
        return binascii.hexlify(SnowflakeS3RestClient._hash_bytes(_input))

    @staticmethod
    def _construct_query_string(
        query_parts: tuple[tuple[str, str], ...],
    ) -> str:
        """Convenience function to build the query part of a URL from key-value pairs.

        It filters out empty strings from the key, value pairs.
        """
        return "&".join(["=".join(filter(bool, e)) for e in query_parts])

    @staticmethod
    def _construct_canonicalized_and_signed_headers(
        headers: dict[str, str | list[str]]
    ) -> tuple[str, str]:
        """Construct canonical headers as per AWS specs, returns the signed headers too.

        Does not support sorting by values in case the keys are the same, don't send
        in duplicate keys, but this is not possible with a dictionary anyways.
        """
        res = []
        low_key_dict = {k.lower(): v for k, v in headers.items()}
        sorted_headers = sorted(low_key_dict.keys())
        _res = [(k, low_key_dict[k]) for k in sorted_headers]

        for k, v in _res:
            # if value is a list, convert to string delimited by comma
            if isinstance(v, list):
                v = ",".join(v)
            # if multiline header, replace withs space
            k = k.replace("\n", " ")
            res.append(k.strip() + ":" + RE_MULTIPLE_SPACES.sub(" ", v.strip()))

        ans = "\n".join(res)
        if ans:
            ans += "\n"

        return ans, ";".join(sorted_headers)

    @staticmethod
    def _construct_canonical_request_and_signed_headers(
        verb: str,
        canonical_uri_parameter: str,
        query_parts: dict[str, str],
        canonical_headers: dict[str, str | list[str]] | None = None,
        payload_hash: str = "",
    ) -> tuple[str, str]:
        """Build canonical request and also return signed headers.

        Note: this doesn't support sorting by values in case the same key is given
         more than once, but doing this is also not possible with a dictionary.
        """
        canonical_query_string = "&".join(
            "=".join([k, v]) for k, v in sorted(query_parts.items(), key=itemgetter(0))
        )
        (
            canonical_headers,
            signed_headers,
        ) = SnowflakeS3RestClient._construct_canonicalized_and_signed_headers(
            canonical_headers
        )

        return (
            "\n".join(
                [
                    verb,
                    canonical_uri_parameter or "/",
                    canonical_query_string,
                    canonical_headers,
                    signed_headers,
                    payload_hash,
                ]
            ),
            signed_headers,
        )

    @staticmethod
    def _construct_string_to_sign(
        region_name: str,
        service_name: str,
        amzdate: str,
        short_amzdate: str,
        canonical_request_hash: bytes,
    ) -> tuple[str, str]:
        """Given all the necessary information construct a V4 string to sign.

        As per AWS specs it requires the scope, the hash of the canonical request and
        the current date in the following format: YYYYMMDDTHHMMSSZ where T and Z are
        constant characters.
        This function generates the scope from the amzdate (which is just the date
        portion of amzdate), region name and service we want to use (this is only s3
        in our case).
        """
        scope = f"{short_amzdate}/{region_name}/{service_name}/aws4_request"
        return (
            "\n".join(
                [
                    "AWS4-HMAC-SHA256",
                    amzdate,
                    scope,
                    canonical_request_hash.decode("utf-8"),
                ]
            ),
            scope,
        )

    def _has_expired_token(self, response: requests.Response) -> bool:
        """Extract error code and error message from the S3's error response.

        Expected format:
        https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#RESTErrorResponses

        Args:
            response: Rest error response in XML format

        Returns: True if the error response is caused by token expiration

        """
        if response.status_code != 400:
            return False
        message = response.text
        if not message or message.isspace():
            return False
        err = ET.fromstring(message)
        return err.find("Code").text == EXPIRED_TOKEN

    @staticmethod
    def _extract_bucket_name_and_path(stage_location) -> S3Location:
        # split stage location as bucket name and path
        bucket_name, _, path = stage_location.partition("/")
        if path and not path.endswith("/"):
            path += "/"

        return S3Location(bucket_name=bucket_name, path=path)

    def _send_request_with_authentication_and_retry(
        self,
        url: str,
        verb: str,
        retry_id: int | str,
        query_parts: dict[str, str] | None = None,
        x_amz_headers: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        payload: bytes | bytearray | IOBase | None = None,
        unsigned_payload: bool = False,
        ignore_content_encoding: bool = False,
    ) -> requests.Response:
        if x_amz_headers is None:
            x_amz_headers = {}
        if headers is None:
            headers = {}
        if payload is None:
            payload = b""
        if query_parts is None:
            query_parts = {}
        parsed_url = urlparse(url)
        x_amz_headers["x-amz-security-token"] = self.credentials.creds.get(
            "AWS_TOKEN", ""
        )
        x_amz_headers["host"] = parsed_url.hostname
        if unsigned_payload:
            x_amz_headers["x-amz-content-sha256"] = UNSIGNED_PAYLOAD
        else:
            x_amz_headers["x-amz-content-sha256"] = (
                SnowflakeS3RestClient._hash_bytes_hex(payload).lower().decode()
            )

        def generate_authenticated_url_and_args_v4() -> tuple[bytes, dict[str, bytes]]:
            t = datetime.utcnow()
            amzdate = t.strftime("%Y%m%dT%H%M%SZ")
            short_amzdate = amzdate[:8]
            x_amz_headers["x-amz-date"] = amzdate

            (
                canonical_request,
                signed_headers,
            ) = self._construct_canonical_request_and_signed_headers(
                verb=verb,
                canonical_uri_parameter=parsed_url.path
                + (f";{parsed_url.params}" if parsed_url.params else ""),
                query_parts=query_parts,
                canonical_headers=x_amz_headers,
                payload_hash=x_amz_headers["x-amz-content-sha256"],
            )
            string_to_sign, scope = self._construct_string_to_sign(
                self.region_name,
                "s3",
                amzdate,
                short_amzdate,
                self._hash_bytes_hex(canonical_request.encode("utf-8")).lower(),
            )
            kDate = self._sign_bytes(
                ("AWS4" + self.credentials.creds["AWS_SECRET_KEY"]).encode("utf-8"),
                short_amzdate,
            )
            kRegion = self._sign_bytes(kDate, self.region_name)
            kService = self._sign_bytes(kRegion, "s3")
            signing_key = self._sign_bytes(kService, "aws4_request")

            signature = self._sign_bytes_hex(signing_key, string_to_sign).lower()
            authorization_header = (
                "AWS4-HMAC-SHA256 "
                + f"Credential={self.credentials.creds['AWS_KEY_ID']}/{scope}, "
                + f"SignedHeaders={signed_headers}, "
                + f"Signature={signature.decode('utf-8')}"
            )
            headers.update(x_amz_headers)
            headers["Authorization"] = authorization_header
            rest_args = {"headers": headers}

            if payload:
                rest_args["data"] = payload

            # add customized hook: to remove content-encoding from response.
            if ignore_content_encoding:
                rest_args["hooks"] = {"response": remove_content_encoding}

            return url.encode("utf-8"), rest_args

        return self._send_request_with_retry(
            verb, generate_authenticated_url_and_args_v4, retry_id
        )

    def get_file_header(self, filename: str) -> FileHeader | None:
        """Gets the metadata of file in specified location.

        Args:
            filename: Name of remote file.

        Returns:
            None if HEAD returns 404, otherwise a FileHeader instance populated
            with metadata
        """
        path = quote(self.s3location.path + filename.lstrip("/"))
        url = self.endpoint + f"/{path}"

        retry_id = "HEAD"
        self.retry_count[retry_id] = 0
        response = self._send_request_with_authentication_and_retry(
            url=url, verb="HEAD", retry_id=retry_id
        )
        if response.status_code == 200:
            self.meta.result_status = ResultStatus.UPLOADED
            metadata = response.headers
            encryption_metadata = (
                EncryptionMetadata(
                    key=metadata.get(META_PREFIX + AMZ_KEY),
                    iv=metadata.get(META_PREFIX + AMZ_IV),
                    matdesc=metadata.get(META_PREFIX + AMZ_MATDESC),
                )
                if metadata.get(META_PREFIX + AMZ_KEY)
                else None
            )
            return FileHeader(
                digest=metadata.get(META_PREFIX + SFC_DIGEST),
                content_length=int(metadata.get("Content-Length")),
                encryption_metadata=encryption_metadata,
            )
        elif response.status_code == 404:
            logger.debug(
                f"not found. bucket: {self.s3location.bucket_name}, path: {path}"
            )
            self.meta.result_status = ResultStatus.NOT_FOUND_FILE
            return None
        else:
            response.raise_for_status()

    def _prepare_file_metadata(self) -> dict[str, Any]:
        """Construct metadata for a file to be uploaded.

        Returns: File metadata in a dict.

        """
        s3_metadata = {
            META_PREFIX + SFC_DIGEST: self.meta.sha256_digest,
        }
        if self.encryption_metadata:
            s3_metadata.update(
                {
                    META_PREFIX + AMZ_IV: self.encryption_metadata.iv,
                    META_PREFIX + AMZ_KEY: self.encryption_metadata.key,
                    META_PREFIX + AMZ_MATDESC: self.encryption_metadata.matdesc,
                }
            )
        return s3_metadata

    def _initiate_multipart_upload(self) -> None:
        query_parts = (("uploads", ""),)
        path = quote(self.s3location.path + self.meta.dst_file_name.lstrip("/"))
        query_string = self._construct_query_string(query_parts)
        url = self.endpoint + f"/{path}?{query_string}"
        s3_metadata = self._prepare_file_metadata()
        # initiate multipart upload
        retry_id = "Initiate"
        self.retry_count[retry_id] = 0
        response = self._send_request_with_authentication_and_retry(
            url=url,
            verb="POST",
            retry_id=retry_id,
            x_amz_headers=s3_metadata,
            headers={HTTP_HEADER_CONTENT_TYPE: HTTP_HEADER_VALUE_OCTET_STREAM},
            query_parts=dict(query_parts),
        )
        if response.status_code == 200:
            self.upload_id = ET.fromstring(response.content)[2].text
            self.etags = [None] * self.num_of_chunks
        else:
            response.raise_for_status()

    def _upload_chunk(self, chunk_id: int, chunk: bytes) -> None:
        path = quote(self.s3location.path + self.meta.dst_file_name.lstrip("/"))
        url = self.endpoint + f"/{path}"

        if self.num_of_chunks == 1:  # single request
            s3_metadata = self._prepare_file_metadata()
            response = self._send_request_with_authentication_and_retry(
                url=url,
                verb="PUT",
                retry_id=chunk_id,
                payload=chunk,
                x_amz_headers=s3_metadata,
                headers={HTTP_HEADER_CONTENT_TYPE: HTTP_HEADER_VALUE_OCTET_STREAM},
                unsigned_payload=True,
            )
            response.raise_for_status()
        else:
            # multipart PUT
            query_parts = (
                ("partNumber", str(chunk_id + 1)),
                ("uploadId", self.upload_id),
            )
            query_string = self._construct_query_string(query_parts)
            chunk_url = f"{url}?{query_string}"
            response = self._send_request_with_authentication_and_retry(
                url=chunk_url,
                verb="PUT",
                retry_id=chunk_id,
                payload=chunk,
                unsigned_payload=True,
                query_parts=dict(query_parts),
            )
            if response.status_code == 200:
                self.etags[chunk_id] = response.headers["ETag"]
            response.raise_for_status()

    def _complete_multipart_upload(self) -> None:
        query_parts = (("uploadId", self.upload_id),)
        path = quote(self.s3location.path + self.meta.dst_file_name.lstrip("/"))
        query_string = self._construct_query_string(query_parts)
        url = self.endpoint + f"/{path}?{query_string}"
        logger.debug("Initiating multipart upload complete")
        # Complete multipart upload
        root = ET.Element("CompleteMultipartUpload")
        for idx, etag_str in enumerate(self.etags):
            part = ET.Element("Part")
            etag = ET.Element("ETag")
            etag.text = etag_str
            part.append(etag)
            part_number = ET.Element("PartNumber")
            part_number.text = str(idx + 1)
            part.append(part_number)
            root.append(part)
        retry_id = "Complete"
        self.retry_count[retry_id] = 0
        response = self._send_request_with_authentication_and_retry(
            url=url,
            verb="POST",
            retry_id=retry_id,
            payload=ET.tostring(root),
            query_parts=dict(query_parts),
        )
        response.raise_for_status()

    def _abort_multipart_upload(self) -> None:
        if self.upload_id is None:
            return
        query_parts = (("uploadId", self.upload_id),)
        path = quote(self.s3location.path + self.meta.dst_file_name.lstrip("/"))
        query_string = self._construct_query_string(query_parts)
        url = self.endpoint + f"/{path}?{query_string}"

        retry_id = "Abort"
        self.retry_count[retry_id] = 0
        response = self._send_request_with_authentication_and_retry(
            url=url,
            verb="DELETE",
            retry_id=retry_id,
            query_parts=dict(query_parts),
        )
        response.raise_for_status()

    def download_chunk(self, chunk_id: int) -> None:
        logger.debug(f"Downloading chunk {chunk_id}")
        path = quote(self.s3location.path + self.meta.src_file_name.lstrip("/"))
        url = self.endpoint + f"/{path}"
        if self.num_of_chunks == 1:
            response = self._send_request_with_authentication_and_retry(
                url=url,
                verb="GET",
                retry_id=chunk_id,
                ignore_content_encoding=True,
            )
            if response.status_code == 200:
                self.write_downloaded_chunk(0, response.content)
                self.meta.result_status = ResultStatus.DOWNLOADED
            response.raise_for_status()
        else:
            chunk_size = self.chunk_size
            if chunk_id < self.num_of_chunks - 1:
                _range = f"{chunk_id * chunk_size}-{(chunk_id+1)*chunk_size-1}"
            else:
                _range = f"{chunk_id * chunk_size}-"

            response = self._send_request_with_authentication_and_retry(
                url=url,
                verb="GET",
                retry_id=chunk_id,
                headers={"Range": f"bytes={_range}"},
            )
            if response.status_code in (200, 206):
                self.write_downloaded_chunk(chunk_id, response.content)
            response.raise_for_status()

    def _get_bucket_accelerate_config(self, bucket_name: str) -> bool:
        query_parts = (("accelerate", ""),)
        query_string = self._construct_query_string(query_parts)
        url = f"https://{bucket_name}.s3.amazonaws.com/?{query_string}"
        retry_id = "accelerate"
        self.retry_count[retry_id] = 0
        response = self._send_request_with_authentication_and_retry(
            url=url, verb="GET", retry_id=retry_id, query_parts=dict(query_parts)
        )
        if response.status_code == 200:
            config = ET.fromstring(response.text)
            namespace = config.tag[: config.tag.index("}") + 1]
            statusTag = f"{namespace}Status"
            found = config.find(statusTag)
            use_accelerate_endpoint = (
                False if found is None else (found.text == "Enabled")
            )
            logger.debug(f"use_accelerate_endpoint: {use_accelerate_endpoint}")
            return use_accelerate_endpoint
        return False
