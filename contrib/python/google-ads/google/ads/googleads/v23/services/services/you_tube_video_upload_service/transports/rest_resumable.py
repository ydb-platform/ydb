# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import base64
import json  # type: ignore
import os
from typing import Callable, Dict, Optional, Sequence, Tuple, Union, BinaryIO
from datetime import datetime, timedelta, timezone

from google.api_core import exceptions as core_exceptions
from google.api_core import gapic_v1
from google.api_core import retry as retries
from google.auth import credentials as ga_credentials  # type: ignore
from google.auth.transport.requests import AuthorizedSession  # type: ignore

import google.protobuf
from google.protobuf import empty_pb2  # type: ignore
from google.protobuf import json_format

from google.ads.googleads.v23.services.types import youtube_video_upload_service

from .rest_resumable_base import (
    _BaseYouTubeVideoUploadServiceRestResumableTransport,
)
from .resumable_upload import make_resumable_upload


try:
    OptionalRetry = Union[retries.Retry, gapic_v1.method._MethodDefault, None]
except AttributeError:  # pragma: NO COVER
    OptionalRetry = Union[retries.Retry, object, None]  # type: ignore

_DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024  # 10MB
_DEFAULT_TIMEOUT = 14400
_DEFAULT_CONTENT_TYPE = "application/octet-stream"

_DEFAULT_RETRY = retries.Retry(
    predicate=retries.if_exception_type(
        core_exceptions.ServiceUnavailable,
        core_exceptions.DeadlineExceeded,
    ),
    initial=1.0,
    maximum=60.0,
    multiplier=1.3,
)

DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo()

if hasattr(DEFAULT_CLIENT_INFO, "protobuf_runtime_version"):  # pragma: NO COVER
    DEFAULT_CLIENT_INFO.protobuf_runtime_version = google.protobuf.__version__


class YouTubeVideoUploadServiceRestResumableInterceptor:
    """Interceptor for YouTubeVideoUploadService. Not implemented yet"""


class YouTubeVideoUploadServiceRestResumableTransport(
    _BaseYouTubeVideoUploadServiceRestResumableTransport
):
    """REST backend synchronous transport for YouTubeVideoUploadService.
    It is only used by Resumable Media Upload functionality
    """

    def __init__(
        self,
        *,
        host: str = "googleads.googleapis.com",
        credentials: Optional[ga_credentials.Credentials] = None,
        credentials_file: Optional[str] = None,
        scopes: Optional[Sequence[str]] = None,
        client_cert_source_for_mtls: Optional[
            Callable[[], Tuple[bytes, bytes]]
        ] = None,
        quota_project_id: Optional[str] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
        always_use_jwt_access: Optional[bool] = False,
        url_scheme: str = "https",
        interceptor: Optional[
            YouTubeVideoUploadServiceRestResumableInterceptor
        ] = None,
        api_audience: Optional[str] = None,
        developer_token: Optional[str] = None,
        login_customer_id: Optional[str] = None,
        linked_customer_id: Optional[str] = None,
        use_cloud_org_for_api_access: bool = False,
        **kwargs,  # Catch-all for forward compatibility
    ) -> None:
        """Instantiate the transport.

        Args:
            host (Optional[str]):
                 The hostname to connect to (default: 'googleads.googleapis.com').
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.

            credentials_file (Optional[str]): Deprecated. A file with credentials that can
                be loaded with :func:`google.auth.load_credentials_from_file`.
                This argument is ignored if ``channel`` is provided. This argument will be
                removed in the next major version of this library.
            scopes (Optional(Sequence[str])): A list of scopes. This argument is
                ignored if ``channel`` is provided.
            client_cert_source_for_mtls (Callable[[], Tuple[bytes, bytes]]): Client
                certificate to configure mutual TLS HTTP channel. It is ignored
                if ``channel`` is provided.
            quota_project_id (Optional[str]): An optional project to use for billing
                and quota.
            client_info (google.api_core.gapic_v1.client_info.ClientInfo):
                The client info used to send a user-agent string along with
                API requests. If ``None``, then default info will be used.
                Generally, you only need to set this if you are developing
                your own client library.
            always_use_jwt_access (Optional[bool]): Whether self signed JWT should
                be used for service account credentials.
            url_scheme: the protocol scheme for the API endpoint.  Normally
                "https", but for testing or local servers,
                "http" can be specified.
            developer_token (Optional[str]): The developer token to use for the
                resumable upload transport.
            login_customer_id (Optional[str]): The login customer ID to use for
                the resumable upload transport.
            linked_customer_id (Optional[str]): The linked customer ID to use for
                the resumable upload transport.
            use_cloud_org_for_api_access (bool): Whether to use the cloud org for
                API access for the resumable upload transport.
        """
        # Run the base constructor
        super().__init__(
            host=host,
            credentials=credentials,
            client_info=client_info,
            always_use_jwt_access=always_use_jwt_access,
            url_scheme=url_scheme,
            api_audience=api_audience,
        )
        self._session = AuthorizedSession(
            self._credentials, default_host=self.DEFAULT_HOST
        )
        if client_cert_source_for_mtls:
            self._session.configure_mtls_channel(client_cert_source_for_mtls)

        self._developer_token = developer_token
        self._login_customer_id = login_customer_id
        self._linked_customer_id = linked_customer_id
        self._use_cloud_org_for_api_access = use_cloud_org_for_api_access

    def _sanitize_headers_for_rest(
        self, metadata: Union[Dict, Sequence[Tuple[str, Union[str, bytes]]]]
    ) -> Dict[str, str]:
        """
        Converts gRPC metadata into REST-friendly headers.

        - Decodes byte keys to strings.
        - Base64 encodes byte values if the key ends with '-bin'.
        - Raises ValueError if binary data is found in a non-'-bin' key.
        """
        # Normalize input to a dict if it's a sequence
        raw_headers = (
            dict(metadata) if not isinstance(metadata, dict) else metadata
        )
        clean_headers = {}

        for key, value in raw_headers.items():
            # Ensure Key is a String
            k_str = key.decode("utf-8") if isinstance(key, bytes) else key

            if isinstance(value, bytes):
                if not k_str.endswith("-bin"):
                    raise ValueError(
                        f"Invalid binary header '{k_str}'. Binary data is only "
                        "allowed in keys ending with '-bin'."
                    )
                # Valid binary header: Base64 encode
                clean_headers[k_str] = base64.b64encode(value).decode("ascii")
            else:
                # String value: Keep as is
                clean_headers[k_str] = str(value)

        return clean_headers

    def _update_headers_with_googleads(
        self, headers: dict[str, str]
    ) -> dict[str, str]:
        """
        Returns a new dictionary with identity headers injected, mirroring the
        MetadataInterceptor logic. Does not modify the input dictionary.
        """
        # Create a shallow copy to avoid in-place modification
        new_headers = headers.copy() if headers else {}

        # 1. Developer Token Logic
        # Only add if we are NOT using Cloud Org access AND the token exists.
        # This prevents the crash if developer_token is None.
        if not self._use_cloud_org_for_api_access and self._developer_token:
            new_headers["developer-token"] = self._developer_token

        # 2. Login Customer ID Logic
        if self._login_customer_id:
            new_headers["login-customer-id"] = self._login_customer_id

        # 3. Linked Customer ID Logic
        if self._linked_customer_id:
            new_headers["linked-customer-id"] = self._linked_customer_id

        return new_headers

    @staticmethod
    def _maybe_rewind(stream, rewind=False):
        """Rewind the stream if desired.

        :type stream: IO[bytes]
        :param stream: A bytes IO object open for reading.

        :type rewind: bool
        :param rewind: Indicates if we should seek to the beginning of the stream.
        """
        if rewind:
            stream.seek(0, os.SEEK_SET)

    def create_you_tube_video_upload_resumable(
        self,
        request: youtube_video_upload_service.CreateYouTubeVideoUploadRequest,
        *,
        stream: BinaryIO,
        rewind: bool = False,
        size: Optional[int] = None,
        content_type: Optional[str] = None,
        chunk_size: Optional[int] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> youtube_video_upload_service.CreateYouTubeVideoUploadResponse:
        r"""Uploads a video to Google-managed or advertiser owned
        (brand) YouTube channel.

        Args:
            request (Union[google.ads.googleads.v23.services.types.CreateYouTubeVideoUploadRequest, dict]):
                The request object. Request message for
                [YouTubeVideoUploadService.CreateYouTubeVideoUpload][google.ads.googleads.v23.services.YouTubeVideoUploadService.CreateYouTubeVideoUpload].
            stream (BinaryIO):
                A stream of bytes, such as a file opened in binary mode for reading.
            rewind (bool):
                If True, seek to the beginning of the file handle before uploading from the stream.
            size (int):
                The number of bytes to be uploaded (which will be read from
                ``stream``). If not provided, the upload will be concluded once
                ``stream`` is exhausted.
            content_type (str):
                The MIME type of the content being uploaded.
            chunk_size (int):
                The size of each chunk to be uploaded.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.CreateYouTubeVideoUploadResponse:
                Response message for
                   [YouTubeVideoUploadService.CreateYouTubeVideoUpload][google.ads.googleads.v23.services.YouTubeVideoUploadService.CreateYouTubeVideoUpload].


        """

        self._maybe_rewind(stream, rewind=rewind)

        http_options = (
            _BaseYouTubeVideoUploadServiceRestResumableTransport._BaseCreateYouTubeVideoUploadRequest._get_http_options()
        )
        transcoded_request = _BaseYouTubeVideoUploadServiceRestResumableTransport._BaseCreateYouTubeVideoUploadRequest._get_transcoded_request(
            http_options, request
        )
        request_url = "{host}/resumable/upload{uri}".format(
            host=self._host, uri=transcoded_request["uri"]
        )

        # preserving_proto_field_name=True ensures we get snake case "customer_id"
        json_body = json_format.MessageToJson(
            request._pb,
            preserving_proto_field_name=True,
        )

        if chunk_size is None:
            chunk_size = _DEFAULT_CHUNK_SIZE
        if retry is gapic_v1.method.DEFAULT:
            retry = _DEFAULT_RETRY
        if timeout is None or timeout is gapic_v1.method.DEFAULT:
            timeout = _DEFAULT_TIMEOUT
        if content_type is None:
            content_type = _DEFAULT_CONTENT_TYPE

        deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)
        headers = dict(metadata)

        # 1. Sanitize (will raise ValueError if headers are invalid)
        headers = self._sanitize_headers_for_rest(headers or {})

        # 2. Inject google-ads specific metadata
        headers = self._update_headers_with_googleads(headers)

        response = make_resumable_upload(
            transport=self._session,
            request_body=json_body,
            stream=stream,
            upload_url=request_url,
            size=size,
            content_type=content_type,
            chunk_size=chunk_size,
            request_retry=retry,
            deadline=deadline,
            headers=headers,
            on_progress=None,
        )

        data = response.json()
        response_message = (
            youtube_video_upload_service.CreateYouTubeVideoUploadResponse()
        )
        json_format.ParseDict(
            data, response_message._pb, ignore_unknown_fields=True
        )

        return response_message


__all__ = ("YouTubeVideoUploadServiceRestResumableTransport",)
