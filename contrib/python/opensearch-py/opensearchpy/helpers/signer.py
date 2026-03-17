# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from typing import Any, Callable, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import requests


class AWSV4Signer:
    """
    Generic AWS V4 Request Signer.
    """

    def __init__(self, credentials, region: str, service: str = "es") -> Any:  # type: ignore
        if not credentials:
            raise ValueError("Credentials cannot be empty")
        self.credentials = credentials

        if not region:
            raise ValueError("Region cannot be empty")
        self.region = region

        if not service:
            raise ValueError("Service name cannot be empty")
        self.service = service

    def sign(
        self, method: str, url: str, body: Any, headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """
        This method signs the request and returns headers.
        :param method: HTTP method
        :param url: url
        :param body: body
        :return: headers
        """

        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        signature_host = self._fetch_url(url, headers or dict())

        # create an AWS request object and sign it using SigV4Auth
        aws_request = AWSRequest(method=method.upper(), url=signature_host, data=body)

        # credentials objects expose access_key, secret_key and token attributes
        # via @property annotations that call _refresh() on every access,
        # creating a race condition if the credentials expire before secret_key
        # is called but after access_key- the end result is the access_key doesn't
        # correspond to the secret_key used to sign the request. To avoid this,
        # get_frozen_credentials() which returns non-refreshing credentials is
        # called if it exists.
        credentials = (
            self.credentials.get_frozen_credentials()
            if hasattr(self.credentials, "get_frozen_credentials")
            and callable(self.credentials.get_frozen_credentials)
            else self.credentials
        )

        sig_v4_auth = SigV4Auth(credentials, self.service, self.region)
        sig_v4_auth.add_auth(aws_request)

        # copy the headers from AWS request object into the prepared_request
        headers = dict(aws_request.headers.items())
        headers["X-Amz-Content-SHA256"] = sig_v4_auth.payload(aws_request)

        return headers

    @staticmethod
    def _fetch_url(url: str, headers: Optional[Dict[str, str]]) -> str:
        """
        This is a util method that helps in reconstructing the request url.
        :param prepared_request: unsigned request
        :return: reconstructed url
        """
        parsed_url = urlparse(url)
        path = parsed_url.path or "/"

        # fetch the query string if present in the request
        querystring = ""
        if parsed_url.query:
            querystring = "?" + urlencode(
                parse_qs(parsed_url.query, keep_blank_values=True), doseq=True
            )

        # fetch the host information from headers
        headers = {key.lower(): value for key, value in (headers or dict()).items()}
        location = headers.get("host") or parsed_url.netloc

        # construct the url and return
        return parsed_url.scheme + "://" + location + path + querystring


class RequestsAWSV4SignerAuth(requests.auth.AuthBase):
    """
    AWS V4 Request Signer for Requests.
    """

    def __init__(self, credentials, region, service: str = "es") -> None:  # type: ignore
        self.signer = AWSV4Signer(credentials, region, service)
        self.service = service  # tools like LangChain rely on this, see https://github.com/opensearch-project/opensearch-py/issues/600

    def __call__(self, request):  # type: ignore
        return self._sign_request(request)  # type: ignore

    def _sign_request(self, prepared_request):  # type: ignore
        """
        This method helps in signing the request by injecting the required headers.
        :param prepared_request: unsigned request
        :return: signed request
        """

        updated_headers = self.signer.sign(
            method=prepared_request.method,
            url=prepared_request.url,
            body=prepared_request.body,
            headers=prepared_request.headers,
        )

        prepared_request.headers.update(updated_headers)

        return prepared_request


# Deprecated: use RequestsAWSV4SignerAuth
class AWSV4SignerAuth(RequestsAWSV4SignerAuth):
    pass


class Urllib3AWSV4SignerAuth(Callable):  # type: ignore
    def __init__(self, credentials, region, service: str = "es") -> None:  # type: ignore
        self.signer = AWSV4Signer(credentials, region, service)
        self.service = service  # tools like LangChain rely on this, see https://github.com/opensearch-project/opensearch-py/issues/600

    def __call__(
        self, method: str, url: str, body: Any, headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        return self.signer.sign(method, url, body, headers)
