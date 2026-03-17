# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from typing import Any, Mapping, Optional

from .client import Client
from .utils import NamespacedClient


class HttpClient(NamespacedClient):
    def __init__(self, client: Client) -> None:
        super().__init__(client)

    async def get(
        self,
        url: str,
        headers: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
        body: Any = None,
    ) -> Any:
        """
        Perform a GET request and return the data.

        :arg url: absolute url (without host) to target
        :arg headers: dictionary of headers, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class
        :arg params: dictionary of query parameters, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class for serialization
        :arg body: body of the request, will be serialized using serializer and
            passed to the connection
        """
        return await self.transport.perform_request(
            "GET", url=url, headers=headers, params=params, body=body
        )

    async def head(
        self,
        url: str,
        headers: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
        body: Any = None,
    ) -> Any:
        """
        Perform a HEAD request and return the data.

        :arg url: absolute url (without host) to target
        :arg headers: dictionary of headers, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class
        :arg params: dictionary of query parameters, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class for serialization
        :arg body: body of the request, will be serialized using serializer and
            passed to the connection
        """
        return await self.transport.perform_request(
            "HEAD", url=url, headers=headers, params=params, body=body
        )

    async def post(
        self,
        url: str,
        headers: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
        body: Any = None,
    ) -> Any:
        """
        Perform a POST request and return the data.

        :arg url: absolute url (without host) to target
        :arg headers: dictionary of headers, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class
        :arg params: dictionary of query parameters, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class for serialization
        :arg body: body of the request, will be serialized using serializer and
            passed to the connection
        """
        return await self.transport.perform_request(
            "POST", url=url, headers=headers, params=params, body=body
        )

    async def delete(
        self,
        url: str,
        headers: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
        body: Any = None,
    ) -> Any:
        """
        Perform a DELETE request and return the data.

        :arg url: absolute url (without host) to target
        :arg headers: dictionary of headers, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class
        :arg params: dictionary of query parameters, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class for serialization
        :arg body: body of the request, will be serialized using serializer and
            passed to the connection
        """
        return await self.transport.perform_request(
            "DELETE", url=url, headers=headers, params=params, body=body
        )

    async def put(
        self,
        url: str,
        headers: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
        body: Any = None,
    ) -> Any:
        """
        Perform a PUT request and return the data.

        :arg url: absolute url (without host) to target
        :arg headers: dictionary of headers, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class
        :arg params: dictionary of query parameters, will be handed over to the
            underlying :class:`~opensearchpy.Connection` class for serialization
        :arg body: body of the request, will be serialized using serializer and
            passed to the connection
        """
        return await self.transport.perform_request(
            "PUT", url=url, headers=headers, params=params, body=body
        )
