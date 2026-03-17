# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


from typing import Any

from ..client.utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class GeospatialClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def geojson_upload_post(
        self,
        *,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Use an OpenSearch query to upload `GeoJSON`, operation will fail if index
        exists. - When type is `geo_point`, only Point geometry is allowed - When type
        is `geo_shape`, all geometry types are allowed (Point, MultiPoint, LineString,
        MultiLineString, Polygon, MultiPolygon, GeometryCollection, Envelope).


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return await self.transport.perform_request(
            "POST",
            "/_plugins/geospatial/geojson/_upload",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def geojson_upload_put(
        self,
        *,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Use an OpenSearch query to upload `GeoJSON` regardless if index exists. - When
        type is `geo_point`, only Point geometry is allowed - When type is `geo_shape`,
        all geometry types are allowed (Point, MultiPoint, LineString, MultiLineString,
        Polygon, MultiPolygon, GeometryCollection, Envelope).


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return await self.transport.perform_request(
            "PUT",
            "/_plugins/geospatial/geojson/_upload",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def get_upload_stats(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieves statistics for all geospatial uploads.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return await self.transport.perform_request(
            "GET", "/_plugins/geospatial/_upload/stats", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def delete_ip2geo_datasource(
        self,
        *,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Delete a specific IP2Geo data source.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "geospatial", "ip2geo", "datasource", name),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def get_ip2geo_datasource(
        self,
        *,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Get one or more IP2Geo data sources, defaulting to returning all if no names
        specified.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "geospatial", "ip2geo", "datasource", name),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def put_ip2geo_datasource(
        self,
        *,
        name: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Create a specific IP2Geo data source. Default values:   - `endpoint`:
        `"https://geoip.maps.opensearch.org/v1/geolite2-city/manifest.json"`   -
        `update_interval_in_days`: 3.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return await self.transport.perform_request(
            "PUT",
            _make_path("_plugins", "geospatial", "ip2geo", "datasource", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def put_ip2geo_datasource_settings(
        self,
        *,
        name: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Update a specific IP2Geo data source.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (name, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return await self.transport.perform_request(
            "PUT",
            _make_path(
                "_plugins", "geospatial", "ip2geo", "datasource", name, "_settings"
            ),
            params=params,
            headers=headers,
            body=body,
        )
