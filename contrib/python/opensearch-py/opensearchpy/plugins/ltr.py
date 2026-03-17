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


class LtrClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def cache_stats(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieves cache statistics for all feature stores.


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
        return self.transport.perform_request(
            "GET", "/_ltr/_cachestats", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def clear_cache(
        self,
        *,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Clears the store caches.


        :arg store: The name of the feature store for which to clear the
            cache.
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
        return self.transport.perform_request(
            "POST",
            _make_path("_ltr", store, "_clearcache"),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def create_default_store(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates the default feature store.


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
        return self.transport.perform_request(
            "PUT", "/_ltr", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def create_store(
        self,
        *,
        store: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a new feature store with the specified name.


        :arg store: The name of the feature store.
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
        if store in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'store'.")

        return self.transport.perform_request(
            "PUT", _make_path("_ltr", store), params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_default_store(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes the default feature store.


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
        return self.transport.perform_request(
            "DELETE", "/_ltr", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_store(
        self,
        *,
        store: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a feature store with the specified name.


        :arg store: The name of the feature store.
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
        if store in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'store'.")

        return self.transport.perform_request(
            "DELETE", _make_path("_ltr", store), params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_store(
        self,
        *,
        store: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Checks if a store exists.


        :arg store: The name of the feature store.
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
        if store in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'store'.")

        return self.transport.perform_request(
            "GET", _make_path("_ltr", store), params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def list_stores(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists all available feature stores.


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
        return self.transport.perform_request(
            "GET", "/_ltr", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source", "timeout")
    def stats(
        self,
        *,
        node_id: Any = None,
        stat: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides information about the current status of the LTR plugin.


        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information; use `_local` to return information from
            the node you're connecting to, leave empty to get information from all
            nodes.
        :arg stat: A comma-separated list of stats to retrieve; use
            `_all` or empty string to retrieve all stats.
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
        :arg timeout: The time in milliseconds to wait for a response.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_ltr", node_id, "stats", stat),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "merge",
        "pretty",
        "routing",
        "source",
        "version",
    )
    def add_features_to_set(
        self,
        *,
        name: Any,
        body: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Add features to an existing feature set in the default feature store.


        :arg name: The name of the feature set to add features to.
        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg merge: Whether to merge the feature list or append only.
            Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: Specific routing value.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg version: Version check to ensure feature set is modified
            with expected version.
        """
        for param in (name, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "POST",
            _make_path("_ltr", store, "_featureset", name, "_addfeatures"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "merge",
        "pretty",
        "routing",
        "source",
        "version",
    )
    def add_features_to_set_by_query(
        self,
        *,
        name: Any,
        query: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Add features to an existing feature set in the default feature store.


        :arg name: The name of the feature set to add features to.
        :arg query: Query string to filter existing features from the
            store by name. When provided, only features matching this query will be
            added to the feature set, and no request body should be included.
        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg merge: Whether to merge the feature list or append only.
            Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: Specific routing value.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg version: Version check to ensure feature set is modified
            with expected version.
        """
        for param in (name, query):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "POST",
            _make_path("_ltr", store, "_featureset", name, "_addfeatures", query),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "routing", "source")
    def create_feature(
        self,
        *,
        id: Any,
        body: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Create or update a feature in the default feature store.


        :arg id: The name of the feature.
        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: Specific routing value.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_ltr", store, "_feature", id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "routing", "source")
    def create_featureset(
        self,
        *,
        id: Any,
        body: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Create or update a feature set in the default feature store.


        :arg id: The name of the feature set.
        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: Specific routing value.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_ltr", store, "_featureset", id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "routing", "source")
    def create_model(
        self,
        *,
        id: Any,
        body: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Create or update a model in the default feature store.


        :arg id: The name of the model.
        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: Specific routing value.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_ltr", store, "_model", id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "routing", "source")
    def create_model_from_set(
        self,
        *,
        name: Any,
        body: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Create a model from an existing feature set in the default feature store.


        :arg name: The name of the feature set to use for creating the
            model.
        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: Specific routing value.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (name, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "POST",
            _make_path("_ltr", store, "_featureset", name, "_createmodel"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_feature(
        self,
        *,
        id: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Delete a feature from the default feature store.


        :arg id: The name of the feature.
        :arg store: The name of the feature store.
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
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_ltr", store, "_feature", id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_featureset(
        self,
        *,
        id: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Delete a feature set from the default feature store.


        :arg id: The name of the feature set.
        :arg store: The name of the feature store.
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
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_ltr", store, "_featureset", id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_model(
        self,
        *,
        id: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Delete a model from the default feature store.


        :arg id: The name of the model.
        :arg store: The name of the feature store.
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
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_ltr", store, "_model", id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_feature(
        self,
        *,
        id: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Get a feature from the default feature store.


        :arg id: The name of the feature.
        :arg store: The name of the feature store.
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
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_ltr", store, "_feature", id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_featureset(
        self,
        *,
        id: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Get a feature set from the default feature store.


        :arg id: The name of the feature set.
        :arg store: The name of the feature store.
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
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_ltr", store, "_featureset", id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_model(
        self,
        *,
        id: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Get a model from the default feature store.


        :arg id: The name of the model.
        :arg store: The name of the feature store.
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
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_ltr", store, "_model", id),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "from_",
        "human",
        "prefix",
        "pretty",
        "size",
        "source",
    )
    def search_features(
        self,
        *,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Search for features in a feature store.


        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg from_: The offset from the first result (for pagination).
            Default is 0.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg prefix: A name prefix to filter features by.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg size: The number of features to return. Default is 20.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        return self.transport.perform_request(
            "GET", _make_path("_ltr", store, "_feature"), params=params, headers=headers
        )

    @query_params(
        "error_trace",
        "filter_path",
        "from_",
        "human",
        "prefix",
        "pretty",
        "size",
        "source",
    )
    def search_featuresets(
        self,
        *,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Search for feature sets in a feature store.


        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg from_: The offset from the first result (for pagination).
            Default is 0.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg prefix: A name prefix to filter feature sets by.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg size: The number of feature sets to return. Default is 20.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        return self.transport.perform_request(
            "GET",
            _make_path("_ltr", store, "_featureset"),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "from_",
        "human",
        "prefix",
        "pretty",
        "size",
        "source",
    )
    def search_models(
        self,
        *,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Search for models in a feature store.


        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg from_: The offset from the first result (for pagination).
            Default is 0.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg prefix: A name prefix to filter models by.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg size: The number of models to return. Default is 20.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        return self.transport.perform_request(
            "GET", _make_path("_ltr", store, "_model"), params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "routing", "source")
    def update_feature(
        self,
        *,
        id: Any,
        body: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Update a feature in the default feature store.


        :arg id: The name of the feature.
        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: Specific routing value.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "POST",
            _make_path("_ltr", store, "_feature", id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "routing", "source")
    def update_featureset(
        self,
        *,
        id: Any,
        body: Any,
        store: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Update a feature set in the default feature store.


        :arg id: The name of the feature set.
        :arg store: The name of the feature store.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: Specific routing value.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "POST",
            _make_path("_ltr", store, "_featureset", id),
            params=params,
            headers=headers,
            body=body,
        )
