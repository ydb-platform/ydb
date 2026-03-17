#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import typing as t

from elastic_transport import ObjectApiResponse

from ._base import NamespacedClient
from .utils import (
    SKIP_IN_PATH,
    Stability,
    _quote,
    _rewrite_parameters,
    _stability_warning,
)


class SearchApplicationClient(NamespacedClient):

    @_rewrite_parameters()
    @_stability_warning(Stability.BETA)
    async def delete(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a search application.</p>
          <p>Remove a search application and its associated alias. Indices attached to the search application are not removed.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-delete>`_

        :param name: The name of the search application to delete.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_application/search_application/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="search_application.delete",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def delete_behavioral_analytics(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a behavioral analytics collection.
          The associated data stream is also deleted.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-delete-behavioral-analytics>`_

        :param name: The name of the analytics collection to be deleted
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_application/analytics/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="search_application.delete_behavioral_analytics",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.BETA)
    async def get(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get search application details.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-get>`_

        :param name: The name of the search application
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_application/search_application/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="search_application.get",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def get_behavioral_analytics(
        self,
        *,
        name: t.Optional[t.Sequence[str]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get behavioral analytics collections.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-get-behavioral-analytics>`_

        :param name: A list of analytics collections to limit the returned information
        """
        __path_parts: t.Dict[str, str]
        if name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_application/analytics/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_application/analytics"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="search_application.get_behavioral_analytics",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={"from": "from_"},
    )
    @_stability_warning(Stability.BETA)
    async def list(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        q: t.Optional[str] = None,
        size: t.Optional[int] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get search applications.
          Get information about search applications.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-get-behavioral-analytics>`_

        :param from_: Starting offset.
        :param q: Query in the Lucene query string syntax.
        :param size: Specifies a max number of results to get.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_application/search_application"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if from_ is not None:
            __query["from"] = from_
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if q is not None:
            __query["q"] = q
        if size is not None:
            __query["size"] = size
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="search_application.list",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="payload",
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def post_behavioral_analytics_event(
        self,
        *,
        collection_name: str,
        event_type: t.Union[str, t.Literal["page_view", "search", "search_click"]],
        payload: t.Optional[t.Any] = None,
        body: t.Optional[t.Any] = None,
        debug: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a behavioral analytics collection event.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-post-behavioral-analytics-event>`_

        :param collection_name: The name of the behavioral analytics collection.
        :param event_type: The analytics event type.
        :param payload:
        :param debug: Whether the response type has to include more details
        """
        if collection_name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'collection_name'")
        if event_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'event_type'")
        if payload is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'payload' and 'body', one of them should be set."
            )
        elif payload is not None and body is not None:
            raise ValueError("Cannot set both 'payload' and 'body'")
        __path_parts: t.Dict[str, str] = {
            "collection_name": _quote(collection_name),
            "event_type": _quote(event_type),
        }
        __path = f'/_application/analytics/{__path_parts["collection_name"]}/event/{__path_parts["event_type"]}'
        __query: t.Dict[str, t.Any] = {}
        if debug is not None:
            __query["debug"] = debug
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __body = payload if payload is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="search_application.post_behavioral_analytics_event",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="search_application",
    )
    @_stability_warning(Stability.BETA)
    async def put(
        self,
        *,
        name: str,
        search_application: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        create: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update a search application.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-put>`_

        :param name: The name of the search application to be created or updated.
        :param search_application:
        :param create: If `true`, this request cannot replace or update existing Search
            Applications.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        if search_application is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'search_application' and 'body', one of them should be set."
            )
        elif search_application is not None and body is not None:
            raise ValueError("Cannot set both 'search_application' and 'body'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_application/search_application/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if create is not None:
            __query["create"] = create
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __body = search_application if search_application is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="search_application.put",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def put_behavioral_analytics(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a behavioral analytics collection.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-put-behavioral-analytics>`_

        :param name: The name of the analytics collection to be created or updated.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_application/analytics/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="search_application.put_behavioral_analytics",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("params",),
        ignore_deprecated_options={"params"},
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def render_query(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        params: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Render a search application query.
          Generate an Elasticsearch query using the specified query parameters and the search template associated with the search application or a default template if none is specified.
          If a parameter used in the search template is not specified in <code>params</code>, the parameter's default value will be used.
          The API returns the specific Elasticsearch query that would be generated and run by calling the search application search API.</p>
          <p>You must have <code>read</code> privileges on the backing alias of the search application.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-render-query>`_

        :param name: The name of the search application to render teh query for.
        :param params:
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = (
            f'/_application/search_application/{__path_parts["name"]}/_render_query'
        )
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if params is not None:
                __body["params"] = params
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="search_application.render_query",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("params",),
        ignore_deprecated_options={"params"},
    )
    @_stability_warning(Stability.BETA)
    async def search(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        params: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        typed_keys: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run a search application search.
          Generate and run an Elasticsearch query that uses the specified query parameteter and the search template associated with the search application or default template.
          Unspecified template parameters are assigned their default values if applicable.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-search>`_

        :param name: The name of the search application to be searched.
        :param params: Query parameters specific to this request, which will override
            any defaults specified in the template.
        :param typed_keys: Determines whether aggregation names are prefixed by their
            respective types in the response.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_application/search_application/{__path_parts["name"]}/_search'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if typed_keys is not None:
            __query["typed_keys"] = typed_keys
        if not __body:
            if params is not None:
                __body["params"] = params
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="search_application.search",
            path_parts=__path_parts,
        )
