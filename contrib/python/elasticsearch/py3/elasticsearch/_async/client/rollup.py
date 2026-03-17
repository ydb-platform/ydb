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


class RollupClient(NamespacedClient):

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def delete_job(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a rollup job.</p>
          <p>A job must be stopped before it can be deleted.
          If you attempt to delete a started job, an error occurs.
          Similarly, if you attempt to delete a nonexistent job, an exception occurs.</p>
          <p>IMPORTANT: When you delete a job, you remove only the process that is actively monitoring and rolling up data.
          The API does not delete any previously rolled up data.
          This is by design; a user may wish to roll up a static data set.
          Because the data set is static, after it has been fully rolled up there is no need to keep the indexing rollup job around (as there will be no new data).
          Thus the job can be deleted, leaving behind the rolled up data for analysis.
          If you wish to also remove the rollup data and the rollup index contains the data for only a single job, you can delete the whole rollup index.
          If the rollup index stores data from several jobs, you must issue a delete-by-query that targets the rollup job's identifier in the rollup index. For example:</p>
          <pre><code>POST my_rollup_index/_delete_by_query
          {
            &quot;query&quot;: {
              &quot;term&quot;: {
                &quot;_rollup.id&quot;: &quot;the_rollup_job_id&quot;
              }
            }
          }
          </code></pre>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-delete-job>`_

        :param id: Identifier for the job.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_rollup/job/{__path_parts["id"]}'
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
            endpoint_id="rollup.delete_job",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def get_jobs(
        self,
        *,
        id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get rollup job information.
          Get the configuration, stats, and status of rollup jobs.</p>
          <p>NOTE: This API returns only active (both <code>STARTED</code> and <code>STOPPED</code>) jobs.
          If a job was created, ran for a while, then was deleted, the API does not return any details about it.
          For details about a historical rollup job, the rollup capabilities API may be more useful.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-get-jobs>`_

        :param id: Identifier for the rollup job. If it is `_all` or omitted, the API
            returns all rollup jobs.
        """
        __path_parts: t.Dict[str, str]
        if id not in SKIP_IN_PATH:
            __path_parts = {"id": _quote(id)}
            __path = f'/_rollup/job/{__path_parts["id"]}'
        else:
            __path_parts = {}
            __path = "/_rollup/job"
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
            endpoint_id="rollup.get_jobs",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def get_rollup_caps(
        self,
        *,
        id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get the rollup job capabilities.
          Get the capabilities of any rollup jobs that have been configured for a specific index or index pattern.</p>
          <p>This API is useful because a rollup job is often configured to rollup only a subset of fields from the source index.
          Furthermore, only certain aggregations can be configured for various fields, leading to a limited subset of functionality depending on that configuration.
          This API enables you to inspect an index and determine:</p>
          <ol>
          <li>Does this index have associated rollup data somewhere in the cluster?</li>
          <li>If yes to the first question, what fields were rolled up, what aggregations can be performed, and where does the data live?</li>
          </ol>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-get-rollup-caps>`_

        :param id: Index, indices or index-pattern to return rollup capabilities for.
            `_all` may be used to fetch rollup capabilities from all jobs.
        """
        __path_parts: t.Dict[str, str]
        if id not in SKIP_IN_PATH:
            __path_parts = {"id": _quote(id)}
            __path = f'/_rollup/data/{__path_parts["id"]}'
        else:
            __path_parts = {}
            __path = "/_rollup/data"
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
            endpoint_id="rollup.get_rollup_caps",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def get_rollup_index_caps(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get the rollup index capabilities.
          Get the rollup capabilities of all jobs inside of a rollup index.
          A single rollup index may store the data for multiple rollup jobs and may have a variety of capabilities depending on those jobs. This API enables you to determine:</p>
          <ul>
          <li>What jobs are stored in an index (or indices specified via a pattern)?</li>
          <li>What target indices were rolled up, what fields were used in those rollups, and what aggregations can be performed on each job?</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-get-rollup-index-caps>`_

        :param index: Data stream or index to check for rollup capabilities. Wildcard
            (`*`) expressions are supported.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_rollup/data'
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
            endpoint_id="rollup.get_rollup_index_caps",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "cron",
            "groups",
            "index_pattern",
            "page_size",
            "rollup_index",
            "headers",
            "metrics",
            "timeout",
        ),
        ignore_deprecated_options={"headers"},
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def put_job(
        self,
        *,
        id: str,
        cron: t.Optional[str] = None,
        groups: t.Optional[t.Mapping[str, t.Any]] = None,
        index_pattern: t.Optional[str] = None,
        page_size: t.Optional[int] = None,
        rollup_index: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        headers: t.Optional[t.Mapping[str, t.Union[str, t.Sequence[str]]]] = None,
        human: t.Optional[bool] = None,
        metrics: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a rollup job.</p>
          <p>WARNING: From 8.15.0, calling this API in a cluster with no rollup usage will fail with a message about the deprecation and planned removal of rollup features. A cluster needs to contain either a rollup job or a rollup index in order for this API to be allowed to run.</p>
          <p>The rollup job configuration contains all the details about how the job should run, when it indexes documents, and what future queries will be able to run against the rollup index.</p>
          <p>There are three main sections to the job configuration: the logistical details about the job (for example, the cron schedule), the fields that are used for grouping, and what metrics to collect for each group.</p>
          <p>Jobs are created in a <code>STOPPED</code> state. You can start them with the start rollup jobs API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-put-job>`_

        :param id: Identifier for the rollup job. This can be any alphanumeric string
            and uniquely identifies the data that is associated with the rollup job.
            The ID is persistent; it is stored with the rolled up data. If you create
            a job, let it run for a while, then delete the job, the data that the job
            rolled up is still be associated with this job ID. You cannot create a new
            job with the same ID since that could lead to problems with mismatched job
            configurations.
        :param cron: A cron string which defines the intervals when the rollup job should
            be executed. When the interval triggers, the indexer attempts to rollup the
            data in the index pattern. The cron pattern is unrelated to the time interval
            of the data being rolled up. For example, you may wish to create hourly rollups
            of your document but to only run the indexer on a daily basis at midnight,
            as defined by the cron. The cron pattern is defined just like a Watcher cron
            schedule.
        :param groups: Defines the grouping fields and aggregations that are defined
            for this rollup job. These fields will then be available later for aggregating
            into buckets. These aggs and fields can be used in any combination. Think
            of the groups configuration as defining a set of tools that can later be
            used in aggregations to partition the data. Unlike raw data, we have to think
            ahead to which fields and aggregations might be used. Rollups provide enough
            flexibility that you simply need to determine which fields are needed, not
            in what order they are needed.
        :param index_pattern: The index or index pattern to roll up. Supports wildcard-style
            patterns (`logstash-*`). The job attempts to rollup the entire index or index-pattern.
        :param page_size: The number of bucket results that are processed on each iteration
            of the rollup indexer. A larger value tends to execute faster, but requires
            more memory during processing. This value has no effect on how the data is
            rolled up; it is merely used for tweaking the speed or memory cost of the
            indexer.
        :param rollup_index: The index that contains the rollup results. The index can
            be shared with other rollup jobs. The data is stored so that it doesn’t interfere
            with unrelated jobs.
        :param headers:
        :param metrics: Defines the metrics to collect for each grouping tuple. By default,
            only the doc_counts are collected for each group. To make rollup useful,
            you will often add metrics like averages, mins, maxes, etc. Metrics are defined
            on a per-field basis and for each field you configure which metric should
            be collected.
        :param timeout: Time to wait for the request to complete.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        if cron is None and body is None:
            raise ValueError("Empty value passed for parameter 'cron'")
        if groups is None and body is None:
            raise ValueError("Empty value passed for parameter 'groups'")
        if index_pattern is None and body is None:
            raise ValueError("Empty value passed for parameter 'index_pattern'")
        if page_size is None and body is None:
            raise ValueError("Empty value passed for parameter 'page_size'")
        if rollup_index is None and body is None:
            raise ValueError("Empty value passed for parameter 'rollup_index'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_rollup/job/{__path_parts["id"]}'
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
            if cron is not None:
                __body["cron"] = cron
            if groups is not None:
                __body["groups"] = groups
            if index_pattern is not None:
                __body["index_pattern"] = index_pattern
            if page_size is not None:
                __body["page_size"] = page_size
            if rollup_index is not None:
                __body["rollup_index"] = rollup_index
            if headers is not None:
                __body["headers"] = headers
            if metrics is not None:
                __body["metrics"] = metrics
            if timeout is not None:
                __body["timeout"] = timeout
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="rollup.put_job",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("aggregations", "aggs", "query", "size"),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def rollup_search(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        aggregations: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        aggs: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        rest_total_hits_as_int: t.Optional[bool] = None,
        size: t.Optional[int] = None,
        typed_keys: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Search rolled-up data.
          The rollup search endpoint is needed because, internally, rolled-up documents utilize a different document structure than the original data.
          It rewrites standard Query DSL into a format that matches the rollup documents then takes the response and rewrites it back to what a client would expect given the original query.</p>
          <p>The request body supports a subset of features from the regular search API.
          The following functionality is not available:</p>
          <p><code>size</code>: Because rollups work on pre-aggregated data, no search hits can be returned and so size must be set to zero or omitted entirely.
          <code>highlighter</code>, <code>suggestors</code>, <code>post_filter</code>, <code>profile</code>, <code>explain</code>: These are similarly disallowed.</p>
          <p>For more detailed examples of using the rollup search API, including querying rolled-up data only or combining rolled-up and live data, refer to the External documentation.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-rollup-search>`_

        :param index: A comma-separated list of data streams and indices used to limit
            the request. This parameter has the following rules: * At least one data
            stream, index, or wildcard expression must be specified. This target can
            include a rollup or non-rollup index. For data streams, the stream's backing
            indices can only serve as non-rollup indices. Omitting the parameter or using
            `_all` are not permitted. * Multiple non-rollup indices may be specified.
            * Only one rollup index may be specified. If more than one are supplied,
            an exception occurs. * Wildcard expressions (`*`) may be used. If they match
            more than one rollup index, an exception occurs. However, you can use an
            expression to match multiple non-rollup indices or data streams.
        :param aggregations: Specifies aggregations.
        :param aggs: Specifies aggregations.
        :param query: Specifies a DSL query that is subject to some limitations.
        :param rest_total_hits_as_int: Indicates whether hits.total should be rendered
            as an integer or an object in the rest search response
        :param size: Must be zero if set, as rollups work on pre-aggregated data.
        :param typed_keys: Specify whether aggregation and suggester names should be
            prefixed by their respective types in the response
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_rollup_search'
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
        if rest_total_hits_as_int is not None:
            __query["rest_total_hits_as_int"] = rest_total_hits_as_int
        if typed_keys is not None:
            __query["typed_keys"] = typed_keys
        if not __body:
            if aggregations is not None:
                __body["aggregations"] = aggregations
            if aggs is not None:
                __body["aggs"] = aggs
            if query is not None:
                __body["query"] = query
            if size is not None:
                __body["size"] = size
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="rollup.rollup_search",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def start_job(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Start rollup jobs.
          If you try to start a job that does not exist, an exception occurs.
          If you try to start a job that is already started, nothing happens.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-start-job>`_

        :param id: Identifier for the rollup job.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_rollup/job/{__path_parts["id"]}/_start'
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
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="rollup.start_job",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def stop_job(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_completion: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Stop rollup jobs.
          If you try to stop a job that does not exist, an exception occurs.
          If you try to stop a job that is already stopped, nothing happens.</p>
          <p>Since only a stopped job can be deleted, it can be useful to block the API until the indexer has fully stopped.
          This is accomplished with the <code>wait_for_completion</code> query parameter, and optionally a timeout. For example:</p>
          <pre><code>POST _rollup/job/sensor/_stop?wait_for_completion=true&amp;timeout=10s
          </code></pre>
          <p>The parameter blocks the API call from returning until either the job has moved to STOPPED or the specified time has elapsed.
          If the specified time elapses without the job moving to STOPPED, a timeout exception occurs.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-stop-job>`_

        :param id: Identifier for the rollup job.
        :param timeout: If `wait_for_completion` is `true`, the API blocks for (at maximum)
            the specified duration while waiting for the job to stop. If more than `timeout`
            time has passed, the API throws a timeout exception. NOTE: Even if a timeout
            occurs, the stop request is still processing and eventually moves the job
            to STOPPED. The timeout simply means the API call itself timed out while
            waiting for the status change.
        :param wait_for_completion: If set to `true`, causes the API to block until the
            indexer state completely stops. If set to `false`, the API returns immediately
            and the indexer is stopped asynchronously in the background.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_rollup/job/{__path_parts["id"]}/_stop'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_completion is not None:
            __query["wait_for_completion"] = wait_for_completion
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="rollup.stop_job",
            path_parts=__path_parts,
        )
