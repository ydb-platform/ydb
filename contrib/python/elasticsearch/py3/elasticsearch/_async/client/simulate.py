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


class SimulateClient(NamespacedClient):

    @_rewrite_parameters(
        body_fields=(
            "docs",
            "component_template_substitutions",
            "index_template_substitutions",
            "mapping_addition",
            "pipeline_substitutions",
        ),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def ingest(
        self,
        *,
        docs: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        index: t.Optional[str] = None,
        component_template_substitutions: t.Optional[
            t.Mapping[str, t.Mapping[str, t.Any]]
        ] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        index_template_substitutions: t.Optional[
            t.Mapping[str, t.Mapping[str, t.Any]]
        ] = None,
        mapping_addition: t.Optional[t.Mapping[str, t.Any]] = None,
        pipeline: t.Optional[str] = None,
        pipeline_substitutions: t.Optional[
            t.Mapping[str, t.Mapping[str, t.Any]]
        ] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Simulate data ingestion.
          Run ingest pipelines against a set of provided documents, optionally with substitute pipeline definitions, to simulate ingesting data into an index.</p>
          <p>This API is meant to be used for troubleshooting or pipeline development, as it does not actually index any data into Elasticsearch.</p>
          <p>The API runs the default and final pipeline for that index against a set of documents provided in the body of the request.
          If a pipeline contains a reroute processor, it follows that reroute processor to the new index, running that index's pipelines as well the same way that a non-simulated ingest would.
          No data is indexed into Elasticsearch.
          Instead, the transformed document is returned, along with the list of pipelines that have been run and the name of the index where the document would have been indexed if this were not a simulation.
          The transformed document is validated against the mappings that would apply to this index, and any validation error is reported in the result.</p>
          <p>This API differs from the simulate pipeline API in that you specify a single pipeline for that API, and it runs only that one pipeline.
          The simulate pipeline API is more useful for developing a single pipeline, while the simulate ingest API is more useful for troubleshooting the interaction of the various pipelines that get applied when ingesting into an index.</p>
          <p>By default, the pipeline definitions that are currently in the system are used.
          However, you can supply substitute pipeline definitions in the body of the request.
          These will be used in place of the pipeline definitions that are already in the system. This can be used to replace existing pipeline definitions or to create new ones. The pipeline substitutions are used only within this request.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-simulate-ingest>`_

        :param docs: Sample documents to test in the pipeline.
        :param index: The index to simulate ingesting into. This value can be overridden
            by specifying an index on each document. If you specify this parameter in
            the request path, it is used for any documents that do not explicitly specify
            an index argument.
        :param component_template_substitutions: A map of component template names to
            substitute component template definition objects.
        :param index_template_substitutions: A map of index template names to substitute
            index template definition objects.
        :param mapping_addition:
        :param pipeline: The pipeline to use as the default pipeline. This value can
            be used to override the default pipeline of the index.
        :param pipeline_substitutions: Pipelines to test. If you don’t specify the `pipeline`
            request path parameter, this parameter is required. If you specify both this
            and the request path parameter, the API only uses the request path parameter.
        """
        if docs is None and body is None:
            raise ValueError("Empty value passed for parameter 'docs'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/_ingest/{__path_parts["index"]}/_simulate'
        else:
            __path_parts = {}
            __path = "/_ingest/_simulate"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pipeline is not None:
            __query["pipeline"] = pipeline
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if docs is not None:
                __body["docs"] = docs
            if component_template_substitutions is not None:
                __body["component_template_substitutions"] = (
                    component_template_substitutions
                )
            if index_template_substitutions is not None:
                __body["index_template_substitutions"] = index_template_substitutions
            if mapping_addition is not None:
                __body["mapping_addition"] = mapping_addition
            if pipeline_substitutions is not None:
                __body["pipeline_substitutions"] = pipeline_substitutions
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="simulate.ingest",
            path_parts=__path_parts,
        )
