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
from .utils import SKIP_IN_PATH, _quote, _rewrite_parameters


class InferenceClient(NamespacedClient):

    @_rewrite_parameters(
        body_fields=("input", "task_settings"),
    )
    def completion(
        self,
        *,
        inference_id: str,
        input: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Any] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Perform completion inference on the service</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-inference>`_

        :param inference_id: The inference Id
        :param input: Inference input. Either a string or an array of strings.
        :param task_settings: Optional task settings
        :param timeout: Specifies the amount of time to wait for the inference request
            to complete.
        """
        if inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'inference_id'")
        if input is None and body is None:
            raise ValueError("Empty value passed for parameter 'input'")
        __path_parts: t.Dict[str, str] = {"inference_id": _quote(inference_id)}
        __path = f'/_inference/completion/{__path_parts["inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if input is not None:
                __body["input"] = input
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.completion",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete(
        self,
        *,
        inference_id: str,
        task_type: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "chat_completion",
                    "completion",
                    "rerank",
                    "sparse_embedding",
                    "text_embedding",
                ],
            ]
        ] = None,
        dry_run: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        force: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete an inference endpoint</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-delete>`_

        :param inference_id: The inference identifier.
        :param task_type: The task type
        :param dry_run: When true, the endpoint is not deleted and a list of ingest processors
            which reference this endpoint is returned.
        :param force: When true, the inference endpoint is forcefully deleted even if
            it is still being used by ingest processors or semantic text fields.
        """
        if inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'inference_id'")
        __path_parts: t.Dict[str, str]
        if task_type not in SKIP_IN_PATH and inference_id not in SKIP_IN_PATH:
            __path_parts = {
                "task_type": _quote(task_type),
                "inference_id": _quote(inference_id),
            }
            __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["inference_id"]}'
        elif inference_id not in SKIP_IN_PATH:
            __path_parts = {"inference_id": _quote(inference_id)}
            __path = f'/_inference/{__path_parts["inference_id"]}'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
        __query: t.Dict[str, t.Any] = {}
        if dry_run is not None:
            __query["dry_run"] = dry_run
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if force is not None:
            __query["force"] = force
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="inference.delete",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get(
        self,
        *,
        task_type: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "chat_completion",
                    "completion",
                    "rerank",
                    "sparse_embedding",
                    "text_embedding",
                ],
            ]
        ] = None,
        inference_id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get an inference endpoint</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-get>`_

        :param task_type: The task type
        :param inference_id: The inference Id
        """
        __path_parts: t.Dict[str, str]
        if task_type not in SKIP_IN_PATH and inference_id not in SKIP_IN_PATH:
            __path_parts = {
                "task_type": _quote(task_type),
                "inference_id": _quote(inference_id),
            }
            __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["inference_id"]}'
        elif inference_id not in SKIP_IN_PATH:
            __path_parts = {"inference_id": _quote(inference_id)}
            __path = f'/_inference/{__path_parts["inference_id"]}'
        else:
            __path_parts = {}
            __path = "/_inference"
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
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="inference.get",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("input", "input_type", "query", "task_settings"),
    )
    def inference(
        self,
        *,
        inference_id: str,
        input: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        task_type: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "chat_completion",
                    "completion",
                    "rerank",
                    "sparse_embedding",
                    "text_embedding",
                ],
            ]
        ] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        input_type: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[str] = None,
        task_settings: t.Optional[t.Any] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Perform inference on the service.</p>
          <p>This API enables you to use machine learning models to perform specific tasks on data that you provide as an input.
          It returns a response with the results of the tasks.
          The inference endpoint you use can perform one specific task that has been defined when the endpoint was created with the create inference API.</p>
          <p>For details about using this API with a service, such as Amazon Bedrock, Anthropic, or HuggingFace, refer to the service-specific documentation.</p>
          <blockquote>
          <p>info
          The inference APIs enable you to use certain services, such as built-in machine learning models (ELSER, E5), models uploaded through Eland, Cohere, OpenAI, Azure, Google AI Studio, Google Vertex AI, Anthropic, Watsonx.ai, or Hugging Face. For built-in models and models uploaded through Eland, the inference APIs offer an alternative way to use and manage trained models. However, if you do not plan to use the inference APIs to use these models or if you want to use non-NLP models, use the machine learning trained model APIs.</p>
          </blockquote>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-inference>`_

        :param inference_id: The unique identifier for the inference endpoint.
        :param input: The text on which you want to perform the inference task. It can
            be a single string or an array. > info > Inference endpoints for the `completion`
            task type currently only support a single string as input.
        :param task_type: The type of inference task that the model performs.
        :param input_type: Specifies the input data type for the text embedding model.
            The `input_type` parameter only applies to Inference Endpoints with the `text_embedding`
            task type. Possible values include: * `SEARCH` * `INGEST` * `CLASSIFICATION`
            * `CLUSTERING` Not all services support all values. Unsupported values will
            trigger a validation exception. Accepted values depend on the configured
            inference service, refer to the relevant service-specific documentation for
            more info. > info > The `input_type` parameter specified on the root level
            of the request body will take precedence over the `input_type` parameter
            specified in `task_settings`.
        :param query: The query input, which is required only for the `rerank` task.
            It is not required for other tasks.
        :param task_settings: Task settings for the individual inference request. These
            settings are specific to the task type you specified and override the task
            settings specified when initializing the service.
        :param timeout: The amount of time to wait for the inference request to complete.
        """
        if inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'inference_id'")
        if input is None and body is None:
            raise ValueError("Empty value passed for parameter 'input'")
        __path_parts: t.Dict[str, str]
        if task_type not in SKIP_IN_PATH and inference_id not in SKIP_IN_PATH:
            __path_parts = {
                "task_type": _quote(task_type),
                "inference_id": _quote(inference_id),
            }
            __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["inference_id"]}'
        elif inference_id not in SKIP_IN_PATH:
            __path_parts = {"inference_id": _quote(inference_id)}
            __path = f'/_inference/{__path_parts["inference_id"]}'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if input is not None:
                __body["input"] = input
            if input_type is not None:
                __body["input_type"] = input_type
            if query is not None:
                __body["query"] = query
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.inference",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="inference_config",
    )
    def put(
        self,
        *,
        inference_id: str,
        inference_config: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        task_type: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "chat_completion",
                    "completion",
                    "rerank",
                    "sparse_embedding",
                    "text_embedding",
                ],
            ]
        ] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an inference endpoint.</p>
          <p>IMPORTANT: The inference APIs enable you to use certain services, such as built-in machine learning models (ELSER, E5), models uploaded through Eland, Cohere, OpenAI, Mistral, Azure OpenAI, Google AI Studio, Google Vertex AI, Anthropic, Watsonx.ai, or Hugging Face.
          For built-in models and models uploaded through Eland, the inference APIs offer an alternative way to use and manage trained models.
          However, if you do not plan to use the inference APIs to use these models or if you want to use non-NLP models, use the machine learning trained model APIs.</p>
          <p>The following integrations are available through the inference API. You can find the available task types next to the integration name:</p>
          <ul>
          <li>AlibabaCloud AI Search (<code>completion</code>, <code>rerank</code>, <code>sparse_embedding</code>, <code>text_embedding</code>)</li>
          <li>Amazon Bedrock (<code>completion</code>, <code>text_embedding</code>)</li>
          <li>Amazon SageMaker (<code>chat_completion</code>, <code>completion</code>, <code>rerank</code>, <code>sparse_embedding</code>, <code>text_embedding</code>)</li>
          <li>Anthropic (<code>completion</code>)</li>
          <li>Azure AI Studio (<code>completion</code>, <code>text_embedding</code>)</li>
          <li>Azure OpenAI (<code>completion</code>, <code>text_embedding</code>)</li>
          <li>Cohere (<code>completion</code>, <code>rerank</code>, <code>text_embedding</code>)</li>
          <li>DeepSeek (<code>chat_completion</code>, <code>completion</code>)</li>
          <li>Elasticsearch (<code>rerank</code>, <code>sparse_embedding</code>, <code>text_embedding</code> - this service is for built-in models and models uploaded through Eland)</li>
          <li>ELSER (<code>sparse_embedding</code>)</li>
          <li>Google AI Studio (<code>completion</code>, <code>text_embedding</code>)</li>
          <li>Google Vertex AI (<code>chat_completion</code>, <code>completion</code>, <code>rerank</code>, <code>text_embedding</code>)</li>
          <li>Hugging Face (<code>chat_completion</code>, <code>completion</code>, <code>rerank</code>, <code>text_embedding</code>)</li>
          <li>JinaAI (<code>rerank</code>, <code>text_embedding</code>)</li>
          <li>Llama (<code>chat_completion</code>, <code>completion</code>, <code>text_embedding</code>)</li>
          <li>Mistral (<code>chat_completion</code>, <code>completion</code>, <code>text_embedding</code>)</li>
          <li>OpenAI (<code>chat_completion</code>, <code>completion</code>, <code>text_embedding</code>)</li>
          <li>VoyageAI (<code>rerank</code>, <code>text_embedding</code>)</li>
          <li>Watsonx inference integration (<code>text_embedding</code>)</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put>`_

        :param inference_id: The inference Id
        :param inference_config:
        :param task_type: The task type. Refer to the integration list in the API description
            for the available task types.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'inference_id'")
        if inference_config is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'inference_config' and 'body', one of them should be set."
            )
        elif inference_config is not None and body is not None:
            raise ValueError("Cannot set both 'inference_config' and 'body'")
        __path_parts: t.Dict[str, str]
        if task_type not in SKIP_IN_PATH and inference_id not in SKIP_IN_PATH:
            __path_parts = {
                "task_type": _quote(task_type),
                "inference_id": _quote(inference_id),
            }
            __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["inference_id"]}'
        elif inference_id not in SKIP_IN_PATH:
            __path_parts = {"inference_id": _quote(inference_id)}
            __path = f'/_inference/{__path_parts["inference_id"]}'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
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
        __body = inference_config if inference_config is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_alibabacloud(
        self,
        *,
        task_type: t.Union[
            str, t.Literal["completion", "rerank", "space_embedding", "text_embedding"]
        ],
        alibabacloud_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["alibabacloud-ai-search"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an AlibabaCloud AI Search inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>alibabacloud-ai-search</code> service.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-alibabacloud>`_

        :param task_type: The type of the inference task that the model will perform.
        :param alibabacloud_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `alibabacloud-ai-search`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `alibabacloud-ai-search` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if alibabacloud_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'alibabacloud_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "alibabacloud_inference_id": _quote(alibabacloud_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["alibabacloud_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_alibabacloud",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_amazonbedrock(
        self,
        *,
        task_type: t.Union[str, t.Literal["completion", "text_embedding"]],
        amazonbedrock_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["amazonbedrock"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an Amazon Bedrock inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>amazonbedrock</code> service.</p>
          <blockquote>
          <p>info
          You need to provide the access and secret keys only once, during the inference model creation. The get inference API does not retrieve your access or secret keys. After creating the inference model, you cannot change the associated key pairs. If you want to use a different access and secret key pair, delete the inference model and recreate it with the same name and the updated keys.</p>
          </blockquote>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-amazonbedrock>`_

        :param task_type: The type of the inference task that the model will perform.
        :param amazonbedrock_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `amazonbedrock`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `amazonbedrock` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if amazonbedrock_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'amazonbedrock_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "amazonbedrock_inference_id": _quote(amazonbedrock_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["amazonbedrock_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_amazonbedrock",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_amazonsagemaker(
        self,
        *,
        task_type: t.Union[
            str,
            t.Literal[
                "chat_completion",
                "completion",
                "rerank",
                "sparse_embedding",
                "text_embedding",
            ],
        ],
        amazonsagemaker_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["amazon_sagemaker"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an Amazon SageMaker inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>amazon_sagemaker</code> service.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-amazonsagemaker>`_

        :param task_type: The type of the inference task that the model will perform.
        :param amazonsagemaker_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `amazon_sagemaker`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `amazon_sagemaker` service and `service_settings.api`
            you specified.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type and `service_settings.api` you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if amazonsagemaker_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'amazonsagemaker_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "amazonsagemaker_inference_id": _quote(amazonsagemaker_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["amazonsagemaker_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_amazonsagemaker",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_anthropic(
        self,
        *,
        task_type: t.Union[str, t.Literal["completion"]],
        anthropic_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["anthropic"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an Anthropic inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>anthropic</code> service.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-anthropic>`_

        :param task_type: The task type. The only valid task type for the model to perform
            is `completion`.
        :param anthropic_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `anthropic`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `watsonxai` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if anthropic_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'anthropic_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "anthropic_inference_id": _quote(anthropic_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["anthropic_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_anthropic",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_azureaistudio(
        self,
        *,
        task_type: t.Union[str, t.Literal["completion", "text_embedding"]],
        azureaistudio_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["azureaistudio"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an Azure AI studio inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>azureaistudio</code> service.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-azureaistudio>`_

        :param task_type: The type of the inference task that the model will perform.
        :param azureaistudio_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `azureaistudio`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `openai` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if azureaistudio_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'azureaistudio_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "azureaistudio_inference_id": _quote(azureaistudio_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["azureaistudio_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_azureaistudio",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_azureopenai(
        self,
        *,
        task_type: t.Union[str, t.Literal["completion", "text_embedding"]],
        azureopenai_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["azureopenai"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an Azure OpenAI inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>azureopenai</code> service.</p>
          <p>The list of chat completion models that you can choose from in your Azure OpenAI deployment include:</p>
          <ul>
          <li><a href="https://learn.microsoft.com/en-us/azure/ai-services/openai/concepts/models?tabs=global-standard%2Cstandard-chat-completions#gpt-4-and-gpt-4-turbo-models">GPT-4 and GPT-4 Turbo models</a></li>
          <li><a href="https://learn.microsoft.com/en-us/azure/ai-services/openai/concepts/models?tabs=global-standard%2Cstandard-chat-completions#gpt-35">GPT-3.5</a></li>
          </ul>
          <p>The list of embeddings models that you can choose from in your deployment can be found in the <a href="https://learn.microsoft.com/en-us/azure/ai-services/openai/concepts/models?tabs=global-standard%2Cstandard-chat-completions#embeddings">Azure models documentation</a>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-azureopenai>`_

        :param task_type: The type of the inference task that the model will perform.
            NOTE: The `chat_completion` task type only supports streaming and only through
            the _stream API.
        :param azureopenai_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `azureopenai`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `azureopenai` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if azureopenai_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'azureopenai_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "azureopenai_inference_id": _quote(azureopenai_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["azureopenai_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_azureopenai",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_cohere(
        self,
        *,
        task_type: t.Union[str, t.Literal["completion", "rerank", "text_embedding"]],
        cohere_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["cohere"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a Cohere inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>cohere</code> service.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-cohere>`_

        :param task_type: The type of the inference task that the model will perform.
        :param cohere_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `cohere`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `cohere` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if cohere_inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'cohere_inference_id'")
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "cohere_inference_id": _quote(cohere_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["cohere_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_cohere",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_custom(
        self,
        *,
        task_type: t.Union[
            str, t.Literal["completion", "rerank", "sparse_embedding", "text_embedding"]
        ],
        custom_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["custom"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a custom inference endpoint.</p>
          <p>The custom service gives more control over how to interact with external inference services that aren't explicitly supported through dedicated integrations.
          The custom service gives you the ability to define the headers, url, query parameters, request body, and secrets.
          The custom service supports the template replacement functionality, which enables you to define a template that can be replaced with the value associated with that key.
          Templates are portions of a string that start with <code>${</code> and end with <code>}</code>.
          The parameters <code>secret_parameters</code> and <code>task_settings</code> are checked for keys for template replacement. Template replacement is supported in the <code>request</code>, <code>headers</code>, <code>url</code>, and <code>query_parameters</code>.
          If the definition (key) is not found for a template, an error message is returned.
          In case of an endpoint definition like the following:</p>
          <pre><code>PUT _inference/text_embedding/test-text-embedding
          {
            &quot;service&quot;: &quot;custom&quot;,
            &quot;service_settings&quot;: {
               &quot;secret_parameters&quot;: {
                    &quot;api_key&quot;: &quot;&lt;some api key&gt;&quot;
               },
               &quot;url&quot;: &quot;...endpoints.huggingface.cloud/v1/embeddings&quot;,
               &quot;headers&quot;: {
                   &quot;Authorization&quot;: &quot;Bearer ${api_key}&quot;,
                   &quot;Content-Type&quot;: &quot;application/json&quot;
               },
               &quot;request&quot;: &quot;{\\&quot;input\\&quot;: ${input}}&quot;,
               &quot;response&quot;: {
                   &quot;json_parser&quot;: {
                       &quot;text_embeddings&quot;:&quot;$.data[*].embedding[*]&quot;
                   }
               }
            }
          }
          </code></pre>
          <p>To replace <code>${api_key}</code> the <code>secret_parameters</code> and <code>task_settings</code> are checked for a key named <code>api_key</code>.</p>
          <blockquote>
          <p>info
          Templates should not be surrounded by quotes.</p>
          </blockquote>
          <p>Pre-defined templates:</p>
          <ul>
          <li><code>${input}</code> refers to the array of input strings that comes from the <code>input</code> field of the subsequent inference requests.</li>
          <li><code>${input_type}</code> refers to the input type translation values.</li>
          <li><code>${query}</code> refers to the query field used specifically for reranking tasks.</li>
          <li><code>${top_n}</code> refers to the <code>top_n</code> field available when performing rerank requests.</li>
          <li><code>${return_documents}</code> refers to the <code>return_documents</code> field available when performing rerank requests.</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-custom>`_

        :param task_type: The type of the inference task that the model will perform.
        :param custom_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `custom`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `custom` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if custom_inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'custom_inference_id'")
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "custom_inference_id": _quote(custom_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["custom_inference_id"]}'
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
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_custom",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("service", "service_settings", "chunking_settings"),
    )
    def put_deepseek(
        self,
        *,
        task_type: t.Union[str, t.Literal["chat_completion", "completion"]],
        deepseek_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["deepseek"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a DeepSeek inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>deepseek</code> service.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-deepseek>`_

        :param task_type: The type of the inference task that the model will perform.
        :param deepseek_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `deepseek`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `deepseek` service.
        :param chunking_settings: The chunking configuration object.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if deepseek_inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'deepseek_inference_id'")
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "deepseek_inference_id": _quote(deepseek_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["deepseek_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_deepseek",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_elasticsearch(
        self,
        *,
        task_type: t.Union[
            str, t.Literal["rerank", "sparse_embedding", "text_embedding"]
        ],
        elasticsearch_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["elasticsearch"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an Elasticsearch inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>elasticsearch</code> service.</p>
          <blockquote>
          <p>info
          Your Elasticsearch deployment contains preconfigured ELSER and E5 inference endpoints, you only need to create the enpoints using the API if you want to customize the settings.</p>
          </blockquote>
          <p>If you use the ELSER or the E5 model through the <code>elasticsearch</code> service, the API request will automatically download and deploy the model if it isn't downloaded yet.</p>
          <blockquote>
          <p>info
          You might see a 502 bad gateway error in the response when using the Kibana Console. This error usually just reflects a timeout, while the model downloads in the background. You can check the download progress in the Machine Learning UI. If using the Python client, you can set the timeout parameter to a higher value.</p>
          </blockquote>
          <p>After creating the endpoint, wait for the model deployment to complete before using it.
          To verify the deployment status, use the get trained model statistics API.
          Look for <code>&quot;state&quot;: &quot;fully_allocated&quot;</code> in the response and ensure that the <code>&quot;allocation_count&quot;</code> matches the <code>&quot;target_allocation_count&quot;</code>.
          Avoid creating multiple endpoints for the same model unless required, as each endpoint consumes significant resources.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-elasticsearch>`_

        :param task_type: The type of the inference task that the model will perform.
        :param elasticsearch_inference_id: The unique identifier of the inference endpoint.
            The must not match the `model_id`.
        :param service: The type of service supported for the specified task type. In
            this case, `elasticsearch`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `elasticsearch` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if elasticsearch_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'elasticsearch_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "elasticsearch_inference_id": _quote(elasticsearch_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["elasticsearch_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_elasticsearch",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("service", "service_settings", "chunking_settings"),
    )
    def put_elser(
        self,
        *,
        task_type: t.Union[str, t.Literal["sparse_embedding"]],
        elser_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["elser"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an ELSER inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>elser</code> service.
          You can also deploy ELSER by using the Elasticsearch inference integration.</p>
          <blockquote>
          <p>info
          Your Elasticsearch deployment contains a preconfigured ELSER inference endpoint, you only need to create the enpoint using the API if you want to customize the settings.</p>
          </blockquote>
          <p>The API request will automatically download and deploy the ELSER model if it isn't already downloaded.</p>
          <blockquote>
          <p>info
          You might see a 502 bad gateway error in the response when using the Kibana Console. This error usually just reflects a timeout, while the model downloads in the background. You can check the download progress in the Machine Learning UI. If using the Python client, you can set the timeout parameter to a higher value.</p>
          </blockquote>
          <p>After creating the endpoint, wait for the model deployment to complete before using it.
          To verify the deployment status, use the get trained model statistics API.
          Look for <code>&quot;state&quot;: &quot;fully_allocated&quot;</code> in the response and ensure that the <code>&quot;allocation_count&quot;</code> matches the <code>&quot;target_allocation_count&quot;</code>.
          Avoid creating multiple endpoints for the same model unless required, as each endpoint consumes significant resources.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-elser>`_

        :param task_type: The type of the inference task that the model will perform.
        :param elser_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `elser`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `elser` service.
        :param chunking_settings: The chunking configuration object.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if elser_inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'elser_inference_id'")
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "elser_inference_id": _quote(elser_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["elser_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_elser",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("service", "service_settings", "chunking_settings"),
    )
    def put_googleaistudio(
        self,
        *,
        task_type: t.Union[str, t.Literal["completion", "text_embedding"]],
        googleaistudio_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["googleaistudio"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an Google AI Studio inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>googleaistudio</code> service.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-googleaistudio>`_

        :param task_type: The type of the inference task that the model will perform.
        :param googleaistudio_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `googleaistudio`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `googleaistudio` service.
        :param chunking_settings: The chunking configuration object.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if googleaistudio_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'googleaistudio_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "googleaistudio_inference_id": _quote(googleaistudio_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["googleaistudio_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_googleaistudio",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_googlevertexai(
        self,
        *,
        task_type: t.Union[
            str, t.Literal["chat_completion", "completion", "rerank", "text_embedding"]
        ],
        googlevertexai_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["googlevertexai"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a Google Vertex AI inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>googlevertexai</code> service.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-googlevertexai>`_

        :param task_type: The type of the inference task that the model will perform.
        :param googlevertexai_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `googlevertexai`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `googlevertexai` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if googlevertexai_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'googlevertexai_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "googlevertexai_inference_id": _quote(googlevertexai_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["googlevertexai_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_googlevertexai",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_hugging_face(
        self,
        *,
        task_type: t.Union[
            str, t.Literal["chat_completion", "completion", "rerank", "text_embedding"]
        ],
        huggingface_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["hugging_face"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a Hugging Face inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>hugging_face</code> service.
          Supported tasks include: <code>text_embedding</code>, <code>completion</code>, and <code>chat_completion</code>.</p>
          <p>To configure the endpoint, first visit the Hugging Face Inference Endpoints page and create a new endpoint.
          Select a model that supports the task you intend to use.</p>
          <p>For Elastic's <code>text_embedding</code> task:
          The selected model must support the <code>Sentence Embeddings</code> task. On the new endpoint creation page, select the <code>Sentence Embeddings</code> task under the <code>Advanced Configuration</code> section.
          After the endpoint has initialized, copy the generated endpoint URL.
          Recommended models for <code>text_embedding</code> task:</p>
          <ul>
          <li><code>all-MiniLM-L6-v2</code></li>
          <li><code>all-MiniLM-L12-v2</code></li>
          <li><code>all-mpnet-base-v2</code></li>
          <li><code>e5-base-v2</code></li>
          <li><code>e5-small-v2</code></li>
          <li><code>multilingual-e5-base</code></li>
          <li><code>multilingual-e5-small</code></li>
          </ul>
          <p>For Elastic's <code>chat_completion</code> and <code>completion</code> tasks:
          The selected model must support the <code>Text Generation</code> task and expose OpenAI API. HuggingFace supports both serverless and dedicated endpoints for <code>Text Generation</code>. When creating dedicated endpoint select the <code>Text Generation</code> task.
          After the endpoint is initialized (for dedicated) or ready (for serverless), ensure it supports the OpenAI API and includes <code>/v1/chat/completions</code> part in URL. Then, copy the full endpoint URL for use.
          Recommended models for <code>chat_completion</code> and <code>completion</code> tasks:</p>
          <ul>
          <li><code>Mistral-7B-Instruct-v0.2</code></li>
          <li><code>QwQ-32B</code></li>
          <li><code>Phi-3-mini-128k-instruct</code></li>
          </ul>
          <p>For Elastic's <code>rerank</code> task:
          The selected model must support the <code>sentence-ranking</code> task and expose OpenAI API.
          HuggingFace supports only dedicated (not serverless) endpoints for <code>Rerank</code> so far.
          After the endpoint is initialized, copy the full endpoint URL for use.
          Tested models for <code>rerank</code> task:</p>
          <ul>
          <li><code>bge-reranker-base</code></li>
          <li><code>jina-reranker-v1-turbo-en-GGUF</code></li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-hugging-face>`_

        :param task_type: The type of the inference task that the model will perform.
        :param huggingface_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `hugging_face`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `hugging_face` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if huggingface_inference_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for parameter 'huggingface_inference_id'"
            )
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "huggingface_inference_id": _quote(huggingface_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["huggingface_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_hugging_face",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_jinaai(
        self,
        *,
        task_type: t.Union[str, t.Literal["rerank", "text_embedding"]],
        jinaai_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["jinaai"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an JinaAI inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>jinaai</code> service.</p>
          <p>To review the available <code>rerank</code> models, refer to <a href="https://jina.ai/reranker">https://jina.ai/reranker</a>.
          To review the available <code>text_embedding</code> models, refer to the <a href="https://jina.ai/embeddings/">https://jina.ai/embeddings/</a>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-jinaai>`_

        :param task_type: The type of the inference task that the model will perform.
        :param jinaai_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `jinaai`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `jinaai` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if jinaai_inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'jinaai_inference_id'")
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "jinaai_inference_id": _quote(jinaai_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["jinaai_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_jinaai",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("service", "service_settings", "chunking_settings"),
    )
    def put_mistral(
        self,
        *,
        task_type: t.Union[
            str, t.Literal["chat_completion", "completion", "text_embedding"]
        ],
        mistral_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["mistral"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a Mistral inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>mistral</code> service.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-mistral>`_

        :param task_type: The type of the inference task that the model will perform.
        :param mistral_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `mistral`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `mistral` service.
        :param chunking_settings: The chunking configuration object.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if mistral_inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'mistral_inference_id'")
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "mistral_inference_id": _quote(mistral_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["mistral_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_mistral",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_openai(
        self,
        *,
        task_type: t.Union[
            str, t.Literal["chat_completion", "completion", "text_embedding"]
        ],
        openai_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["openai"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an OpenAI inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>openai</code> service or <code>openai</code> compatible APIs.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-openai>`_

        :param task_type: The type of the inference task that the model will perform.
            NOTE: The `chat_completion` task type only supports streaming and only through
            the _stream API.
        :param openai_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `openai`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `openai` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if openai_inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'openai_inference_id'")
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "openai_inference_id": _quote(openai_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["openai_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_openai",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "service",
            "service_settings",
            "chunking_settings",
            "task_settings",
        ),
    )
    def put_voyageai(
        self,
        *,
        task_type: t.Union[str, t.Literal["rerank", "text_embedding"]],
        voyageai_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["voyageai"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        chunking_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a VoyageAI inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>voyageai</code> service.</p>
          <p>Avoid creating multiple endpoints for the same model unless required, as each endpoint consumes significant resources.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-voyageai>`_

        :param task_type: The type of the inference task that the model will perform.
        :param voyageai_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `voyageai`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `voyageai` service.
        :param chunking_settings: The chunking configuration object.
        :param task_settings: Settings to configure the inference task. These settings
            are specific to the task type you specified.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if voyageai_inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'voyageai_inference_id'")
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "voyageai_inference_id": _quote(voyageai_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["voyageai_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
            if chunking_settings is not None:
                __body["chunking_settings"] = chunking_settings
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_voyageai",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("service", "service_settings"),
    )
    def put_watsonx(
        self,
        *,
        task_type: t.Union[
            str, t.Literal["chat_completion", "completion", "text_embedding"]
        ],
        watsonx_inference_id: str,
        service: t.Optional[t.Union[str, t.Literal["watsonxai"]]] = None,
        service_settings: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a Watsonx inference endpoint.</p>
          <p>Create an inference endpoint to perform an inference task with the <code>watsonxai</code> service.
          You need an IBM Cloud Databases for Elasticsearch deployment to use the <code>watsonxai</code> inference service.
          You can provision one through the IBM catalog, the Cloud Databases CLI plug-in, the Cloud Databases API, or Terraform.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put-watsonx>`_

        :param task_type: The type of the inference task that the model will perform.
        :param watsonx_inference_id: The unique identifier of the inference endpoint.
        :param service: The type of service supported for the specified task type. In
            this case, `watsonxai`.
        :param service_settings: Settings used to install the inference model. These
            settings are specific to the `watsonxai` service.
        :param timeout: Specifies the amount of time to wait for the inference endpoint
            to be created.
        """
        if task_type in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_type'")
        if watsonx_inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'watsonx_inference_id'")
        if service is None and body is None:
            raise ValueError("Empty value passed for parameter 'service'")
        if service_settings is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_settings'")
        __path_parts: t.Dict[str, str] = {
            "task_type": _quote(task_type),
            "watsonx_inference_id": _quote(watsonx_inference_id),
        }
        __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["watsonx_inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if service is not None:
                __body["service"] = service
            if service_settings is not None:
                __body["service_settings"] = service_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.put_watsonx",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("input", "query", "task_settings"),
    )
    def rerank(
        self,
        *,
        inference_id: str,
        input: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        query: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Any] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Perform reranking inference on the service</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-inference>`_

        :param inference_id: The unique identifier for the inference endpoint.
        :param input: The text on which you want to perform the inference task. It can
            be a single string or an array. > info > Inference endpoints for the `completion`
            task type currently only support a single string as input.
        :param query: Query input.
        :param task_settings: Task settings for the individual inference request. These
            settings are specific to the task type you specified and override the task
            settings specified when initializing the service.
        :param timeout: The amount of time to wait for the inference request to complete.
        """
        if inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'inference_id'")
        if input is None and body is None:
            raise ValueError("Empty value passed for parameter 'input'")
        if query is None and body is None:
            raise ValueError("Empty value passed for parameter 'query'")
        __path_parts: t.Dict[str, str] = {"inference_id": _quote(inference_id)}
        __path = f'/_inference/rerank/{__path_parts["inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if input is not None:
                __body["input"] = input
            if query is not None:
                __body["query"] = query
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.rerank",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("input", "task_settings"),
    )
    def sparse_embedding(
        self,
        *,
        inference_id: str,
        input: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Any] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Perform sparse embedding inference on the service</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-inference>`_

        :param inference_id: The inference Id
        :param input: Inference input. Either a string or an array of strings.
        :param task_settings: Optional task settings
        :param timeout: Specifies the amount of time to wait for the inference request
            to complete.
        """
        if inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'inference_id'")
        if input is None and body is None:
            raise ValueError("Empty value passed for parameter 'input'")
        __path_parts: t.Dict[str, str] = {"inference_id": _quote(inference_id)}
        __path = f'/_inference/sparse_embedding/{__path_parts["inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if input is not None:
                __body["input"] = input
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.sparse_embedding",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("input", "task_settings"),
    )
    def text_embedding(
        self,
        *,
        inference_id: str,
        input: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        task_settings: t.Optional[t.Any] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Perform text embedding inference on the service</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-inference>`_

        :param inference_id: The inference Id
        :param input: Inference input. Either a string or an array of strings.
        :param task_settings: Optional task settings
        :param timeout: Specifies the amount of time to wait for the inference request
            to complete.
        """
        if inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'inference_id'")
        if input is None and body is None:
            raise ValueError("Empty value passed for parameter 'input'")
        __path_parts: t.Dict[str, str] = {"inference_id": _quote(inference_id)}
        __path = f'/_inference/text_embedding/{__path_parts["inference_id"]}'
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
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if input is not None:
                __body["input"] = input
            if task_settings is not None:
                __body["task_settings"] = task_settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.text_embedding",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="inference_config",
    )
    def update(
        self,
        *,
        inference_id: str,
        inference_config: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        task_type: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "chat_completion",
                    "completion",
                    "rerank",
                    "sparse_embedding",
                    "text_embedding",
                ],
            ]
        ] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update an inference endpoint.</p>
          <p>Modify <code>task_settings</code>, secrets (within <code>service_settings</code>), or <code>num_allocations</code> for an inference endpoint, depending on the specific endpoint service and <code>task_type</code>.</p>
          <p>IMPORTANT: The inference APIs enable you to use certain services, such as built-in machine learning models (ELSER, E5), models uploaded through Eland, Cohere, OpenAI, Azure, Google AI Studio, Google Vertex AI, Anthropic, Watsonx.ai, or Hugging Face.
          For built-in models and models uploaded through Eland, the inference APIs offer an alternative way to use and manage trained models.
          However, if you do not plan to use the inference APIs to use these models or if you want to use non-NLP models, use the machine learning trained model APIs.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-update>`_

        :param inference_id: The unique identifier of the inference endpoint.
        :param inference_config:
        :param task_type: The type of inference task that the model performs.
        """
        if inference_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'inference_id'")
        if inference_config is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'inference_config' and 'body', one of them should be set."
            )
        elif inference_config is not None and body is not None:
            raise ValueError("Cannot set both 'inference_config' and 'body'")
        __path_parts: t.Dict[str, str]
        if task_type not in SKIP_IN_PATH and inference_id not in SKIP_IN_PATH:
            __path_parts = {
                "task_type": _quote(task_type),
                "inference_id": _quote(inference_id),
            }
            __path = f'/_inference/{__path_parts["task_type"]}/{__path_parts["inference_id"]}/_update'
        elif inference_id not in SKIP_IN_PATH:
            __path_parts = {"inference_id": _quote(inference_id)}
            __path = f'/_inference/{__path_parts["inference_id"]}/_update'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __body = inference_config if inference_config is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="inference.update",
            path_parts=__path_parts,
        )
