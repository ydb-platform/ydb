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

# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

from typing import List, Union, Iterable
from typing_extensions import Literal, overload

import httpx

from ..types import interaction_get_params, interaction_create_params
from .._types import Body, Omit, Query, Headers, NotGiven, omit, not_given
from .._utils import required_args, maybe_transform, async_maybe_transform
from .._compat import cached_property
from .._resource import SyncAPIResource, AsyncAPIResource
from .._response import (
    to_raw_response_wrapper,
    to_streamed_response_wrapper,
    async_to_raw_response_wrapper,
    async_to_streamed_response_wrapper,
)
from .._streaming import Stream, AsyncStream
from .._base_client import make_request_options
from ..types.tool_param import ToolParam
from ..types.interaction import Interaction
from ..types.model_param import ModelParam
from ..types.interaction_sse_event import InteractionSSEEvent
from ..types.generation_config_param import GenerationConfigParam

__all__ = ["InteractionsResource", "AsyncInteractionsResource"]


class InteractionsResource(SyncAPIResource):
    @cached_property
    def with_raw_response(self) -> InteractionsResourceWithRawResponse:
        """
        This property can be used as a prefix for any HTTP method call to return
        the raw response object instead of the parsed content.

        For more information, see https://www.github.com/stainless-sdks/gemini-next-gen-api-python#accessing-raw-response-data-eg-headers
        """
        return InteractionsResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> InteractionsResourceWithStreamingResponse:
        """
        An alternative to `.with_raw_response` that doesn't eagerly read the response body.

        For more information, see https://www.github.com/stainless-sdks/gemini-next-gen-api-python#with_streaming_response
        """
        return InteractionsResourceWithStreamingResponse(self)

    @overload
    def create(
        self,
        *,
        api_version: str | None = None,
        input: interaction_create_params.Input,
        model: ModelParam,
        background: bool | Omit = omit,
        generation_config: GenerationConfigParam | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        stream: Literal[False] | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction:
        """
        Creates a new interaction.

        Args:
          input: The inputs for the interaction.

          model: The name of the `Model` used for generating the interaction.

          background: Input only. Whether to run the model interaction in the background.

          generation_config: Input only. Configuration parameters for the model interaction.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          stream: Input only. Whether the interaction will be streamed.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    def create(
        self,
        *,
        api_version: str | None = None,
        input: interaction_create_params.Input,
        model: ModelParam,
        stream: Literal[True],
        background: bool | Omit = omit,
        generation_config: GenerationConfigParam | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Stream[InteractionSSEEvent]:
        """
        Creates a new interaction.

        Args:
          input: The inputs for the interaction.

          model: The name of the `Model` used for generating the interaction.

          stream: Input only. Whether the interaction will be streamed.

          background: Input only. Whether to run the model interaction in the background.

          generation_config: Input only. Configuration parameters for the model interaction.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    def create(
        self,
        *,
        api_version: str | None = None,
        agent: Union[str, Literal["deep-research-pro-preview-12-2025"]],
        input: interaction_create_params.Input,
        agent_config: interaction_create_params.AgentConfig | Omit = omit,
        background: bool | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        stream: Literal[False] | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction:
        """
        Creates a new interaction.

        Args:
          agent: The name of the `Agent` used for generating the interaction.

          input: The inputs for the interaction.

          agent_config: Configuration for the agent.

          background: Input only. Whether to run the model interaction in the background.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          stream: Input only. Whether the interaction will be streamed.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    def create(
        self,
        *,
        api_version: str | None = None,
        agent: Union[str, Literal["deep-research-pro-preview-12-2025"]],
        input: interaction_create_params.Input,
        stream: Literal[True],
        agent_config: interaction_create_params.AgentConfig | Omit = omit,
        background: bool | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Stream[InteractionSSEEvent]:
        """
        Creates a new interaction.

        Args:
          agent: The name of the `Agent` used for generating the interaction.

          input: The inputs for the interaction.

          stream: Input only. Whether the interaction will be streamed.

          agent_config: Configuration for the agent.

          background: Input only. Whether to run the model interaction in the background.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    def create(
        self,
        *,
        api_version: str | None = None,
        input: interaction_create_params.Input,
        model: ModelParam,
        stream: bool,
        background: bool | Omit = omit,
        generation_config: GenerationConfigParam | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction | Stream[InteractionSSEEvent]:
        """
        Creates a new interaction.

        Args:
          input: The inputs for the interaction.

          model: The name of the `Model` used for generating the interaction.

          stream: Input only. Whether the interaction will be streamed.

          background: Input only. Whether to run the model interaction in the background.

          generation_config: Input only. Configuration parameters for the model interaction.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @required_args(["input", "model"], ["input", "model", "stream"], ["agent", "input"], ["agent", "input", "stream"])
    def create(
        self,
        *,
        api_version: str | None = None,
        input: interaction_create_params.Input,
        model: ModelParam | Omit = omit,
        background: bool | Omit = omit,
        generation_config: GenerationConfigParam | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        stream: Literal[False] | Literal[True] | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        agent: Union[str, Literal["deep-research-pro-preview-12-2025"]] | Omit = omit,
        agent_config: interaction_create_params.AgentConfig | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction | Stream[InteractionSSEEvent]:
        if api_version is None:
            api_version = self._client._get_api_version_path_param()
        if not api_version:
            raise ValueError(f"Expected a non-empty value for `api_version` but received {api_version!r}")
        if model is not omit and agent_config is not omit:
            raise ValueError("Invalid request: specified `model` and `agent_config`. If specifying `model`, use `generation_config`.")
        if agent is not omit and generation_config is not omit:
            raise ValueError("Invalid request: specified `agent` and `generation_config`. If specifying `agent`, use `agent_config`.")
        return self._post(
            self._client._build_maybe_vertex_path(api_version=api_version, path='interactions'),
            body=maybe_transform(
                {
                    "input": input,
                    "model": model,
                    "background": background,
                    "generation_config": generation_config,
                    "previous_interaction_id": previous_interaction_id,
                    "response_format": response_format,
                    "response_mime_type": response_mime_type,
                    "response_modalities": response_modalities,
                    "store": store,
                    "stream": stream,
                    "system_instruction": system_instruction,
                    "tools": tools,
                    "agent": agent,
                    "agent_config": agent_config,
                },
                interaction_create_params.CreateModelInteractionParamsStreaming
                if stream
                else interaction_create_params.CreateModelInteractionParamsNonStreaming,
            ),
            options=make_request_options(
                extra_headers=extra_headers, extra_query=extra_query, extra_body=extra_body, timeout=timeout
            ),
            cast_to=Interaction,
            stream=stream or False,
            stream_cls=Stream[InteractionSSEEvent],
        )

    def delete(
        self,
        id: str,
        *,
        api_version: str | None = None,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> object:
        """
        Deletes the interaction by id.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if api_version is None:
            api_version = self._client._get_api_version_path_param()
        if not api_version:
            raise ValueError(f"Expected a non-empty value for `api_version` but received {api_version!r}")
        if not id:
            raise ValueError(f"Expected a non-empty value for `id` but received {id!r}")
        return self._delete(
            self._client._build_maybe_vertex_path(api_version=api_version, path=f'interactions/{id}'),
            options=make_request_options(
                extra_headers=extra_headers, extra_query=extra_query, extra_body=extra_body, timeout=timeout
            ),
            cast_to=object,
        )

    def cancel(
        self,
        id: str,
        *,
        api_version: str | None = None,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction:
        """Cancels an interaction by id.

        This only applies to background interactions that are still running.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if api_version is None:
            api_version = self._client._get_api_version_path_param()
        if not api_version:
            raise ValueError(f"Expected a non-empty value for `api_version` but received {api_version!r}")
        if not id:
            raise ValueError(f"Expected a non-empty value for `id` but received {id!r}")
        return self._post(
            self._client._build_maybe_vertex_path(api_version=api_version, path=f'interactions/{id}/cancel'),
            options=make_request_options(
                extra_headers=extra_headers, extra_query=extra_query, extra_body=extra_body, timeout=timeout
            ),
            cast_to=Interaction,
        )

    @overload
    def get(
        self,
        id: str,
        *,
        api_version: str | None = None,
        include_input: bool | Omit = omit,
        last_event_id: str | Omit = omit,
        stream: Literal[False] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction:
        """
        Retrieves the full details of a single interaction based on its `Interaction.id`.

        Args:
          include_input: If set to true, includes the input in the response.

          last_event_id: Optional. If set, resumes the interaction stream from the next chunk after the event marked by the event id. Can only be used if `stream` is true.

          stream: If set to true, the generated content will be streamed incrementally.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    def get(
        self,
        id: str,
        *,
        api_version: str | None = None,
        stream: Literal[True],
        include_input: bool | Omit = omit,
        last_event_id: str | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Stream[InteractionSSEEvent]:
        """
        Retrieves the full details of a single interaction based on its `Interaction.id`.

        Args:
          stream: If set to true, the generated content will be streamed incrementally.

          include_input: If set to true, includes the input in the response.

          last_event_id: Optional. If set, resumes the interaction stream from the next chunk after the event marked by the event id. Can only be used if `stream` is true.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    def get(
        self,
        id: str,
        *,
        api_version: str | None = None,
        stream: bool,
        include_input: bool | Omit = omit,
        last_event_id: str | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction | Stream[InteractionSSEEvent]:
        """
        Retrieves the full details of a single interaction based on its `Interaction.id`.

        Args:
          stream: If set to true, the generated content will be streamed incrementally.

          include_input: If set to true, includes the input in the response.

          last_event_id: Optional. If set, resumes the interaction stream from the next chunk after the event marked by the event id. Can only be used if `stream` is true.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    def get(
        self,
        id: str,
        *,
        api_version: str | None = None,
        include_input: bool | Omit = omit,
        last_event_id: str | Omit = omit,
        stream: Literal[False] | Literal[True] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction | Stream[InteractionSSEEvent]:
        if api_version is None:
            api_version = self._client._get_api_version_path_param()
        if not api_version:
            raise ValueError(f"Expected a non-empty value for `api_version` but received {api_version!r}")
        if not id:
            raise ValueError(f"Expected a non-empty value for `id` but received {id!r}")
        return self._get(
            self._client._build_maybe_vertex_path(api_version=api_version, path=f'interactions/{id}'),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=maybe_transform(
                    {
                        "include_input": include_input,
                        "last_event_id": last_event_id,
                        "stream": stream,
                    },
                    interaction_get_params.InteractionGetParams,
                ),
            ),
            cast_to=Interaction,
            stream=stream or False,
            stream_cls=Stream[InteractionSSEEvent],
        )


class AsyncInteractionsResource(AsyncAPIResource):
    @cached_property
    def with_raw_response(self) -> AsyncInteractionsResourceWithRawResponse:
        """
        This property can be used as a prefix for any HTTP method call to return
        the raw response object instead of the parsed content.

        For more information, see https://www.github.com/stainless-sdks/gemini-next-gen-api-python#accessing-raw-response-data-eg-headers
        """
        return AsyncInteractionsResourceWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> AsyncInteractionsResourceWithStreamingResponse:
        """
        An alternative to `.with_raw_response` that doesn't eagerly read the response body.

        For more information, see https://www.github.com/stainless-sdks/gemini-next-gen-api-python#with_streaming_response
        """
        return AsyncInteractionsResourceWithStreamingResponse(self)

    @overload
    async def create(
        self,
        *,
        api_version: str | None = None,
        input: interaction_create_params.Input,
        model: ModelParam,
        background: bool | Omit = omit,
        generation_config: GenerationConfigParam | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        stream: Literal[False] | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction:
        """
        Creates a new interaction.

        Args:
          input: The inputs for the interaction.

          model: The name of the `Model` used for generating the interaction.

          background: Input only. Whether to run the model interaction in the background.

          generation_config: Input only. Configuration parameters for the model interaction.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          stream: Input only. Whether the interaction will be streamed.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    async def create(
        self,
        *,
        api_version: str | None = None,
        input: interaction_create_params.Input,
        model: ModelParam,
        stream: Literal[True],
        background: bool | Omit = omit,
        generation_config: GenerationConfigParam | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> AsyncStream[InteractionSSEEvent]:
        """
        Creates a new interaction.

        Args:
          input: The inputs for the interaction.

          model: The name of the `Model` used for generating the interaction.

          stream: Input only. Whether the interaction will be streamed.

          background: Input only. Whether to run the model interaction in the background.

          generation_config: Input only. Configuration parameters for the model interaction.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    async def create(
        self,
        *,
        api_version: str | None = None,
        agent: Union[str, Literal["deep-research-pro-preview-12-2025"]],
        input: interaction_create_params.Input,
        agent_config: interaction_create_params.AgentConfig | Omit = omit,
        background: bool | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        stream: Literal[False] | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction:
        """
        Creates a new interaction.

        Args:
          agent: The name of the `Agent` used for generating the interaction.

          input: The inputs for the interaction.

          agent_config: Configuration for the agent.

          background: Input only. Whether to run the model interaction in the background.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          stream: Input only. Whether the interaction will be streamed.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    async def create(
        self,
        *,
        api_version: str | None = None,
        agent: Union[str, Literal["deep-research-pro-preview-12-2025"]],
        input: interaction_create_params.Input,
        stream: Literal[True],
        agent_config: interaction_create_params.AgentConfig | Omit = omit,
        background: bool | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> AsyncStream[InteractionSSEEvent]:
        """
        Creates a new interaction.

        Args:
          agent: The name of the `Agent` used for generating the interaction.

          input: The inputs for the interaction.

          stream: Input only. Whether the interaction will be streamed.

          agent_config: Configuration for the agent.

          background: Input only. Whether to run the model interaction in the background.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    async def create(
        self,
        *,
        api_version: str | None = None,
        input: interaction_create_params.Input,
        model: ModelParam,
        stream: bool,
        background: bool | Omit = omit,
        generation_config: GenerationConfigParam | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction | AsyncStream[InteractionSSEEvent]:
        """
        Creates a new interaction.

        Args:
          input: The inputs for the interaction.

          model: The name of the `Model` used for generating the interaction.

          stream: Input only. Whether the interaction will be streamed.

          background: Input only. Whether to run the model interaction in the background.

          generation_config: Input only. Configuration parameters for the model interaction.

          previous_interaction_id: The ID of the previous interaction, if any.

          response_format: Enforces that the generated response is a JSON object that complies with
              the JSON schema specified in this field.

          response_mime_type: The mime type of the response. This is required if response_format is set.

          response_modalities: The requested modalities of the response (TEXT, IMAGE, AUDIO).

          store: Input only. Whether to store the response and request for later retrieval.

          system_instruction: System instruction for the interaction.

          tools: A list of tool declarations the model may call during interaction.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @required_args(["input", "model"], ["input", "model", "stream"], ["agent", "input"], ["agent", "input", "stream"])
    async def create(
        self,
        *,
        api_version: str | None = None,
        input: interaction_create_params.Input,
        model: ModelParam | Omit = omit,
        background: bool | Omit = omit,
        generation_config: GenerationConfigParam | Omit = omit,
        previous_interaction_id: str | Omit = omit,
        response_format: object | Omit = omit,
        response_mime_type: str | Omit = omit,
        response_modalities: List[Literal["text", "image", "audio"]] | Omit = omit,
        store: bool | Omit = omit,
        stream: Literal[False] | Literal[True] | Omit = omit,
        system_instruction: str | Omit = omit,
        tools: Iterable[ToolParam] | Omit = omit,
        agent: Union[str, Literal["deep-research-pro-preview-12-2025"]] | Omit = omit,
        agent_config: interaction_create_params.AgentConfig | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction | AsyncStream[InteractionSSEEvent]:
        if api_version is None:
            api_version = self._client._get_api_version_path_param()
        if not api_version:
            raise ValueError(f"Expected a non-empty value for `api_version` but received {api_version!r}")
        if model is not omit and agent_config is not omit:
            raise ValueError("Invalid request: specified `model` and `agent_config`. If specifying `model`, use `generation_config`.")
        if agent is not omit and generation_config is not omit:
            raise ValueError("Invalid request: specified `agent` and `generation_config`. If specifying `agent`, use `agent_config`.")
        return await self._post(
            self._client._build_maybe_vertex_path(api_version=api_version, path='interactions'),
            body=await async_maybe_transform(
                {
                    "input": input,
                    "model": model,
                    "background": background,
                    "generation_config": generation_config,
                    "previous_interaction_id": previous_interaction_id,
                    "response_format": response_format,
                    "response_mime_type": response_mime_type,
                    "response_modalities": response_modalities,
                    "store": store,
                    "stream": stream,
                    "system_instruction": system_instruction,
                    "tools": tools,
                    "agent": agent,
                    "agent_config": agent_config,
                },
                interaction_create_params.CreateModelInteractionParamsStreaming
                if stream
                else interaction_create_params.CreateModelInteractionParamsNonStreaming,
            ),
            options=make_request_options(
                extra_headers=extra_headers, extra_query=extra_query, extra_body=extra_body, timeout=timeout
            ),
            cast_to=Interaction,
            stream=stream or False,
            stream_cls=AsyncStream[InteractionSSEEvent],
        )

    async def delete(
        self,
        id: str,
        *,
        api_version: str | None = None,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> object:
        """
        Deletes the interaction by id.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if api_version is None:
            api_version = self._client._get_api_version_path_param()
        if not api_version:
            raise ValueError(f"Expected a non-empty value for `api_version` but received {api_version!r}")
        if not id:
            raise ValueError(f"Expected a non-empty value for `id` but received {id!r}")
        return await self._delete(
            self._client._build_maybe_vertex_path(api_version=api_version, path=f'interactions/{id}'),
            options=make_request_options(
                extra_headers=extra_headers, extra_query=extra_query, extra_body=extra_body, timeout=timeout
            ),
            cast_to=object,
        )

    async def cancel(
        self,
        id: str,
        *,
        api_version: str | None = None,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction:
        """Cancels an interaction by id.

        This only applies to background interactions that are still running.

        Args:
          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        if api_version is None:
            api_version = self._client._get_api_version_path_param()
        if not api_version:
            raise ValueError(f"Expected a non-empty value for `api_version` but received {api_version!r}")
        if not id:
            raise ValueError(f"Expected a non-empty value for `id` but received {id!r}")
        return await self._post(
            self._client._build_maybe_vertex_path(api_version=api_version, path=f'interactions/{id}/cancel'),
            options=make_request_options(
                extra_headers=extra_headers, extra_query=extra_query, extra_body=extra_body, timeout=timeout
            ),
            cast_to=Interaction,
        )

    @overload
    async def get(
        self,
        id: str,
        *,
        api_version: str | None = None,
        include_input: bool | Omit = omit,
        last_event_id: str | Omit = omit,
        stream: Literal[False] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction:
        """
        Retrieves the full details of a single interaction based on its `Interaction.id`.

        Args:
          include_input: If set to true, includes the input in the response.

          last_event_id: Optional. If set, resumes the interaction stream from the next chunk after the event marked by the event id. Can only be used if `stream` is true.

          stream: If set to true, the generated content will be streamed incrementally.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    async def get(
        self,
        id: str,
        *,
        api_version: str | None = None,
        stream: Literal[True],
        include_input: bool | Omit = omit,
        last_event_id: str | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> AsyncStream[InteractionSSEEvent]:
        """
        Retrieves the full details of a single interaction based on its `Interaction.id`.

        Args:
          stream: If set to true, the generated content will be streamed incrementally.

          include_input: If set to true, includes the input in the response.

          last_event_id: Optional. If set, resumes the interaction stream from the next chunk after the event marked by the event id. Can only be used if `stream` is true.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    @overload
    async def get(
        self,
        id: str,
        *,
        api_version: str | None = None,
        stream: bool,
        include_input: bool | Omit = omit,
        last_event_id: str | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction | AsyncStream[InteractionSSEEvent]:
        """
        Retrieves the full details of a single interaction based on its `Interaction.id`.

        Args:
          stream: If set to true, the generated content will be streamed incrementally.

          include_input: If set to true, includes the input in the response.

          last_event_id: Optional. If set, resumes the interaction stream from the next chunk after the event marked by the event id. Can only be used if `stream` is true.

          extra_headers: Send extra headers

          extra_query: Add additional query parameters to the request

          extra_body: Add additional JSON properties to the request

          timeout: Override the client-level default timeout for this request, in seconds
        """
        ...

    async def get(
        self,
        id: str,
        *,
        api_version: str | None = None,
        include_input: bool | Omit = omit,
        last_event_id: str | Omit = omit,
        stream: Literal[False] | Literal[True] | Omit = omit,
        # Use the following arguments if you need to pass additional parameters to the API that aren't available via kwargs.
        # The extra values given here take precedence over values defined on the client or passed to this method.
        extra_headers: Headers | None = None,
        extra_query: Query | None = None,
        extra_body: Body | None = None,
        timeout: float | httpx.Timeout | None | NotGiven = not_given,
    ) -> Interaction | AsyncStream[InteractionSSEEvent]:
        if api_version is None:
            api_version = self._client._get_api_version_path_param()
        if not api_version:
            raise ValueError(f"Expected a non-empty value for `api_version` but received {api_version!r}")
        if not id:
            raise ValueError(f"Expected a non-empty value for `id` but received {id!r}")
        return await self._get(
            self._client._build_maybe_vertex_path(api_version=api_version, path=f'interactions/{id}'),
            options=make_request_options(
                extra_headers=extra_headers,
                extra_query=extra_query,
                extra_body=extra_body,
                timeout=timeout,
                query=await async_maybe_transform(
                    {
                        "include_input": include_input,
                        "last_event_id": last_event_id,
                        "stream": stream,
                    },
                    interaction_get_params.InteractionGetParams,
                ),
            ),
            cast_to=Interaction,
            stream=stream or False,
            stream_cls=AsyncStream[InteractionSSEEvent],
        )


class InteractionsResourceWithRawResponse:
    def __init__(self, interactions: InteractionsResource) -> None:
        self._interactions = interactions

        self.create = to_raw_response_wrapper(
            interactions.create,
        )
        self.delete = to_raw_response_wrapper(
            interactions.delete,
        )
        self.cancel = to_raw_response_wrapper(
            interactions.cancel,
        )
        self.get = to_raw_response_wrapper(
            interactions.get,
        )


class AsyncInteractionsResourceWithRawResponse:
    def __init__(self, interactions: AsyncInteractionsResource) -> None:
        self._interactions = interactions

        self.create = async_to_raw_response_wrapper(
            interactions.create,
        )
        self.delete = async_to_raw_response_wrapper(
            interactions.delete,
        )
        self.cancel = async_to_raw_response_wrapper(
            interactions.cancel,
        )
        self.get = async_to_raw_response_wrapper(
            interactions.get,
        )


class InteractionsResourceWithStreamingResponse:
    def __init__(self, interactions: InteractionsResource) -> None:
        self._interactions = interactions

        self.create = to_streamed_response_wrapper(
            interactions.create,
        )
        self.delete = to_streamed_response_wrapper(
            interactions.delete,
        )
        self.cancel = to_streamed_response_wrapper(
            interactions.cancel,
        )
        self.get = to_streamed_response_wrapper(
            interactions.get,
        )


class AsyncInteractionsResourceWithStreamingResponse:
    def __init__(self, interactions: AsyncInteractionsResource) -> None:
        self._interactions = interactions

        self.create = async_to_streamed_response_wrapper(
            interactions.create,
        )
        self.delete = async_to_streamed_response_wrapper(
            interactions.delete,
        )
        self.cancel = async_to_streamed_response_wrapper(
            interactions.cancel,
        )
        self.get = async_to_streamed_response_wrapper(
            interactions.get,
        )
