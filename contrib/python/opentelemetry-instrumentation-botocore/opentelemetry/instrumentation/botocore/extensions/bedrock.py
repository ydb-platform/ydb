# Copyright The OpenTelemetry Authors
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

# Includes work from:
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: disable=too-many-lines

from __future__ import annotations

import io
import json
import logging
from timeit import default_timer
from typing import Any

from botocore.eventstream import EventStream
from botocore.response import StreamingBody

from opentelemetry.context import get_current
from opentelemetry.instrumentation.botocore.extensions.bedrock_utils import (
    ConverseStreamWrapper,
    InvokeModelWithResponseStreamWrapper,
    _Choice,
    estimate_token_count,
    genai_capture_message_content,
    message_to_event,
)
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AttributeMapT,
    _AwsSdkExtension,
    _BotoClientErrorT,
    _BotocoreInstrumentorContext,
)
from opentelemetry.instrumentation.botocore.utils import get_server_attributes
from opentelemetry.metrics import Instrument, Meter
from opentelemetry.semconv._incubating.attributes.error_attributes import (
    ERROR_TYPE,
)
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_OPERATION_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_REQUEST_STOP_SEQUENCES,
    GEN_AI_REQUEST_TEMPERATURE,
    GEN_AI_REQUEST_TOP_P,
    GEN_AI_RESPONSE_FINISH_REASONS,
    GEN_AI_SYSTEM,
    GEN_AI_TOKEN_TYPE,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GenAiOperationNameValues,
    GenAiSystemValues,
    GenAiTokenTypeValues,
)
from opentelemetry.semconv._incubating.metrics.gen_ai_metrics import (
    GEN_AI_CLIENT_OPERATION_DURATION,
    GEN_AI_CLIENT_TOKEN_USAGE,
)
from opentelemetry.trace.propagation import set_span_in_context
from opentelemetry.trace.span import Span
from opentelemetry.trace.status import Status, StatusCode

_logger = logging.getLogger(__name__)

_GEN_AI_CLIENT_OPERATION_DURATION_BUCKETS = [
    0.01,
    0.02,
    0.04,
    0.08,
    0.16,
    0.32,
    0.64,
    1.28,
    2.56,
    5.12,
    10.24,
    20.48,
    40.96,
    81.92,
]

_GEN_AI_CLIENT_TOKEN_USAGE_BUCKETS = [
    1,
    4,
    16,
    64,
    256,
    1024,
    4096,
    16384,
    65536,
    262144,
    1048576,
    4194304,
    16777216,
    67108864,
]

_MODEL_ID_KEY: str = "modelId"


class _BedrockRuntimeExtension(_AwsSdkExtension):
    """
    This class is an extension for <a
    href="https://docs.aws.amazon.com/bedrock/latest/APIReference/API_Operations_Amazon_Bedrock_Runtime.html">
    Amazon Bedrock Runtime</a>.
    """

    _HANDLED_OPERATIONS = {
        "Converse",
        "ConverseStream",
        "InvokeModel",
        "InvokeModelWithResponseStream",
    }
    _DONT_CLOSE_SPAN_ON_END_OPERATIONS = {
        "ConverseStream",
        "InvokeModelWithResponseStream",
    }

    def should_end_span_on_exit(self):
        return (
            self._call_context.operation
            not in self._DONT_CLOSE_SPAN_ON_END_OPERATIONS
        )

    def setup_metrics(self, meter: Meter, metrics: dict[str, Instrument]):
        metrics[GEN_AI_CLIENT_OPERATION_DURATION] = meter.create_histogram(
            name=GEN_AI_CLIENT_OPERATION_DURATION,
            description="GenAI operation duration",
            unit="s",
            explicit_bucket_boundaries_advisory=_GEN_AI_CLIENT_OPERATION_DURATION_BUCKETS,
        )
        metrics[GEN_AI_CLIENT_TOKEN_USAGE] = meter.create_histogram(
            name=GEN_AI_CLIENT_TOKEN_USAGE,
            description="Measures number of input and output tokens used",
            unit="{token}",
            explicit_bucket_boundaries_advisory=_GEN_AI_CLIENT_TOKEN_USAGE_BUCKETS,
        )

    def _extract_metrics_attributes(self) -> _AttributeMapT:
        attributes = {
            GEN_AI_SYSTEM: GenAiSystemValues.AWS_BEDROCK.value,
            **get_server_attributes(self._call_context.endpoint_url),
        }

        model_id = self._call_context.params.get(_MODEL_ID_KEY)
        if not model_id:
            return attributes

        attributes[GEN_AI_REQUEST_MODEL] = model_id

        # titan in invoke model is a text completion one
        if "body" in self._call_context.params and "amazon.titan" in model_id:
            attributes[GEN_AI_OPERATION_NAME] = (
                GenAiOperationNameValues.TEXT_COMPLETION.value
            )
        else:
            attributes[GEN_AI_OPERATION_NAME] = (
                GenAiOperationNameValues.CHAT.value
            )

        return attributes

    def extract_attributes(self, attributes: _AttributeMapT):
        if self._call_context.operation not in self._HANDLED_OPERATIONS:
            return

        attributes[GEN_AI_SYSTEM] = GenAiSystemValues.AWS_BEDROCK.value

        model_id = self._call_context.params.get(_MODEL_ID_KEY)
        if model_id:
            attributes[GEN_AI_REQUEST_MODEL] = model_id
            attributes[GEN_AI_OPERATION_NAME] = (
                GenAiOperationNameValues.CHAT.value
            )

            # Converse / ConverseStream
            if inference_config := self._call_context.params.get(
                "inferenceConfig"
            ):
                self._set_if_not_none(
                    attributes,
                    GEN_AI_REQUEST_TEMPERATURE,
                    inference_config.get("temperature"),
                )
                self._set_if_not_none(
                    attributes,
                    GEN_AI_REQUEST_TOP_P,
                    inference_config.get("topP"),
                )
                self._set_if_not_none(
                    attributes,
                    GEN_AI_REQUEST_MAX_TOKENS,
                    inference_config.get("maxTokens"),
                )
                self._set_if_not_none(
                    attributes,
                    GEN_AI_REQUEST_STOP_SEQUENCES,
                    inference_config.get("stopSequences"),
                )

            # InvokeModel
            # Get the request body if it exists
            body = self._call_context.params.get("body")
            if body:
                try:
                    request_body = json.loads(body)

                    if "amazon.titan" in model_id:
                        # titan interface is a text completion one
                        attributes[GEN_AI_OPERATION_NAME] = (
                            GenAiOperationNameValues.TEXT_COMPLETION.value
                        )
                        self._extract_titan_attributes(
                            attributes, request_body
                        )
                    elif "amazon.nova" in model_id:
                        self._extract_nova_attributes(attributes, request_body)
                    elif "anthropic.claude" in model_id:
                        self._extract_claude_attributes(
                            attributes, request_body
                        )
                    elif "cohere.command-r" in model_id:
                        self._extract_command_r_attributes(
                            attributes, request_body
                        )
                    elif "cohere.command" in model_id:
                        self._extract_command_attributes(
                            attributes, request_body
                        )
                    elif "meta.llama" in model_id:
                        self._extract_llama_attributes(
                            attributes, request_body
                        )
                    elif "mistral" in model_id:
                        self._extract_mistral_attributes(
                            attributes, request_body
                        )

                except json.JSONDecodeError:
                    _logger.debug("Error: Unable to parse the body as JSON")

    def _extract_titan_attributes(self, attributes, request_body):
        config = request_body.get("textGenerationConfig", {})
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_TEMPERATURE, config.get("temperature")
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_TOP_P, config.get("topP")
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_MAX_TOKENS, config.get("maxTokenCount")
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_STOP_SEQUENCES,
            config.get("stopSequences"),
        )

    def _extract_nova_attributes(self, attributes, request_body):
        config = request_body.get("inferenceConfig", {})
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_TEMPERATURE, config.get("temperature")
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_TOP_P, config.get("topP")
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_MAX_TOKENS, config.get("max_new_tokens")
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_STOP_SEQUENCES,
            config.get("stopSequences"),
        )

    def _extract_claude_attributes(self, attributes, request_body):
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_MAX_TOKENS,
            request_body.get("max_tokens"),
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_TEMPERATURE,
            request_body.get("temperature"),
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_TOP_P, request_body.get("top_p")
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_STOP_SEQUENCES,
            request_body.get("stop_sequences"),
        )

    def _extract_command_r_attributes(self, attributes, request_body):
        prompt = request_body.get("message")
        self._set_if_not_none(
            attributes, GEN_AI_USAGE_INPUT_TOKENS, estimate_token_count(prompt)
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_MAX_TOKENS,
            request_body.get("max_tokens"),
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_TEMPERATURE,
            request_body.get("temperature"),
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_TOP_P, request_body.get("p")
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_STOP_SEQUENCES,
            request_body.get("stop_sequences"),
        )

    def _extract_command_attributes(self, attributes, request_body):
        prompt = request_body.get("prompt")
        self._set_if_not_none(
            attributes, GEN_AI_USAGE_INPUT_TOKENS, estimate_token_count(prompt)
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_MAX_TOKENS,
            request_body.get("max_tokens"),
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_TEMPERATURE,
            request_body.get("temperature"),
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_TOP_P, request_body.get("p")
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_STOP_SEQUENCES,
            request_body.get("stop_sequences"),
        )

    def _extract_llama_attributes(self, attributes, request_body):
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_MAX_TOKENS,
            request_body.get("max_gen_len"),
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_TEMPERATURE,
            request_body.get("temperature"),
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_TOP_P, request_body.get("top_p")
        )
        # request for meta llama models does not contain stop_sequences field

    def _extract_mistral_attributes(self, attributes, request_body):
        prompt = request_body.get("prompt")
        if prompt:
            self._set_if_not_none(
                attributes,
                GEN_AI_USAGE_INPUT_TOKENS,
                estimate_token_count(prompt),
            )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_MAX_TOKENS,
            request_body.get("max_tokens"),
        )
        self._set_if_not_none(
            attributes,
            GEN_AI_REQUEST_TEMPERATURE,
            request_body.get("temperature"),
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_TOP_P, request_body.get("top_p")
        )
        self._set_if_not_none(
            attributes, GEN_AI_REQUEST_STOP_SEQUENCES, request_body.get("stop")
        )

    @staticmethod
    def _set_if_not_none(attributes, key, value):
        if value is not None:
            attributes[key] = value

    def _get_request_messages(self):
        """Extracts and normalize system and user / assistant messages"""
        if system := self._call_context.params.get("system", []):
            system_messages = [{"role": "system", "content": system}]
        else:
            system_messages = []

        if not (messages := self._call_context.params.get("messages", [])):
            if body := self._call_context.params.get("body"):
                decoded_body = json.loads(body)
                if system := decoded_body.get("system"):
                    if isinstance(system, str):
                        content = [{"text": system}]
                    else:
                        content = system
                    system_messages = [{"role": "system", "content": content}]

                messages = decoded_body.get("messages", [])
                # if no messages interface, convert to messages format from generic API
                if not messages:
                    model_id = self._call_context.params.get(_MODEL_ID_KEY)
                    if "amazon.titan" in model_id:
                        messages = self._get_messages_from_input_text(
                            decoded_body, "inputText"
                        )
                    elif "cohere.command-r" in model_id:
                        # chat_history can be converted to messages; for now, just use message
                        messages = self._get_messages_from_input_text(
                            decoded_body, "message"
                        )
                    elif (
                        "cohere.command" in model_id
                        or "meta.llama" in model_id
                        or "mistral.mistral" in model_id
                    ):
                        messages = self._get_messages_from_input_text(
                            decoded_body, "prompt"
                        )

        return system_messages + messages

    # pylint: disable=no-self-use
    def _get_messages_from_input_text(
        self, decoded_body: dict[str, Any], input_name: str
    ):
        if input_text := decoded_body.get(input_name):
            return [{"role": "user", "content": [{"text": input_text}]}]
        return []

    def before_service_call(
        self, span: Span, instrumentor_context: _BotocoreInstrumentorContext
    ):
        if self._call_context.operation not in self._HANDLED_OPERATIONS:
            return

        capture_content = genai_capture_message_content()

        messages = self._get_request_messages()
        for message in messages:
            logger = instrumentor_context.logger
            for event in message_to_event(message, capture_content):
                logger.emit(event)

        if span.is_recording():
            operation_name = span.attributes.get(GEN_AI_OPERATION_NAME, "")
            request_model = span.attributes.get(GEN_AI_REQUEST_MODEL, "")
            # avoid setting to an empty string if are not available
            if operation_name and request_model:
                span.update_name(f"{operation_name} {request_model}")

        # this is used to calculate the operation duration metric, duration may be skewed by request_hook
        # pylint: disable=attribute-defined-outside-init
        self._operation_start = default_timer()

    # pylint: disable=no-self-use,too-many-locals
    def _converse_on_success(
        self,
        span: Span,
        result: dict[str, Any],
        instrumentor_context: _BotocoreInstrumentorContext,
        capture_content,
    ):
        if span.is_recording():
            if usage := result.get("usage"):
                if input_tokens := usage.get("inputTokens"):
                    span.set_attribute(
                        GEN_AI_USAGE_INPUT_TOKENS,
                        input_tokens,
                    )
                if output_tokens := usage.get("outputTokens"):
                    span.set_attribute(
                        GEN_AI_USAGE_OUTPUT_TOKENS,
                        output_tokens,
                    )

            if stop_reason := result.get("stopReason"):
                span.set_attribute(
                    GEN_AI_RESPONSE_FINISH_REASONS,
                    [stop_reason],
                )

        # In case of an early stream closure, the result may not contain outputs
        if self._stream_has_output_content(result):
            logger = instrumentor_context.logger
            choice = _Choice.from_converse(result, capture_content)
            # this path is used by streaming apis, in that case we are already out of the span
            # context so need to add the span context manually
            context = set_span_in_context(span, get_current())
            logger.emit(choice.to_choice_event(context=context))

        metrics = instrumentor_context.metrics
        metrics_attributes = self._extract_metrics_attributes()
        if operation_duration_histogram := metrics.get(
            GEN_AI_CLIENT_OPERATION_DURATION
        ):
            duration = max((default_timer() - self._operation_start), 0)
            operation_duration_histogram.record(
                duration,
                attributes=metrics_attributes,
            )

        if token_usage_histogram := metrics.get(GEN_AI_CLIENT_TOKEN_USAGE):
            if usage := result.get("usage"):
                if input_tokens := usage.get("inputTokens"):
                    input_attributes = {
                        **metrics_attributes,
                        GEN_AI_TOKEN_TYPE: GenAiTokenTypeValues.INPUT.value,
                    }
                    token_usage_histogram.record(
                        input_tokens, input_attributes
                    )

                if output_tokens := usage.get("outputTokens"):
                    output_attributes = {
                        **metrics_attributes,
                        GEN_AI_TOKEN_TYPE: GenAiTokenTypeValues.COMPLETION.value,
                    }
                    token_usage_histogram.record(
                        output_tokens, output_attributes
                    )

    def _invoke_model_on_success(
        self,
        span: Span,
        result: dict[str, Any],
        model_id: str,
        instrumentor_context: _BotocoreInstrumentorContext,
        capture_content,
    ):
        original_body = None
        try:
            original_body = result["body"]
            body_content = original_body.read()

            # Replenish stream for downstream application use
            new_stream = io.BytesIO(body_content)
            result["body"] = StreamingBody(new_stream, len(body_content))

            response_body = json.loads(body_content.decode("utf-8"))
            if "amazon.titan" in model_id:
                self._handle_amazon_titan_response(
                    span, response_body, instrumentor_context, capture_content
                )
            elif "amazon.nova" in model_id:
                self._handle_amazon_nova_response(
                    span, response_body, instrumentor_context, capture_content
                )
            elif "anthropic.claude" in model_id:
                self._handle_anthropic_claude_response(
                    span, response_body, instrumentor_context, capture_content
                )
            elif "cohere.command-r" in model_id:
                self._handle_cohere_command_r_response(
                    span, response_body, instrumentor_context, capture_content
                )
            elif "cohere.command" in model_id:
                self._handle_cohere_command_response(
                    span, response_body, instrumentor_context, capture_content
                )
            elif "meta.llama" in model_id:
                self._handle_meta_llama_response(
                    span, response_body, instrumentor_context, capture_content
                )
            elif "mistral" in model_id:
                self._handle_mistral_ai_response(
                    span, response_body, instrumentor_context, capture_content
                )
        except json.JSONDecodeError:
            _logger.debug("Error: Unable to parse the response body as JSON")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _logger.debug("Error processing response: %s", exc)
        finally:
            if original_body is not None:
                original_body.close()

    def _on_stream_error_callback(
        self,
        span: Span,
        exception,
        instrumentor_context: _BotocoreInstrumentorContext,
        span_ended: bool,
    ):
        span.set_status(Status(StatusCode.ERROR, str(exception)))
        if span.is_recording():
            span.set_attribute(ERROR_TYPE, type(exception).__qualname__)

        if not span_ended:
            span.end()

        metrics = instrumentor_context.metrics
        metrics_attributes = {
            **self._extract_metrics_attributes(),
            ERROR_TYPE: type(exception).__qualname__,
        }
        if operation_duration_histogram := metrics.get(
            GEN_AI_CLIENT_OPERATION_DURATION
        ):
            duration = max((default_timer() - self._operation_start), 0)
            operation_duration_histogram.record(
                duration,
                attributes=metrics_attributes,
            )

    def on_success(
        self,
        span: Span,
        result: dict[str, Any],
        instrumentor_context: _BotocoreInstrumentorContext,
    ):
        if self._call_context.operation not in self._HANDLED_OPERATIONS:
            return

        capture_content = genai_capture_message_content()

        if self._call_context.operation == "ConverseStream":
            if "stream" in result and isinstance(
                result["stream"], EventStream
            ):

                def stream_done_callback(response, span_ended):
                    self._converse_on_success(
                        span, response, instrumentor_context, capture_content
                    )

                    if not span_ended:
                        span.end()

                def stream_error_callback(exception, span_ended):
                    self._on_stream_error_callback(
                        span, exception, instrumentor_context, span_ended
                    )

                result["stream"] = ConverseStreamWrapper(
                    result["stream"],
                    stream_done_callback,
                    stream_error_callback,
                )
                return
        elif self._call_context.operation == "Converse":
            self._converse_on_success(
                span, result, instrumentor_context, capture_content
            )

        model_id = self._call_context.params.get(_MODEL_ID_KEY)
        if not model_id:
            return

        if self._call_context.operation == "InvokeModel":
            if "body" in result and isinstance(result["body"], StreamingBody):
                self._invoke_model_on_success(
                    span,
                    result,
                    model_id,
                    instrumentor_context,
                    capture_content,
                )
                return
        elif self._call_context.operation == "InvokeModelWithResponseStream":
            if "body" in result and isinstance(result["body"], EventStream):

                def invoke_model_stream_done_callback(response, span_ended):
                    # the callback gets data formatted as the simpler converse API
                    self._converse_on_success(
                        span, response, instrumentor_context, capture_content
                    )
                    if not span_ended:
                        span.end()

                def invoke_model_stream_error_callback(exception, span_ended):
                    self._on_stream_error_callback(
                        span, exception, instrumentor_context, span_ended
                    )

                result["body"] = InvokeModelWithResponseStreamWrapper(
                    result["body"],
                    invoke_model_stream_done_callback,
                    invoke_model_stream_error_callback,
                    model_id,
                )
                return

    # pylint: disable=no-self-use,too-many-locals
    def _handle_amazon_titan_response(
        self,
        span: Span,
        response_body: dict[str, Any],
        instrumentor_context: _BotocoreInstrumentorContext,
        capture_content: bool,
    ):
        if "inputTextTokenCount" in response_body:
            span.set_attribute(
                GEN_AI_USAGE_INPUT_TOKENS, response_body["inputTextTokenCount"]
            )
        if "results" in response_body and response_body["results"]:
            result = response_body["results"][0]
            if "tokenCount" in result:
                span.set_attribute(
                    GEN_AI_USAGE_OUTPUT_TOKENS, result["tokenCount"]
                )
            if "completionReason" in result:
                span.set_attribute(
                    GEN_AI_RESPONSE_FINISH_REASONS,
                    [result["completionReason"]],
                )

            logger = instrumentor_context.logger
            choice = _Choice.from_invoke_amazon_titan(
                response_body, capture_content
            )
            logger.emit(choice.to_choice_event())

            metrics = instrumentor_context.metrics
            metrics_attributes = self._extract_metrics_attributes()
            if operation_duration_histogram := metrics.get(
                GEN_AI_CLIENT_OPERATION_DURATION
            ):
                duration = max((default_timer() - self._operation_start), 0)
                operation_duration_histogram.record(
                    duration,
                    attributes=metrics_attributes,
                )

            if token_usage_histogram := metrics.get(GEN_AI_CLIENT_TOKEN_USAGE):
                if input_tokens := response_body.get("inputTextTokenCount"):
                    input_attributes = {
                        **metrics_attributes,
                        GEN_AI_TOKEN_TYPE: GenAiTokenTypeValues.INPUT.value,
                    }
                    token_usage_histogram.record(
                        input_tokens, input_attributes
                    )

                if results := response_body.get("results"):
                    if output_tokens := results[0].get("tokenCount"):
                        output_attributes = {
                            **metrics_attributes,
                            GEN_AI_TOKEN_TYPE: GenAiTokenTypeValues.COMPLETION.value,
                        }
                        token_usage_histogram.record(
                            output_tokens, output_attributes
                        )

    # pylint: disable=no-self-use,too-many-locals
    def _handle_amazon_nova_response(
        self,
        span: Span,
        response_body: dict[str, Any],
        instrumentor_context: _BotocoreInstrumentorContext,
        capture_content: bool,
    ):
        if "usage" in response_body:
            usage = response_body["usage"]
            if "inputTokens" in usage:
                span.set_attribute(
                    GEN_AI_USAGE_INPUT_TOKENS, usage["inputTokens"]
                )
            if "outputTokens" in usage:
                span.set_attribute(
                    GEN_AI_USAGE_OUTPUT_TOKENS, usage["outputTokens"]
                )
        if "stopReason" in response_body:
            span.set_attribute(
                GEN_AI_RESPONSE_FINISH_REASONS, [response_body["stopReason"]]
            )

        # In case of an early stream closure, the result may not contain outputs
        if self._stream_has_output_content(response_body):
            logger = instrumentor_context.logger
            choice = _Choice.from_converse(response_body, capture_content)
            logger.emit(choice.to_choice_event())

        metrics = instrumentor_context.metrics
        metrics_attributes = self._extract_metrics_attributes()
        if operation_duration_histogram := metrics.get(
            GEN_AI_CLIENT_OPERATION_DURATION
        ):
            duration = max((default_timer() - self._operation_start), 0)
            operation_duration_histogram.record(
                duration,
                attributes=metrics_attributes,
            )

        if token_usage_histogram := metrics.get(GEN_AI_CLIENT_TOKEN_USAGE):
            if usage := response_body.get("usage"):
                if input_tokens := usage.get("inputTokens"):
                    input_attributes = {
                        **metrics_attributes,
                        GEN_AI_TOKEN_TYPE: GenAiTokenTypeValues.INPUT.value,
                    }
                    token_usage_histogram.record(
                        input_tokens, input_attributes
                    )

                if output_tokens := usage.get("outputTokens"):
                    output_attributes = {
                        **metrics_attributes,
                        GEN_AI_TOKEN_TYPE: GenAiTokenTypeValues.COMPLETION.value,
                    }
                    token_usage_histogram.record(
                        output_tokens, output_attributes
                    )

    # pylint: disable=no-self-use
    def _handle_anthropic_claude_response(
        self,
        span: Span,
        response_body: dict[str, Any],
        instrumentor_context: _BotocoreInstrumentorContext,
        capture_content: bool,
    ):
        if usage := response_body.get("usage"):
            if "input_tokens" in usage:
                span.set_attribute(
                    GEN_AI_USAGE_INPUT_TOKENS, usage["input_tokens"]
                )
            if "output_tokens" in usage:
                span.set_attribute(
                    GEN_AI_USAGE_OUTPUT_TOKENS, usage["output_tokens"]
                )
        if "stop_reason" in response_body:
            span.set_attribute(
                GEN_AI_RESPONSE_FINISH_REASONS, [response_body["stop_reason"]]
            )

        logger = instrumentor_context.logger
        choice = _Choice.from_invoke_anthropic_claude(
            response_body, capture_content
        )
        logger.emit(choice.to_choice_event())

        metrics = instrumentor_context.metrics
        metrics_attributes = self._extract_metrics_attributes()
        if operation_duration_histogram := metrics.get(
            GEN_AI_CLIENT_OPERATION_DURATION
        ):
            duration = max((default_timer() - self._operation_start), 0)
            operation_duration_histogram.record(
                duration,
                attributes=metrics_attributes,
            )

        if token_usage_histogram := metrics.get(GEN_AI_CLIENT_TOKEN_USAGE):
            if usage := response_body.get("usage"):
                if input_tokens := usage.get("input_tokens"):
                    input_attributes = {
                        **metrics_attributes,
                        GEN_AI_TOKEN_TYPE: GenAiTokenTypeValues.INPUT.value,
                    }
                    token_usage_histogram.record(
                        input_tokens, input_attributes
                    )

                if output_tokens := usage.get("output_tokens"):
                    output_attributes = {
                        **metrics_attributes,
                        GEN_AI_TOKEN_TYPE: GenAiTokenTypeValues.COMPLETION.value,
                    }
                    token_usage_histogram.record(
                        output_tokens, output_attributes
                    )

    def _handle_cohere_command_r_response(
        self,
        span: Span,
        response_body: dict[str, Any],
        instrumentor_context: _BotocoreInstrumentorContext,
        capture_content: bool,
    ):
        if "text" in response_body:
            span.set_attribute(
                GEN_AI_USAGE_OUTPUT_TOKENS,
                estimate_token_count(response_body["text"]),
            )
        if "finish_reason" in response_body:
            span.set_attribute(
                GEN_AI_RESPONSE_FINISH_REASONS,
                [response_body["finish_reason"]],
            )

        logger = instrumentor_context.logger
        choice = _Choice.from_invoke_cohere_command_r(
            response_body, capture_content
        )
        logger.emit(choice.to_choice_event())

    def _handle_cohere_command_response(
        self,
        span: Span,
        response_body: dict[str, Any],
        instrumentor_context: _BotocoreInstrumentorContext,
        capture_content: bool,
    ):
        if "generations" in response_body and response_body["generations"]:
            generations = response_body["generations"][0]
            if "text" in generations:
                span.set_attribute(
                    GEN_AI_USAGE_OUTPUT_TOKENS,
                    estimate_token_count(generations["text"]),
                )
            if "finish_reason" in generations:
                span.set_attribute(
                    GEN_AI_RESPONSE_FINISH_REASONS,
                    [generations["finish_reason"]],
                )

        logger = instrumentor_context.logger
        choice = _Choice.from_invoke_cohere_command(
            response_body, capture_content
        )
        logger.emit(choice.to_choice_event())

    def _handle_meta_llama_response(
        self,
        span: Span,
        response_body: dict[str, Any],
        instrumentor_context: _BotocoreInstrumentorContext,
        capture_content: bool,
    ):
        if "prompt_token_count" in response_body:
            span.set_attribute(
                GEN_AI_USAGE_INPUT_TOKENS, response_body["prompt_token_count"]
            )
        if "generation_token_count" in response_body:
            span.set_attribute(
                GEN_AI_USAGE_OUTPUT_TOKENS,
                response_body["generation_token_count"],
            )
        if "stop_reason" in response_body:
            span.set_attribute(
                GEN_AI_RESPONSE_FINISH_REASONS, [response_body["stop_reason"]]
            )

        logger = instrumentor_context.logger
        choice = _Choice.from_invoke_meta_llama(response_body, capture_content)
        logger.emit(choice.to_choice_event())

    def _handle_mistral_ai_response(
        self,
        span: Span,
        response_body: dict[str, Any],
        instrumentor_context: _BotocoreInstrumentorContext,
        capture_content: bool,
    ):
        if "outputs" in response_body:
            outputs = response_body["outputs"][0]
            if "text" in outputs:
                span.set_attribute(
                    GEN_AI_USAGE_OUTPUT_TOKENS,
                    estimate_token_count(outputs["text"]),
                )
            if "stop_reason" in outputs:
                span.set_attribute(
                    GEN_AI_RESPONSE_FINISH_REASONS, [outputs["stop_reason"]]
                )

        logger = instrumentor_context.logger
        choice = _Choice.from_invoke_mistral_mistral(
            response_body, capture_content
        )
        logger.emit(choice.to_choice_event())

    def on_error(
        self,
        span: Span,
        exception: _BotoClientErrorT,
        instrumentor_context: _BotocoreInstrumentorContext,
    ):
        if self._call_context.operation not in self._HANDLED_OPERATIONS:
            return

        span.set_status(Status(StatusCode.ERROR, str(exception)))
        if span.is_recording():
            span.set_attribute(ERROR_TYPE, type(exception).__qualname__)

        if not self.should_end_span_on_exit():
            span.end()

        metrics = instrumentor_context.metrics
        metrics_attributes = {
            **self._extract_metrics_attributes(),
            ERROR_TYPE: type(exception).__qualname__,
        }
        if operation_duration_histogram := metrics.get(
            GEN_AI_CLIENT_OPERATION_DURATION
        ):
            duration = max((default_timer() - self._operation_start), 0)
            operation_duration_histogram.record(
                duration,
                attributes=metrics_attributes,
            )

    def _stream_has_output_content(self, response_body: dict[str, Any]):
        return (
            "output" in response_body and "message" in response_body["output"]
        )
