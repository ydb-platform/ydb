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

from __future__ import annotations

import json
import math
from os import environ
from typing import Any, Callable, Dict, Iterator, Sequence, Union

from botocore.eventstream import EventStream, EventStreamError
from wrapt import ObjectProxy

from opentelemetry._logs import LogRecord
from opentelemetry.context import get_current
from opentelemetry.instrumentation.botocore.environment_variables import (
    OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT,
)
from opentelemetry.semconv._incubating.attributes.gen_ai_attributes import (
    GEN_AI_SYSTEM,
    GenAiSystemValues,
)

_StreamDoneCallableT = Callable[[Dict[str, Union[int, str]]], None]
_StreamErrorCallableT = Callable[[Exception], None]


# pylint: disable=abstract-method
class ConverseStreamWrapper(ObjectProxy):
    """Wrapper for botocore.eventstream.EventStream"""

    def __init__(
        self,
        stream: EventStream,
        stream_done_callback: _StreamDoneCallableT,
        stream_error_callback: _StreamErrorCallableT,
    ):
        super().__init__(stream)

        self._stream_done_callback = stream_done_callback
        self._stream_error_callback = stream_error_callback
        # accumulating things in the same shape of non-streaming version
        # {"usage": {"inputTokens": 0, "outputTokens": 0}, "stopReason": "finish", "output": {"message": {"role": "", "content": [{"text": ""}]}
        self._response = {}
        self._message = None
        self._content_block = {}
        self._tool_json_input_buf = ""
        self._record_message = False
        self._ended = False

    def __iter__(self):
        try:
            for event in self.__wrapped__:
                self._process_event(event)
                yield event
        except EventStreamError as exc:
            self._handle_stream_error(exc)
            raise

    def _process_event(self, event):
        # pylint: disable=too-many-branches
        if "messageStart" in event:
            # {'messageStart': {'role': 'assistant'}}
            if event["messageStart"].get("role") == "assistant":
                self._record_message = True
                self._message = {"role": "assistant", "content": []}
            return

        if "contentBlockStart" in event:
            # {'contentBlockStart': {'start': {'toolUse': {'toolUseId': 'id', 'name': 'func_name'}}, 'contentBlockIndex': 1}}
            start = event["contentBlockStart"].get("start", {})
            if "toolUse" in start:
                self._content_block = {"toolUse": start["toolUse"]}
            return

        if "contentBlockDelta" in event:
            # {'contentBlockDelta': {'delta': {'text': "Hello"}, 'contentBlockIndex': 0}}
            # {'contentBlockDelta': {'delta': {'toolUse': {'input': '{"location":"Seattle"}'}}, 'contentBlockIndex': 1}}
            # {'contentBlockDelta': {'delta': {'toolUse': {'input': 'a", "Yok'}}, 'contentBlockIndex': 1}}
            if self._record_message:
                delta = event["contentBlockDelta"].get("delta", {})
                if "text" in delta:
                    self._content_block.setdefault("text", "")
                    self._content_block["text"] += delta["text"]
                elif "toolUse" in delta:
                    if (
                        input_buf := delta["toolUse"].get("input")
                    ) is not None:
                        self._tool_json_input_buf += input_buf
            return

        if "contentBlockStop" in event:
            # {'contentBlockStop': {'contentBlockIndex': 0}}
            if self._record_message:
                if self._tool_json_input_buf:
                    try:
                        self._content_block["toolUse"]["input"] = json.loads(
                            self._tool_json_input_buf
                        )
                    except json.JSONDecodeError:
                        self._content_block["toolUse"]["input"] = (
                            self._tool_json_input_buf
                        )
                self._message["content"].append(self._content_block)
                self._content_block = {}
                self._tool_json_input_buf = ""
            return

        if "messageStop" in event:
            # {'messageStop': {'stopReason': 'end_turn'}}
            if stop_reason := event["messageStop"].get("stopReason"):
                self._response["stopReason"] = stop_reason

            if self._record_message:
                self._response["output"] = {"message": self._message}
                self._record_message = False
                self._message = None

            return

        if "metadata" in event:
            # {'metadata': {'usage': {'inputTokens': 12, 'outputTokens': 15, 'totalTokens': 27}, 'metrics': {'latencyMs': 2980}}}
            if usage := event["metadata"].get("usage"):
                self._response["usage"] = {}
                if input_tokens := usage.get("inputTokens"):
                    self._response["usage"]["inputTokens"] = input_tokens

                if output_tokens := usage.get("outputTokens"):
                    self._response["usage"]["outputTokens"] = output_tokens
            self._complete_stream(self._response)

            return

    def close(self):
        self.__wrapped__.close()
        # Treat the stream as done to ensure the span end.
        self._complete_stream(self._response)

    def _complete_stream(self, response):
        self._stream_done_callback(response, self._ended)
        self._ended = True

    def _handle_stream_error(self, exc):
        self._stream_error_callback(exc, self._ended)
        self._ended = True


# pylint: disable=abstract-method
class InvokeModelWithResponseStreamWrapper(ObjectProxy):
    """Wrapper for botocore.eventstream.EventStream"""

    def __init__(
        self,
        stream: EventStream,
        stream_done_callback: _StreamDoneCallableT,
        stream_error_callback: _StreamErrorCallableT,
        model_id: str,
    ):
        super().__init__(stream)

        self._stream_done_callback = stream_done_callback
        self._stream_error_callback = stream_error_callback
        self._model_id = model_id

        # accumulating things in the same shape of the Converse API
        # {"usage": {"inputTokens": 0, "outputTokens": 0}, "stopReason": "finish", "output": {"message": {"role": "", "content": [{"text": ""}]}
        self._response = {}
        self._message = None
        self._content_block = {}
        self._tool_json_input_buf = ""
        self._record_message = False
        self._ended = False

    def close(self):
        self.__wrapped__.close()
        # Treat the stream as done to ensure the span end.
        self._stream_done_callback(self._response, self._ended)

    def _complete_stream(self, response):
        self._stream_done_callback(response, self._ended)
        self._ended = True

    def _handle_stream_error(self, exc):
        self._stream_error_callback(exc, self._ended)
        self._ended = True

    def __iter__(self):
        try:
            for event in self.__wrapped__:
                self._process_event(event)
                yield event
        except EventStreamError as exc:
            self._handle_stream_error(exc)
            raise

    def _process_event(self, event):
        if "chunk" not in event:
            return

        json_bytes = event["chunk"].get("bytes", b"")
        decoded = json_bytes.decode("utf-8")
        try:
            chunk = json.loads(decoded)
        except json.JSONDecodeError:
            return

        if "amazon.titan" in self._model_id:
            self._process_amazon_titan_chunk(chunk)
        elif "amazon.nova" in self._model_id:
            self._process_amazon_nova_chunk(chunk)
        elif "anthropic.claude" in self._model_id:
            self._process_anthropic_claude_chunk(chunk)

    def _process_invocation_metrics(self, invocation_metrics):
        self._response["usage"] = {}
        if input_tokens := invocation_metrics.get("inputTokenCount"):
            self._response["usage"]["inputTokens"] = input_tokens

        if output_tokens := invocation_metrics.get("outputTokenCount"):
            self._response["usage"]["outputTokens"] = output_tokens

    def _process_amazon_titan_chunk(self, chunk):
        if (stop_reason := chunk.get("completionReason")) is not None:
            self._response["stopReason"] = stop_reason

        if invocation_metrics := chunk.get("amazon-bedrock-invocationMetrics"):
            # "amazon-bedrock-invocationMetrics":{
            #     "inputTokenCount":9,"outputTokenCount":128,"invocationLatency":3569,"firstByteLatency":2180
            # }
            self._process_invocation_metrics(invocation_metrics)

            # transform the shape of the message to match other models
            self._response["output"] = {
                "message": {"content": [{"text": chunk["outputText"]}]}
            }
            self._complete_stream(self._response)

    def _process_amazon_nova_chunk(self, chunk):
        # pylint: disable=too-many-branches
        if "messageStart" in chunk:
            # {'messageStart': {'role': 'assistant'}}
            if chunk["messageStart"].get("role") == "assistant":
                self._record_message = True
                self._message = {"role": "assistant", "content": []}
            return

        if "contentBlockStart" in chunk:
            # {'contentBlockStart': {'start': {'toolUse': {'toolUseId': 'id', 'name': 'name'}}, 'contentBlockIndex': 31}}
            if self._record_message:
                self._message["content"].append(self._content_block)

                start = chunk["contentBlockStart"].get("start", {})
                if "toolUse" in start:
                    self._content_block = start
                else:
                    self._content_block = {}
            return

        if "contentBlockDelta" in chunk:
            # {'contentBlockDelta': {'delta': {'text': "Hello"}, 'contentBlockIndex': 0}}
            # {'contentBlockDelta': {'delta': {'toolUse': {'input': '{"location":"San Francisco"}'}}, 'contentBlockIndex': 31}}
            if self._record_message:
                delta = chunk["contentBlockDelta"].get("delta", {})
                if "text" in delta:
                    self._content_block.setdefault("text", "")
                    self._content_block["text"] += delta["text"]
                elif "toolUse" in delta:
                    self._content_block.setdefault("toolUse", {})
                    self._content_block["toolUse"]["input"] = json.loads(
                        delta["toolUse"]["input"]
                    )
            return

        if "contentBlockStop" in chunk:
            # {'contentBlockStop': {'contentBlockIndex': 0}}
            if self._record_message:
                # create a new content block only for tools
                if "toolUse" in self._content_block:
                    self._message["content"].append(self._content_block)
                    self._content_block = {}
            return

        if "messageStop" in chunk:
            # {'messageStop': {'stopReason': 'end_turn'}}
            if stop_reason := chunk["messageStop"].get("stopReason"):
                self._response["stopReason"] = stop_reason

            if self._record_message:
                self._message["content"].append(self._content_block)
                self._content_block = {}
                self._response["output"] = {"message": self._message}
                self._record_message = False
                self._message = None
            return

        if "metadata" in chunk:
            # {'metadata': {'usage': {'inputTokens': 8, 'outputTokens': 117}, 'metrics': {}, 'trace': {}}}
            if usage := chunk["metadata"].get("usage"):
                self._response["usage"] = {}
                if input_tokens := usage.get("inputTokens"):
                    self._response["usage"]["inputTokens"] = input_tokens

                if output_tokens := usage.get("outputTokens"):
                    self._response["usage"]["outputTokens"] = output_tokens

            self._complete_stream(self._response)
            return

    def _process_anthropic_claude_chunk(self, chunk):
        # pylint: disable=too-many-return-statements,too-many-branches
        if not (message_type := chunk.get("type")):
            return

        if message_type == "message_start":
            # {'type': 'message_start', 'message': {'id': 'id', 'type': 'message', 'role': 'assistant', 'model': 'claude-2.0', 'content': [], 'stop_reason': None, 'stop_sequence': None, 'usage': {'input_tokens': 18, 'output_tokens': 1}}}
            if chunk.get("message", {}).get("role") == "assistant":
                self._record_message = True
                message = chunk["message"]
                self._message = {
                    "role": message["role"],
                    "content": message.get("content", []),
                }
            return

        if message_type == "content_block_start":
            # {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}}
            # {'type': 'content_block_start', 'index': 1, 'content_block': {'type': 'tool_use', 'id': 'id', 'name': 'func_name', 'input': {}}}
            if self._record_message:
                block = chunk.get("content_block", {})
                if block.get("type") == "text":
                    self._content_block = block
                elif block.get("type") == "tool_use":
                    self._content_block = block
            return

        if message_type == "content_block_delta":
            # {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'Here'}}
            # {'type': 'content_block_delta', 'index': 1, 'delta': {'type': 'input_json_delta', 'partial_json': ''}}
            if self._record_message:
                delta = chunk.get("delta", {})
                if delta.get("type") == "text_delta":
                    self._content_block["text"] += delta.get("text", "")
                elif delta.get("type") == "input_json_delta":
                    self._tool_json_input_buf += delta.get("partial_json", "")
            return

        if message_type == "content_block_stop":
            # {'type': 'content_block_stop', 'index': 0}
            if self._tool_json_input_buf:
                try:
                    self._content_block["input"] = json.loads(
                        self._tool_json_input_buf
                    )
                except json.JSONDecodeError:
                    self._content_block["input"] = self._tool_json_input_buf
            self._message["content"].append(self._content_block)
            self._content_block = {}
            self._tool_json_input_buf = ""
            return

        if message_type == "message_delta":
            # {'type': 'message_delta', 'delta': {'stop_reason': 'end_turn', 'stop_sequence': None}, 'usage': {'output_tokens': 123}}
            if (
                stop_reason := chunk.get("delta", {}).get("stop_reason")
            ) is not None:
                self._response["stopReason"] = stop_reason
            return

        if message_type == "message_stop":
            # {'type': 'message_stop', 'amazon-bedrock-invocationMetrics': {'inputTokenCount': 18, 'outputTokenCount': 123, 'invocationLatency': 5250, 'firstByteLatency': 290}}
            if invocation_metrics := chunk.get(
                "amazon-bedrock-invocationMetrics"
            ):
                self._process_invocation_metrics(invocation_metrics)

            if self._record_message:
                self._response["output"] = {"message": self._message}
                self._record_message = False
                self._message = None

            self._complete_stream(self._response)
            return


def estimate_token_count(message: str) -> int:
    # https://docs.aws.amazon.com/bedrock/latest/userguide/model-customization-prepare.html
    # use 6 chars per token to approximate token count when not provided in response body
    return math.ceil(len(message) / 6)


def genai_capture_message_content() -> bool:
    capture_content = environ.get(
        OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT, "false"
    )
    return capture_content.lower() == "true"


def extract_tool_calls(
    message: dict[str, Any], capture_content: bool
) -> Sequence[Dict[str, Any]] | None:
    content = message.get("content")
    if not content:
        return None

    tool_uses = [item["toolUse"] for item in content if "toolUse" in item]
    if not tool_uses:
        tool_uses = [
            item
            for item in content
            if isinstance(item, dict) and item.get("type") == "tool_use"
        ]
        tool_id_key = "id"
    else:
        tool_id_key = "toolUseId"

    if not tool_uses:
        return None

    tool_calls = []
    for tool_use in tool_uses:
        tool_call = {"type": "function"}
        if call_id := tool_use.get(tool_id_key):
            tool_call["id"] = call_id

        if function_name := tool_use.get("name"):
            tool_call["function"] = {"name": function_name}

        if (function_input := tool_use.get("input")) and capture_content:
            tool_call.setdefault("function", {})
            tool_call["function"]["arguments"] = function_input

        tool_calls.append(tool_call)
    return tool_calls


def extract_tool_results(
    message: dict[str, Any], capture_content: bool
) -> Iterator[Dict[str, Any]]:
    content = message.get("content")
    if not content:
        return

    # langchain sends content as string with InvokeModel and Anthropic Claude
    if isinstance(content, str):
        return

    # Converse format
    tool_results = [
        item["toolResult"] for item in content if "toolResult" in item
    ]
    # InvokeModel anthropic.claude format
    if not tool_results:
        tool_results = [
            item for item in content if item.get("type") == "tool_result"
        ]
        tool_id_key = "tool_use_id"
    else:
        tool_id_key = "toolUseId"

    if not tool_results:
        return

    # if we have a user message with toolResult keys we need to send
    # one tool event for each part of the content
    for tool_result in tool_results:
        body = {}
        if tool_id := tool_result.get(tool_id_key):
            body["id"] = tool_id
        tool_content = tool_result.get("content")
        if capture_content and tool_content:
            body["content"] = tool_content

        yield body


def message_to_event(
    message: dict[str, Any], capture_content: bool
) -> Iterator[LogRecord]:
    attributes = {GEN_AI_SYSTEM: GenAiSystemValues.AWS_BEDROCK.value}
    role = message.get("role")
    content = message.get("content")

    body = {}
    if capture_content and content:
        body["content"] = content
    if role == "assistant":
        # the assistant message contains both tool calls and model thinking content
        if tool_calls := extract_tool_calls(message, capture_content):
            body["tool_calls"] = tool_calls
    elif role == "user":
        # in case of tool calls we send one tool event for tool call and one for the user event
        for tool_body in extract_tool_results(message, capture_content):
            yield LogRecord(
                event_name="gen_ai.tool.message",
                attributes=attributes,
                body=tool_body,
                context=get_current(),
            )

    yield LogRecord(
        event_name=f"gen_ai.{role}.message",
        attributes=attributes,
        body=body if body else None,
        context=get_current(),
    )


class _Choice:
    def __init__(
        self, message: dict[str, Any], finish_reason: str, index: int
    ):
        self.message = message
        self.finish_reason = finish_reason
        self.index = index

    @classmethod
    def from_converse(
        cls, response: dict[str, Any], capture_content: bool
    ) -> _Choice:
        # be defensive about malformed responses, refer to #3958 for more context
        output = response.get("output", {})
        orig_message = output.get("message", {})
        if role := orig_message.get("role"):
            message = {"role": role}
        else:
            # amazon.titan does not serialize the role
            message = {}

        if tool_calls := extract_tool_calls(orig_message, capture_content):
            message["tool_calls"] = tool_calls
        elif capture_content and (content := orig_message.get("content")):
            message["content"] = content

        return cls(message, response["stopReason"], index=0)

    @classmethod
    def from_invoke_amazon_titan(
        cls, response: dict[str, Any], capture_content: bool
    ) -> _Choice:
        result = response["results"][0]
        if capture_content:
            message = {"content": result["outputText"]}
        else:
            message = {}
        return cls(message, result["completionReason"], index=0)

    @classmethod
    def from_invoke_anthropic_claude(
        cls, response: dict[str, Any], capture_content: bool
    ) -> _Choice:
        message = {"role": response["role"]}
        if tool_calls := extract_tool_calls(response, capture_content):
            message["tool_calls"] = tool_calls
        elif capture_content:
            message["content"] = response["content"]
        return cls(message, response["stop_reason"], index=0)

    @classmethod
    def from_invoke_cohere_command_r(
        cls, response: dict[str, Any], capture_content: bool
    ) -> _Choice:
        if capture_content:
            message = {"content": response["text"]}
        else:
            message = {}
        return cls(message, response["finish_reason"], index=0)

    @classmethod
    def from_invoke_cohere_command(
        cls, response: dict[str, Any], capture_content: bool
    ) -> _Choice:
        result = response["generations"][0]
        if capture_content:
            message = {"content": result["text"]}
        else:
            message = {}
        return cls(message, result["finish_reason"], index=0)

    @classmethod
    def from_invoke_meta_llama(
        cls, response: dict[str, Any], capture_content: bool
    ) -> _Choice:
        if capture_content:
            message = {"content": response["generation"]}
        else:
            message = {}
        return cls(message, response["stop_reason"], index=0)

    @classmethod
    def from_invoke_mistral_mistral(
        cls, response: dict[str, Any], capture_content: bool
    ) -> _Choice:
        result = response["outputs"][0]
        if capture_content:
            message = {"content": result["text"]}
        else:
            message = {}
        return cls(message, result["stop_reason"], index=0)

    def _to_body_dict(self) -> dict[str, Any]:
        return {
            "finish_reason": self.finish_reason,
            "index": self.index,
            "message": self.message,
        }

    def to_choice_event(self, **event_kwargs) -> LogRecord:
        attributes = {GEN_AI_SYSTEM: GenAiSystemValues.AWS_BEDROCK.value}
        return LogRecord(
            event_name="gen_ai.choice",
            attributes=attributes,
            body=self._to_body_dict(),
            **event_kwargs,
        )
