"""If you use the OpenAI Python SDK, you can use the Langfuse drop-in replacement to get full logging by changing only the import.

```diff
- import openai
+ from langfuse.openai import openai
```

Langfuse automatically tracks:

- All prompts/completions with support for streaming, async and functions
- Latencies
- API Errors
- Model usage (tokens) and cost (USD)

The integration is fully interoperable with the `observe()` decorator and the low-level tracing SDK.

See docs for more details: https://langfuse.com/docs/integrations/openai
"""

import logging
import types
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from inspect import isclass
from typing import Any, Optional, cast

from openai._types import NotGiven
from packaging.version import Version
from pydantic import BaseModel
from wrapt import wrap_function_wrapper

from langfuse._client.get_client import get_client
from langfuse._client.span import LangfuseGeneration
from langfuse._utils import _get_timestamp
from langfuse.media import LangfuseMedia

try:
    import openai
except ImportError:
    raise ModuleNotFoundError(
        "Please install OpenAI to use this feature: 'pip install openai'"
    )

try:
    from openai import AsyncAzureOpenAI, AsyncOpenAI, AzureOpenAI, OpenAI  # noqa: F401
except ImportError:
    AsyncAzureOpenAI = None  # type: ignore
    AsyncOpenAI = None  # type: ignore
    AzureOpenAI = None  # type: ignore
    OpenAI = None  # type: ignore

log = logging.getLogger("langfuse")


@dataclass
class OpenAiDefinition:
    module: str
    object: str
    method: str
    type: str
    sync: bool
    min_version: Optional[str] = None
    max_version: Optional[str] = None


OPENAI_METHODS_V0 = [
    OpenAiDefinition(
        module="openai",
        object="ChatCompletion",
        method="create",
        type="chat",
        sync=True,
    ),
    OpenAiDefinition(
        module="openai",
        object="Completion",
        method="create",
        type="completion",
        sync=True,
    ),
]


OPENAI_METHODS_V1 = [
    OpenAiDefinition(
        module="openai.resources.chat.completions",
        object="Completions",
        method="create",
        type="chat",
        sync=True,
    ),
    OpenAiDefinition(
        module="openai.resources.completions",
        object="Completions",
        method="create",
        type="completion",
        sync=True,
    ),
    OpenAiDefinition(
        module="openai.resources.chat.completions",
        object="AsyncCompletions",
        method="create",
        type="chat",
        sync=False,
    ),
    OpenAiDefinition(
        module="openai.resources.completions",
        object="AsyncCompletions",
        method="create",
        type="completion",
        sync=False,
    ),
    OpenAiDefinition(
        module="openai.resources.beta.chat.completions",
        object="Completions",
        method="parse",
        type="chat",
        sync=True,
        min_version="1.50.0",
        max_version="1.92.0",
    ),
    OpenAiDefinition(
        module="openai.resources.beta.chat.completions",
        object="AsyncCompletions",
        method="parse",
        type="chat",
        sync=False,
        min_version="1.50.0",
        max_version="1.92.0",
    ),
    OpenAiDefinition(
        module="openai.resources.chat.completions",
        object="Completions",
        method="parse",
        type="chat",
        sync=True,
        min_version="1.92.0",
    ),
    OpenAiDefinition(
        module="openai.resources.chat.completions",
        object="AsyncCompletions",
        method="parse",
        type="chat",
        sync=False,
        min_version="1.92.0",
    ),
    OpenAiDefinition(
        module="openai.resources.responses",
        object="Responses",
        method="create",
        type="chat",
        sync=True,
        min_version="1.66.0",
    ),
    OpenAiDefinition(
        module="openai.resources.responses",
        object="AsyncResponses",
        method="create",
        type="chat",
        sync=False,
        min_version="1.66.0",
    ),
    OpenAiDefinition(
        module="openai.resources.responses",
        object="Responses",
        method="parse",
        type="chat",
        sync=True,
        min_version="1.66.0",
    ),
    OpenAiDefinition(
        module="openai.resources.responses",
        object="AsyncResponses",
        method="parse",
        type="chat",
        sync=False,
        min_version="1.66.0",
    ),
    OpenAiDefinition(
        module="openai.resources.embeddings",
        object="Embeddings",
        method="create",
        type="embedding",
        sync=True,
    ),
    OpenAiDefinition(
        module="openai.resources.embeddings",
        object="AsyncEmbeddings",
        method="create",
        type="embedding",
        sync=False,
    ),
]


class OpenAiArgsExtractor:
    def __init__(
        self,
        metadata: Optional[Any] = None,
        name: Optional[str] = None,
        langfuse_prompt: Optional[
            Any
        ] = None,  # we cannot use prompt because it's an argument of the old OpenAI completions API
        langfuse_public_key: Optional[str] = None,
        trace_id: Optional[str] = None,
        parent_observation_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.args = {}
        self.args["metadata"] = (
            metadata
            if "response_format" not in kwargs
            else {
                **(metadata or {}),
                "response_format": kwargs["response_format"].model_json_schema()
                if isclass(kwargs["response_format"])
                and issubclass(kwargs["response_format"], BaseModel)
                else kwargs["response_format"],
            }
        )
        self.args["name"] = name
        self.args["langfuse_public_key"] = langfuse_public_key
        self.args["langfuse_prompt"] = langfuse_prompt
        self.args["trace_id"] = trace_id
        self.args["parent_observation_id"] = parent_observation_id

        self.kwargs = kwargs

    def get_langfuse_args(self) -> Any:
        return {**self.args, **self.kwargs}

    def get_openai_args(self) -> Any:
        # If OpenAI model distillation is enabled, we need to add the metadata to the kwargs
        # https://platform.openai.com/docs/guides/distillation
        if self.kwargs.get("store", False):
            self.kwargs["metadata"] = (
                {} if self.args.get("metadata", None) is None else self.args["metadata"]
            )

            # OpenAI does not support non-string type values in metadata when using
            # model distillation feature
            self.kwargs["metadata"].pop("response_format", None)

        return self.kwargs


def _langfuse_wrapper(func: Any) -> Any:
    def _with_langfuse(open_ai_definitions: Any) -> Any:
        def wrapper(wrapped: Any, instance: Any, args: Any, kwargs: Any) -> Any:
            return func(open_ai_definitions, wrapped, args, kwargs)

        return wrapper

    return _with_langfuse


def _extract_chat_prompt(kwargs: Any) -> Any:
    """Extracts the user input from prompts. Returns an array of messages or dict with messages and functions"""
    prompt = {}

    if kwargs.get("functions") is not None:
        prompt.update({"functions": kwargs["functions"]})

    if kwargs.get("function_call") is not None:
        prompt.update({"function_call": kwargs["function_call"]})

    if kwargs.get("tools") is not None:
        prompt.update({"tools": kwargs["tools"]})

    if prompt:
        # uf user provided functions, we need to send these together with messages to langfuse
        prompt.update(
            {
                "messages": [
                    _process_message(message) for message in kwargs.get("messages", [])
                ],
            }
        )
        return prompt
    else:
        # vanilla case, only send messages in openai format to langfuse
        return [_process_message(message) for message in kwargs.get("messages", [])]


def _process_message(message: Any) -> Any:
    if not isinstance(message, dict):
        return message

    processed_message = {**message}

    content = processed_message.get("content", None)
    if not isinstance(content, list):
        return processed_message

    processed_content = []

    for content_part in content:
        if content_part.get("type") == "input_audio":
            audio_base64 = content_part.get("input_audio", {}).get("data", None)
            format = content_part.get("input_audio", {}).get("format", "wav")

            if audio_base64 is not None:
                base64_data_uri = f"data:audio/{format};base64,{audio_base64}"

                processed_content.append(
                    {
                        "type": "input_audio",
                        "input_audio": {
                            "data": LangfuseMedia(base64_data_uri=base64_data_uri),
                            "format": format,
                        },
                    }
                )
        else:
            processed_content.append(content_part)

    processed_message["content"] = processed_content

    return processed_message


def _extract_chat_response(kwargs: Any) -> Any:
    """Extracts the llm output from the response."""
    response = {
        "role": kwargs.get("role", None),
    }

    audio = None

    if kwargs.get("function_call") is not None:
        response.update({"function_call": kwargs["function_call"]})

    if kwargs.get("tool_calls") is not None:
        response.update({"tool_calls": kwargs["tool_calls"]})

    if kwargs.get("audio") is not None:
        audio = kwargs["audio"].__dict__

        if "data" in audio and audio["data"] is not None:
            base64_data_uri = f"data:audio/{audio.get('format', 'wav')};base64,{audio.get('data', None)}"
            audio["data"] = LangfuseMedia(base64_data_uri=base64_data_uri)

    response.update(
        {
            "content": kwargs.get("content", None),
        }
    )

    if audio is not None:
        response.update({"audio": audio})

    return response


def _get_langfuse_data_from_kwargs(resource: OpenAiDefinition, kwargs: Any) -> Any:
    default_name = (
        "OpenAI-embedding" if resource.type == "embedding" else "OpenAI-generation"
    )
    name = kwargs.get("name", default_name)

    if name is None:
        name = default_name

    if name is not None and not isinstance(name, str):
        raise TypeError("name must be a string")

    langfuse_public_key = kwargs.get("langfuse_public_key", None)
    if langfuse_public_key is not None and not isinstance(langfuse_public_key, str):
        raise TypeError("langfuse_public_key must be a string")

    trace_id = kwargs.get("trace_id", None)
    if trace_id is not None and not isinstance(trace_id, str):
        raise TypeError("trace_id must be a string")

    session_id = kwargs.get("session_id", None)
    if session_id is not None and not isinstance(session_id, str):
        raise TypeError("session_id must be a string")

    user_id = kwargs.get("user_id", None)
    if user_id is not None and not isinstance(user_id, str):
        raise TypeError("user_id must be a string")

    tags = kwargs.get("tags", None)
    if tags is not None and (
        not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags)
    ):
        raise TypeError("tags must be a list of strings")

    parent_observation_id = kwargs.get("parent_observation_id", None)
    if parent_observation_id is not None and not isinstance(parent_observation_id, str):
        raise TypeError("parent_observation_id must be a string")

    if parent_observation_id is not None and trace_id is None:
        raise ValueError("parent_observation_id requires trace_id to be set")

    metadata = kwargs.get("metadata", {})
    if (
        metadata is not None
        and not isinstance(metadata, NotGiven)
        and not isinstance(metadata, dict)
    ):
        if isinstance(metadata, BaseModel):
            metadata = metadata.model_dump()
        else:
            metadata = {}

    model = kwargs.get("model", None) or None

    prompt = None

    if resource.type == "completion":
        prompt = kwargs.get("prompt", None)
    elif resource.object == "Responses" or resource.object == "AsyncResponses":
        prompt = kwargs.get("input", None)
    elif resource.type == "chat":
        prompt = _extract_chat_prompt(kwargs)
    elif resource.type == "embedding":
        prompt = kwargs.get("input", None)

    parsed_temperature = (
        kwargs.get("temperature", 1)
        if not isinstance(kwargs.get("temperature", 1), NotGiven)
        else 1
    )

    parsed_max_tokens = (
        kwargs.get("max_tokens", float("inf"))
        if not isinstance(kwargs.get("max_tokens", float("inf")), NotGiven)
        else float("inf")
    )

    parsed_max_completion_tokens = (
        kwargs.get("max_completion_tokens", None)
        if not isinstance(kwargs.get("max_completion_tokens", float("inf")), NotGiven)
        else None
    )

    parsed_top_p = (
        kwargs.get("top_p", 1)
        if not isinstance(kwargs.get("top_p", 1), NotGiven)
        else 1
    )

    parsed_frequency_penalty = (
        kwargs.get("frequency_penalty", 0)
        if not isinstance(kwargs.get("frequency_penalty", 0), NotGiven)
        else 0
    )

    parsed_presence_penalty = (
        kwargs.get("presence_penalty", 0)
        if not isinstance(kwargs.get("presence_penalty", 0), NotGiven)
        else 0
    )

    parsed_seed = (
        kwargs.get("seed", None)
        if not isinstance(kwargs.get("seed", None), NotGiven)
        else None
    )

    parsed_n = kwargs.get("n", 1) if not isinstance(kwargs.get("n", 1), NotGiven) else 1

    if resource.type == "embedding":
        parsed_dimensions = (
            kwargs.get("dimensions", None)
            if not isinstance(kwargs.get("dimensions", None), NotGiven)
            else None
        )
        parsed_encoding_format = (
            kwargs.get("encoding_format", "float")
            if not isinstance(kwargs.get("encoding_format", "float"), NotGiven)
            else "float"
        )

        modelParameters = {}
        if parsed_dimensions is not None:
            modelParameters["dimensions"] = parsed_dimensions
        if parsed_encoding_format != "float":
            modelParameters["encoding_format"] = parsed_encoding_format
    else:
        modelParameters = {
            "temperature": parsed_temperature,
            "max_tokens": parsed_max_tokens,
            "top_p": parsed_top_p,
            "frequency_penalty": parsed_frequency_penalty,
            "presence_penalty": parsed_presence_penalty,
        }

        if parsed_max_completion_tokens is not None:
            modelParameters.pop("max_tokens", None)
            modelParameters["max_completion_tokens"] = parsed_max_completion_tokens

        if parsed_n is not None and isinstance(parsed_n, int) and parsed_n > 1:
            modelParameters["n"] = parsed_n

        if parsed_seed is not None:
            modelParameters["seed"] = parsed_seed

    langfuse_prompt = kwargs.get("langfuse_prompt", None)

    return {
        "name": name,
        "metadata": metadata,
        "langfuse_public_key": langfuse_public_key,
        "trace_id": trace_id,
        "parent_observation_id": parent_observation_id,
        "user_id": user_id,
        "input": prompt,
        "model_parameters": modelParameters,
        "model": model or None,
        "prompt": langfuse_prompt,
    }


def _create_langfuse_update(
    completion: Any,
    generation: LangfuseGeneration,
    completion_start_time: Any,
    model: Optional[str] = None,
    usage: Optional[Any] = None,
    metadata: Optional[Any] = None,
) -> Any:
    update = {
        "output": completion,
        "completion_start_time": completion_start_time,
    }
    if model is not None:
        update["model"] = model

    if metadata is not None:
        update["metadata"] = metadata

    if usage is not None:
        update["usage_details"] = _parse_usage(usage)
        update["cost_details"] = _parse_cost(usage)

    generation.update(**update)


def _parse_usage(usage: Optional[Any] = None) -> Any:
    if usage is None:
        return

    usage_dict = usage.copy() if isinstance(usage, dict) else usage.__dict__.copy()

    for tokens_details in [
        "prompt_tokens_details",
        "completion_tokens_details",
        "input_token_details",
        "output_token_details",
    ]:
        if tokens_details in usage_dict and usage_dict[tokens_details] is not None:
            tokens_details_dict = (
                usage_dict[tokens_details]
                if isinstance(usage_dict[tokens_details], dict)
                else usage_dict[tokens_details].__dict__
            )
            usage_dict[tokens_details] = {
                k: v for k, v in tokens_details_dict.items() if v is not None
            }

    if (
        len(usage_dict) == 2
        and "prompt_tokens" in usage_dict
        and "total_tokens" in usage_dict
    ):
        # handle embedding usage
        return {"input": usage_dict["prompt_tokens"]}

    return usage_dict


def _parse_cost(usage: Optional[Any] = None) -> Any:
    if usage is None:
        return

    # OpenRouter is returning total cost of the invocation
    # https://openrouter.ai/docs/use-cases/usage-accounting#cost-breakdown
    if hasattr(usage, "cost") and isinstance(getattr(usage, "cost"), float):
        return {"total": getattr(usage, "cost")}

    return None


def _extract_streamed_response_api_response(chunks: Any) -> Any:
    completion, model, usage = None, None, None
    metadata = {}

    for raw_chunk in chunks:
        chunk = raw_chunk.__dict__
        if raw_response := chunk.get("response", None):
            usage = chunk.get("usage", None) or getattr(raw_response, "usage", None)

            response = raw_response.__dict__
            model = response.get("model")

            for key, val in response.items():
                if key not in ["created_at", "model", "output", "usage", "text"]:
                    metadata[key] = val

                if key == "output":
                    output = val
                    if not isinstance(output, list):
                        completion = output
                    elif len(output) > 1:
                        completion = output
                    elif len(output) == 1:
                        completion = output[0]

    return (model, completion, usage, metadata)


def _extract_streamed_openai_response(resource: Any, chunks: Any) -> Any:
    completion: Any = defaultdict(lambda: None) if resource.type == "chat" else ""
    model, usage, finish_reason = None, None, None

    for chunk in chunks:
        if _is_openai_v1():
            chunk = chunk.__dict__

        model = model or chunk.get("model", None) or None
        usage = chunk.get("usage", None)

        choices = chunk.get("choices", [])

        for choice in choices:
            if _is_openai_v1():
                choice = choice.__dict__
            if resource.type == "chat":
                delta = choice.get("delta", None)
                finish_reason = choice.get("finish_reason", None)

                if _is_openai_v1():
                    delta = delta.__dict__

                if delta.get("role", None) is not None:
                    completion["role"] = delta["role"]

                if delta.get("content", None) is not None:
                    completion["content"] = (
                        delta.get("content", None)
                        if completion["content"] is None
                        else completion["content"] + delta.get("content", None)
                    )
                elif delta.get("function_call", None) is not None:
                    curr = completion["function_call"]
                    tool_call_chunk = delta.get("function_call", None)

                    if not curr:
                        completion["function_call"] = {
                            "name": getattr(tool_call_chunk, "name", ""),
                            "arguments": getattr(tool_call_chunk, "arguments", ""),
                        }

                    else:
                        curr["name"] = curr["name"] or getattr(
                            tool_call_chunk, "name", None
                        )
                        curr["arguments"] += getattr(tool_call_chunk, "arguments", "")

                elif (
                    delta.get("tool_calls", None) is not None
                    and len(delta.get("tool_calls")) > 0
                ):
                    curr = completion["tool_calls"]
                    tool_call_chunk = getattr(
                        delta.get("tool_calls", None)[0], "function", None
                    )

                    if not curr:
                        completion["tool_calls"] = [
                            {
                                "name": getattr(tool_call_chunk, "name", ""),
                                "arguments": getattr(tool_call_chunk, "arguments", ""),
                            }
                        ]

                    elif getattr(tool_call_chunk, "name", None) is not None:
                        curr.append(
                            {
                                "name": getattr(tool_call_chunk, "name", None),
                                "arguments": getattr(
                                    tool_call_chunk, "arguments", None
                                ),
                            }
                        )

                    else:
                        curr[-1]["name"] = curr[-1]["name"] or getattr(
                            tool_call_chunk, "name", None
                        )

                        if curr[-1]["arguments"] is None:
                            curr[-1]["arguments"] = ""

                        curr[-1]["arguments"] += getattr(
                            tool_call_chunk, "arguments", ""
                        )

            if resource.type == "completion":
                completion += choice.get("text", "")

    def get_response_for_chat() -> Any:
        return (
            completion["content"]
            or (
                completion["function_call"]
                and {
                    "role": "assistant",
                    "function_call": completion["function_call"],
                }
            )
            or (
                completion["tool_calls"]
                and {
                    "role": "assistant",
                    # "tool_calls": [{"function": completion["tool_calls"]}],
                    "tool_calls": [
                        {"function": data} for data in completion["tool_calls"]
                    ],
                }
            )
            or None
        )

    return (
        model,
        get_response_for_chat() if resource.type == "chat" else completion,
        usage,
        {"finish_reason": finish_reason} if finish_reason is not None else None,
    )


def _get_langfuse_data_from_default_response(
    resource: OpenAiDefinition, response: Any
) -> Any:
    if response is None:
        return None, "<NoneType response returned from OpenAI>", None

    model = response.get("model", None) or None

    completion = None

    if resource.type == "completion":
        choices = response.get("choices", [])
        if len(choices) > 0:
            choice = choices[-1]

            completion = choice.text if _is_openai_v1() else choice.get("text", None)

    elif resource.object == "Responses" or resource.object == "AsyncResponses":
        output = response.get("output", {})

        if not isinstance(output, list):
            completion = output
        elif len(output) > 1:
            completion = output
        elif len(output) == 1:
            completion = output[0]

    elif resource.type == "chat":
        choices = response.get("choices", [])
        if len(choices) > 0:
            # If multiple choices were generated, we'll show all of them in the UI as a list.
            if len(choices) > 1:
                completion = [
                    _extract_chat_response(choice.message.__dict__)
                    if _is_openai_v1()
                    else choice.get("message", None)
                    for choice in choices
                ]
            else:
                choice = choices[0]
                completion = (
                    _extract_chat_response(choice.message.__dict__)
                    if _is_openai_v1()
                    else choice.get("message", None)
                )

    elif resource.type == "embedding":
        data = response.get("data", [])
        if len(data) > 0:
            first_embedding = data[0]
            embedding_vector = (
                first_embedding.embedding
                if hasattr(first_embedding, "embedding")
                else first_embedding.get("embedding", [])
            )
            completion = {
                "dimensions": len(embedding_vector) if embedding_vector else 0,
                "count": len(data),
            }

    usage = _parse_usage(response.get("usage", None))

    return (model, completion, usage)


def _is_openai_v1() -> bool:
    return Version(openai.__version__) >= Version("1.0.0")


def _is_streaming_response(response: Any) -> bool:
    return (
        isinstance(response, types.GeneratorType)
        or isinstance(response, types.AsyncGeneratorType)
        or (_is_openai_v1() and isinstance(response, openai.Stream))
        or (_is_openai_v1() and isinstance(response, openai.AsyncStream))
    )


@_langfuse_wrapper
def _wrap(
    open_ai_resource: OpenAiDefinition, wrapped: Any, args: Any, kwargs: Any
) -> Any:
    arg_extractor = OpenAiArgsExtractor(*args, **kwargs)
    langfuse_args = arg_extractor.get_langfuse_args()

    langfuse_data = _get_langfuse_data_from_kwargs(open_ai_resource, langfuse_args)
    langfuse_client = get_client(public_key=langfuse_args["langfuse_public_key"])

    observation_type = (
        "embedding" if open_ai_resource.type == "embedding" else "generation"
    )

    generation = langfuse_client.start_observation(
        as_type=observation_type,  # type: ignore
        name=langfuse_data["name"],
        input=langfuse_data.get("input", None),
        metadata=langfuse_data.get("metadata", None),
        model_parameters=langfuse_data.get("model_parameters", None),
        trace_context={
            "trace_id": cast(str, langfuse_data.get("trace_id", None)),
            "parent_span_id": cast(
                str, langfuse_data.get("parent_observation_id", None)
            ),
        },
        model=langfuse_data.get("model", None),
        prompt=langfuse_data.get("prompt", None),
    )

    try:
        openai_response = wrapped(**arg_extractor.get_openai_args())

        if _is_streaming_response(openai_response):
            return LangfuseResponseGeneratorSync(
                resource=open_ai_resource,
                response=openai_response,
                generation=generation,
            )

        else:
            model, completion, usage = _get_langfuse_data_from_default_response(
                open_ai_resource,
                (openai_response and openai_response.__dict__)
                if _is_openai_v1()
                else openai_response,
            )

            generation.update(
                model=model,
                output=completion,
                usage_details=usage,
                cost_details=_parse_cost(openai_response.usage)
                if hasattr(openai_response, "usage")
                else None,
            ).end()

        return openai_response
    except Exception as ex:
        log.warning(ex)
        model = kwargs.get("model", None) or None
        generation.update(
            status_message=str(ex),
            level="ERROR",
            model=model,
            cost_details={"input": 0, "output": 0, "total": 0},
        ).end()

        raise ex


@_langfuse_wrapper
async def _wrap_async(
    open_ai_resource: OpenAiDefinition, wrapped: Any, args: Any, kwargs: Any
) -> Any:
    arg_extractor = OpenAiArgsExtractor(*args, **kwargs)
    langfuse_args = arg_extractor.get_langfuse_args()

    langfuse_data = _get_langfuse_data_from_kwargs(open_ai_resource, langfuse_args)
    langfuse_client = get_client(public_key=langfuse_args["langfuse_public_key"])

    observation_type = (
        "embedding" if open_ai_resource.type == "embedding" else "generation"
    )

    generation = langfuse_client.start_observation(
        as_type=observation_type,  # type: ignore
        name=langfuse_data["name"],
        input=langfuse_data.get("input", None),
        metadata=langfuse_data.get("metadata", None),
        trace_context={
            "trace_id": cast(str, langfuse_data.get("trace_id", None)),
            "parent_span_id": cast(
                str, langfuse_data.get("parent_observation_id", None)
            ),
        },
        model_parameters=langfuse_data.get("model_parameters", None),
        model=langfuse_data.get("model", None),
        prompt=langfuse_data.get("prompt", None),
    )

    try:
        openai_response = await wrapped(**arg_extractor.get_openai_args())

        if _is_streaming_response(openai_response):
            return LangfuseResponseGeneratorAsync(
                resource=open_ai_resource,
                response=openai_response,
                generation=generation,
            )

        else:
            model, completion, usage = _get_langfuse_data_from_default_response(
                open_ai_resource,
                (openai_response and openai_response.__dict__)
                if _is_openai_v1()
                else openai_response,
            )
            generation.update(
                model=model,
                output=completion,
                usage=usage,  # backward compat for all V2 self hosters
                usage_details=usage,
                cost_details=_parse_cost(openai_response.usage)
                if hasattr(openai_response, "usage")
                else None,
            ).end()

        return openai_response
    except Exception as ex:
        log.warning(ex)
        model = kwargs.get("model", None) or None
        generation.update(
            status_message=str(ex),
            level="ERROR",
            model=model,
            cost_details={"input": 0, "output": 0, "total": 0},
        ).end()

        raise ex


def register_tracing() -> None:
    resources = OPENAI_METHODS_V1 if _is_openai_v1() else OPENAI_METHODS_V0

    for resource in resources:
        if resource.min_version is not None and Version(openai.__version__) < Version(
            resource.min_version
        ):
            continue

        if resource.max_version is not None and Version(openai.__version__) >= Version(
            resource.max_version
        ):
            continue

        wrap_function_wrapper(
            resource.module,
            f"{resource.object}.{resource.method}",
            _wrap(resource) if resource.sync else _wrap_async(resource),
        )


register_tracing()


class LangfuseResponseGeneratorSync:
    def __init__(
        self,
        *,
        resource: Any,
        response: Any,
        generation: Any,
    ) -> None:
        self.items: list[Any] = []

        self.resource = resource
        self.response = response
        self.generation = generation
        self.completion_start_time: Optional[datetime] = None

    def __iter__(self) -> Any:
        try:
            for i in self.response:
                self.items.append(i)

                if self.completion_start_time is None:
                    self.completion_start_time = _get_timestamp()

                yield i
        finally:
            self._finalize()

    def __next__(self) -> Any:
        try:
            item = self.response.__next__()
            self.items.append(item)

            if self.completion_start_time is None:
                self.completion_start_time = _get_timestamp()

            return item

        except StopIteration:
            self._finalize()

            raise

    def __enter__(self) -> Any:
        return self.__iter__()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        pass

    def _finalize(self) -> None:
        try:
            model, completion, usage, metadata = (
                _extract_streamed_response_api_response(self.items)
                if self.resource.object == "Responses"
                or self.resource.object == "AsyncResponses"
                else _extract_streamed_openai_response(self.resource, self.items)
            )

            _create_langfuse_update(
                completion,
                self.generation,
                self.completion_start_time,
                model=model,
                usage=usage,
                metadata=metadata,
            )
        except Exception:
            pass
        finally:
            self.generation.end()


class LangfuseResponseGeneratorAsync:
    def __init__(
        self,
        *,
        resource: Any,
        response: Any,
        generation: Any,
    ) -> None:
        self.items: list[Any] = []

        self.resource = resource
        self.response = response
        self.generation = generation
        self.completion_start_time: Optional[datetime] = None

    async def __aiter__(self) -> Any:
        try:
            async for i in self.response:
                self.items.append(i)

                if self.completion_start_time is None:
                    self.completion_start_time = _get_timestamp()

                yield i
        finally:
            await self._finalize()

    async def __anext__(self) -> Any:
        try:
            item = await self.response.__anext__()
            self.items.append(item)

            if self.completion_start_time is None:
                self.completion_start_time = _get_timestamp()

            return item

        except StopAsyncIteration:
            await self._finalize()

            raise

    async def __aenter__(self) -> Any:
        return self.__aiter__()

    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        pass

    async def _finalize(self) -> None:
        try:
            model, completion, usage, metadata = (
                _extract_streamed_response_api_response(self.items)
                if self.resource.object == "Responses"
                or self.resource.object == "AsyncResponses"
                else _extract_streamed_openai_response(self.resource, self.items)
            )

            _create_langfuse_update(
                completion,
                self.generation,
                self.completion_start_time,
                model=model,
                usage=usage,
                metadata=metadata,
            )
        except Exception:
            pass
        finally:
            self.generation.end()

    async def close(self) -> None:
        """Close the response and release the connection.

        Automatically called if the response body is read to completion.
        """
        await self.response.close()

    async def aclose(self) -> None:
        """Close the response and release the connection.

        Automatically called if the response body is read to completion.
        """
        await self.response.aclose()
