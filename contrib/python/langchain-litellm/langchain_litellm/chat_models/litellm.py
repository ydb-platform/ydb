"""Wrapper around LiteLLM's model I/O library."""

from __future__ import annotations

import json
import logging
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypedDict,
    Union,
    cast,
)
from typing_extensions import is_typeddict
from operator import itemgetter

from langchain_core.callbacks import (
    AsyncCallbackManagerForLLMRun,
    CallbackManagerForLLMRun,
)
from langchain_core.language_models import LanguageModelInput
from langchain_core.language_models.chat_models import (
    BaseChatModel,
    agenerate_from_stream,
    generate_from_stream,
)
from langchain_core.language_models.llms import create_base_retry_decorator
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
    BaseMessageChunk,
    ChatMessage,
    ChatMessageChunk,
    FunctionMessage,
    FunctionMessageChunk,
    HumanMessage,
    HumanMessageChunk,
    SystemMessage,
    SystemMessageChunk,
    ToolCall,
    ToolCallChunk,
    ToolMessage,
)
from langchain_core.messages.ai import UsageMetadata
from langchain_core.outputs import (
    ChatGeneration,
    ChatGenerationChunk,
    ChatResult,
)
from langchain_core.output_parsers import (
    JsonOutputParser,
    PydanticOutputParser,
    PydanticToolsParser,
    JsonOutputKeyToolsParser
)
from langchain_core.runnables import Runnable, RunnablePassthrough
from langchain_core.tools import BaseTool
from langchain_core.utils.pydantic import TypeBaseModel, is_basemodel_subclass
from langchain_core.utils import get_from_dict_or_env, pre_init
from langchain_core.utils.function_calling import convert_to_openai_tool
from litellm.types.utils import Delta
from litellm.utils import get_valid_models
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ChatLiteLLMException(Exception):
    """Error with the `LiteLLM I/O` library"""


def _create_retry_decorator(
    llm: ChatLiteLLM,
    run_manager: Optional[
        Union[AsyncCallbackManagerForLLMRun, CallbackManagerForLLMRun]
    ] = None,
) -> Callable[[Any], Any]:
    """Returns a tenacity retry decorator, preconfigured to handle PaLM exceptions"""
    import litellm

    errors = [
        litellm.Timeout,
        litellm.APIError,
        litellm.APIConnectionError,
        litellm.RateLimitError,
    ]
    return create_base_retry_decorator(
        error_types=errors, max_retries=llm.max_retries, run_manager=run_manager
    )


def _convert_dict_to_message(_dict: Mapping[str, Any]) -> BaseMessage:
    role = _dict["role"]
    if role == "user":
        return HumanMessage(content=_dict["content"])
    elif role == "assistant":
        # Fix for azure
        # Also OpenAI returns None for tool invocations
        content = _dict.get("content", "") or ""

        additional_kwargs = {}
        if _dict.get("function_call"):
            additional_kwargs["function_call"] = dict(_dict["function_call"])

        if _dict.get("tool_calls"):
            additional_kwargs["tool_calls"] = _dict["tool_calls"]

        if _dict.get("reasoning_content"):
            additional_kwargs["reasoning_content"] = _dict["reasoning_content"]
        
        # Check litellm's response for provider specific fields
        provider_specific_fields = _dict.get("provider_specific_fields")
        
        # Check if litellm's response has null provider_specific_fields but has Vertex AI specific fields
        if not provider_specific_fields:
            provider_specific_fields = _dict.get("vertex_ai_grounding_metadata")
        
        # Attach provider specific fields if present
        if provider_specific_fields:
            additional_kwargs["provider_specific_fields"] = provider_specific_fields

        return AIMessage(content=content, additional_kwargs=additional_kwargs)
    elif role == "system":
        return SystemMessage(content=_dict["content"])
    elif role == "function":
        return FunctionMessage(content=_dict["content"], name=_dict["name"])
    elif role == "tool":
        return ToolMessage(content=_dict["content"], tool_call_id=_dict["tool_call_id"])
    else:
        return ChatMessage(content=_dict["content"], role=role)


def _convert_delta_to_message_chunk(
    delta: Union[Delta, Dict[str, Any]], default_class: Type[BaseMessageChunk]
) -> BaseMessageChunk:
    # Handle both Delta objects and dicts
    if isinstance(delta, dict):
        role = delta.get("role")
        content = delta.get("content") or ""
        function_call = delta.get("function_call")
        raw_tool_calls = delta.get("tool_calls")
        reasoning_content = delta.get("reasoning_content")
        # Check standard field first, then fallback to Vertex specific field
        provider_specific_fields = delta.get("provider_specific_fields")
        if not provider_specific_fields:
            provider_specific_fields = delta.get("vertex_ai_grounding_metadata")
    else:
        role = delta.role
        content = delta.content or ""
        function_call = delta.function_call
        raw_tool_calls = delta.tool_calls
        reasoning_content = getattr(delta, "reasoning_content", None)
        # Check standard field first, then fallback to Vertex specific field
        provider_specific_fields = getattr(delta, "provider_specific_fields", None)
        if not provider_specific_fields:
            provider_specific_fields = getattr(delta, "vertex_ai_grounding_metadata", None)

    if function_call:
        additional_kwargs = {"function_call": dict(function_call)}
    # The hasattr check is necessary because litellm explicitly deletes the
    # `reasoning_content` attribute when it is absent to comply with the OpenAI API.
    # This ensures that the code gracefully handles cases where the attribute is
    # missing, avoiding potential errors or non-compliance with the API.
    elif reasoning_content:
        additional_kwargs = {"reasoning_content": reasoning_content}
    else:
        additional_kwargs = {}
    
    if provider_specific_fields is not None:
        additional_kwargs["provider_specific_fields"] = provider_specific_fields

    tool_call_chunks = []
    if raw_tool_calls:
        additional_kwargs["tool_calls"] = raw_tool_calls
        try:
            tool_call_chunks = [
                ToolCallChunk(
                    name=rtc["function"]["name"]
                    if isinstance(rtc, dict)
                    else rtc.function.name,
                    args=rtc["function"]["arguments"]
                    if isinstance(rtc, dict)
                    else rtc.function.arguments,
                    id=rtc["id"] if isinstance(rtc, dict) else rtc.id,
                    index=rtc["index"] if isinstance(rtc, dict) else rtc.index,
                )
                for rtc in raw_tool_calls
            ]
        except KeyError:
            pass

    if role == "user" or default_class == HumanMessageChunk:
        return HumanMessageChunk(content=content)
    elif role == "assistant" or default_class == AIMessageChunk:
        return AIMessageChunk(
            content=content,
            additional_kwargs=additional_kwargs,
            tool_call_chunks=tool_call_chunks,
        )
    elif role == "system" or default_class == SystemMessageChunk:
        return SystemMessageChunk(content=content)
    elif role == "function" or default_class == FunctionMessageChunk:
        if isinstance(delta, dict):
            func_args = function_call.get("arguments", "") if function_call else ""
            func_name = function_call.get("name", "") if function_call else ""
        else:
            func_args = delta.function_call.arguments if function_call else ""
            func_name = delta.function_call.name if function_call else ""
        return FunctionMessageChunk(content=func_args, name=func_name)
    elif role or default_class == ChatMessageChunk:
        return ChatMessageChunk(content=content, role=role)  # type: ignore[arg-type]
    else:
        return default_class(content=content)  # type: ignore[call-arg]


def _lc_tool_call_to_openai_tool_call(tool_call: ToolCall) -> dict:
    return {
        "type": "function",
        "id": tool_call["id"],
        "function": {
            "name": tool_call["name"],
            "arguments": json.dumps(tool_call["args"]),
        },
    }


def _convert_message_to_dict(message: BaseMessage) -> dict:
    message_dict: Dict[str, Any] = {"content": message.content}
    if isinstance(message, ChatMessage):
        message_dict["role"] = message.role
    elif isinstance(message, HumanMessage):
        message_dict["role"] = "user"
    elif isinstance(message, AIMessage):
        message_dict["role"] = "assistant"
        if "function_call" in message.additional_kwargs:
            message_dict["function_call"] = message.additional_kwargs["function_call"]
        if message.tool_calls:
            message_dict["tool_calls"] = [
                _lc_tool_call_to_openai_tool_call(tc) for tc in message.tool_calls
            ]
        elif "tool_calls" in message.additional_kwargs:
            message_dict["tool_calls"] = message.additional_kwargs["tool_calls"]
    elif isinstance(message, SystemMessage):
        message_dict["role"] = "system"
    elif isinstance(message, FunctionMessage):
        message_dict["role"] = "function"
        message_dict["name"] = message.name
    elif isinstance(message, ToolMessage):
        message_dict["role"] = "tool"
        message_dict["tool_call_id"] = message.tool_call_id
    else:
        raise ValueError(f"Got unknown type {message}")
    if "name" in message.additional_kwargs:
        message_dict["name"] = message.additional_kwargs["name"]
    return message_dict


_OPENAI_MODELS = get_valid_models(custom_llm_provider="openai")


class ChatLiteLLM(BaseChatModel):
    """Chat model that uses the LiteLLM API."""

    client: Any = None  #: :meta private:
    model: str = "gpt-3.5-turbo"
    model_name: Optional[str] = None
    stream_options: Optional[Dict[str, Any]] = Field(
        default_factory=lambda: {"include_usage": True}
    )
    """Model name to use."""
    openai_api_key: Optional[str] = None
    azure_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    replicate_api_key: Optional[str] = None
    cohere_api_key: Optional[str] = None
    openrouter_api_key: Optional[str] = None
    api_key: Optional[str] = None
    streaming: bool = False
    api_base: Optional[str] = None
    organization: Optional[str] = None
    custom_llm_provider: Optional[str] = None
    extra_headers: Optional[Dict[str, str]] = None
    request_timeout: Optional[Union[float, Tuple[float, float]]] = None
    temperature: Optional[float] = None
    """Run inference with this temperature. Must be in the closed
       interval [0.0, 2.0]."""
    model_kwargs: Dict[str, Any] = Field(default_factory=dict)
    """Holds any model parameters valid for API call not explicitly specified."""
    top_p: Optional[float] = None
    """Decode using nucleus sampling: consider the smallest set of tokens whose
       probability sum is at least top_p. Must be in the closed interval [0.0, 1.0]."""
    top_k: Optional[int] = None
    """Decode using top-k sampling: consider the set of top_k most probable tokens.
       Must be positive."""
    n: Optional[int] = None
    """Number of chat completions to generate for each prompt. Note that the API may
       not return the full n completions if duplicates are generated."""
    max_tokens: Optional[int] = None

    max_retries: int = 1

    @property
    def _default_params(self) -> Dict[str, Any]:
        """Get the default parameters for calling OpenAI API."""
        set_model_value = self.model
        if self.model_name is not None:
            set_model_value = self.model_name
        return {
            "model": set_model_value,
            "force_timeout": self.request_timeout,
            "max_tokens": self.max_tokens,
            "stream": self.streaming,
            "n": self.n,
            "temperature": self.temperature,
            "custom_llm_provider": self.custom_llm_provider,
            **self.model_kwargs,
        }

    @property
    def _client_params(self) -> Dict[str, Any]:
        """Get the parameters used for the openai client."""
        set_model_value = self.model
        if self.model_name is not None:
            set_model_value = self.model_name
        self.client.api_base = self.api_base
        self.client.api_key = self.api_key
        for named_api_key in [
            "openai_api_key",
            "azure_api_key",
            "anthropic_api_key",
            "replicate_api_key",
            "cohere_api_key",
            "openrouter_api_key",
        ]:
            if api_key_value := getattr(self, named_api_key):
                setattr(
                    self.client,
                    named_api_key.replace("_api_key", "_key"),
                    api_key_value,
                )
        self.client.organization = self.organization
        creds: Dict[str, Any] = {
            "model": set_model_value,
            "force_timeout": self.request_timeout,
            "api_base": self.api_base,
        }
        # Forward any extra headers to the client and include in params
        if self.extra_headers is not None:
            # set attribute on client for runtime usage
            setattr(self.client, "extra_headers", self.extra_headers)
            creds["extra_headers"] = self.extra_headers
        return {**self._default_params, **creds}

    def completion_with_retry(
        self, run_manager: Optional[CallbackManagerForLLMRun] = None, **kwargs: Any
    ) -> Any:
        """Use tenacity to retry the completion call."""
        retry_decorator = _create_retry_decorator(self, run_manager=run_manager)

        @retry_decorator
        def _completion_with_retry(**kwargs: Any) -> Any:
            return self.client.completion(**kwargs)

        return _completion_with_retry(**kwargs)


    async def acompletion_with_retry(
        self,
        run_manager: Optional[AsyncCallbackManagerForLLMRun] = None,
        **kwargs: Any
    ) -> Any:
        """Use tenacity to retry the async completion call."""
        retry_decorator = _create_retry_decorator(self, run_manager=run_manager)

        @retry_decorator
        async def _completion_with_retry(**kwargs: Any) -> Any:
            return await self.client.acompletion(**kwargs)

        return await _completion_with_retry(**kwargs)

    @pre_init
    def validate_environment(cls, values: Dict) -> Dict:
        """Validate api key, python package exists, temperature, top_p, and top_k."""
        try:
            import litellm
        except ImportError:
            raise ChatLiteLLMException(
                "Could not import litellm python package. "
                "Please install it with `pip install litellm`"
            )

        values["openai_api_key"] = get_from_dict_or_env(
            values, "openai_api_key", "OPENAI_API_KEY", default=""
        )
        values["azure_api_key"] = get_from_dict_or_env(
            values, "azure_api_key", "AZURE_API_KEY", default=""
        )
        values["anthropic_api_key"] = get_from_dict_or_env(
            values, "anthropic_api_key", "ANTHROPIC_API_KEY", default=""
        )
        values["replicate_api_key"] = get_from_dict_or_env(
            values, "replicate_api_key", "REPLICATE_API_KEY", default=""
        )
        values["openrouter_api_key"] = get_from_dict_or_env(
            values, "openrouter_api_key", "OPENROUTER_API_KEY", default=""
        )
        values["cohere_api_key"] = get_from_dict_or_env(
            values, "cohere_api_key", "COHERE_API_KEY", default=""
        )
        values["huggingface_api_key"] = get_from_dict_or_env(
            values, "huggingface_api_key", "HUGGINGFACE_API_KEY", default=""
        )
        values["together_ai_api_key"] = get_from_dict_or_env(
            values, "together_ai_api_key", "TOGETHERAI_API_KEY", default=""
        )
        values["client"] = litellm

        if values["temperature"] is not None and not 0 <= values["temperature"] <= 2:
            raise ValueError("temperature must be in the range [0.0, 2.0]")

        if values["top_p"] is not None and not 0 <= values["top_p"] <= 1:
            raise ValueError("top_p must be in the range [0.0, 1.0]")

        if values["top_k"] is not None and values["top_k"] <= 0:
            raise ValueError("top_k must be positive")

        return values

    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        stream: Optional[bool] = None,
        **kwargs: Any,
    ) -> ChatResult:
        should_stream = stream if stream is not None else self.streaming
        if should_stream:
            stream_iter = self._stream(
                messages, stop=stop, run_manager=run_manager, **kwargs
            )
            return generate_from_stream(stream_iter)

        message_dicts, params = self._create_message_dicts(messages, stop)
        params = {**params, **kwargs}
        response = self.completion_with_retry(
            messages=message_dicts, run_manager=run_manager, **params
        )
        return self._create_chat_result(response)

    def _create_chat_result(self, response: Mapping[str, Any]) -> ChatResult:
        generations = []
        token_usage = response.get("usage", {})
        for res in response["choices"]:
            message = _convert_dict_to_message(res["message"])
            if isinstance(message, AIMessage):
                message.response_metadata = {
                    "model_name": self.model_name or self.model
                }
                message.usage_metadata = _create_usage_metadata(token_usage)
            gen = ChatGeneration(
                message=message,
                generation_info=dict(finish_reason=res.get("finish_reason"), logprobs=res.get("logprobs")),
            )
            generations.append(gen)
        set_model_value = self.model
        if self.model_name is not None:
            set_model_value = self.model_name
        llm_output = {"token_usage": token_usage, "model": set_model_value}
        # Check standard field first, then fallback to Vertex specific field
        provider_specific_fields = response.get("provider_specific_fields")
        if not provider_specific_fields:
            provider_specific_fields = response.get("vertex_ai_grounding_metadata")

        # Add provider_specific_fields if present at response level
        if provider_specific_fields:
            llm_output["provider_specific_fields"] = provider_specific_fields
        return ChatResult(generations=generations, llm_output=llm_output)

    def _create_message_dicts(
        self, messages: List[BaseMessage], stop: Optional[List[str]]
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        params = self._client_params
        if stop is not None:
            if "stop" in params:
                raise ValueError("`stop` found in both the input and default params.")
            params["stop"] = stop
        message_dicts = [_convert_message_to_dict(m) for m in messages]
        return message_dicts, params

    def _stream(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> Iterator[ChatGenerationChunk]:
        message_dicts, params = self._create_message_dicts(messages, stop)
        params = {**params, **kwargs, "stream": True}
        params["stream_options"] = self.stream_options
        default_chunk_class = AIMessageChunk
        for chunk in self.completion_with_retry(
            messages=message_dicts, run_manager=run_manager, **params
        ):
            usage_metadata = None
            if not isinstance(chunk, dict):
                chunk = chunk.model_dump()
            if "usage" in chunk and chunk["usage"]:
                usage_metadata = _create_usage_metadata(chunk["usage"])
            if len(chunk["choices"]) == 0:
                continue
            delta = chunk["choices"][0]["delta"]
            # --- BRIDGE FIX: Inject Root Metadata into Delta ---
            # 1. Grab metadata from the ROOT of the chunk
            root_metadata = chunk.get("provider_specific_fields")
            if not root_metadata:
                root_metadata = chunk.get("vertex_ai_grounding_metadata")
            
            # 2. Inject it into the DELTA so the converter sees it
            if root_metadata:
                delta["provider_specific_fields"] = root_metadata
            # ---------------------------------------------------
            chunk = _convert_delta_to_message_chunk(delta, default_chunk_class)
            if usage_metadata and isinstance(chunk, AIMessageChunk):
                chunk.usage_metadata = usage_metadata

            default_chunk_class = chunk.__class__
            cg_chunk = ChatGenerationChunk(message=chunk)
            if run_manager:
                run_manager.on_llm_new_token(chunk.content, chunk=cg_chunk)
            yield cg_chunk

    async def _astream(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[AsyncCallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        message_dicts, params = self._create_message_dicts(messages, stop)
        params = {**params, **kwargs, "stream": True}
        params["stream_options"] = self.stream_options
        default_chunk_class = AIMessageChunk
        async for chunk in await self.acompletion_with_retry(
            messages=message_dicts, run_manager=run_manager, **params
        ):
            usage_metadata = None
            if not isinstance(chunk, dict):
                chunk = chunk.model_dump()
            if "usage" in chunk and chunk["usage"]:
                usage_metadata = _create_usage_metadata(chunk["usage"])
            if len(chunk["choices"]) == 0:
                continue
            delta = chunk["choices"][0]["delta"]
            # --- BRIDGE FIX: Inject Root Metadata into Delta ---
            root_metadata = chunk.get("provider_specific_fields")
            if not root_metadata:
                root_metadata = chunk.get("vertex_ai_grounding_metadata")
            
            if root_metadata:
                delta["provider_specific_fields"] = root_metadata
            # ---------------------------------------------------
            chunk = _convert_delta_to_message_chunk(delta, default_chunk_class)
            if usage_metadata and isinstance(chunk, AIMessageChunk):
                chunk.usage_metadata = usage_metadata
            default_chunk_class = chunk.__class__
            cg_chunk = ChatGenerationChunk(message=chunk)
            if run_manager:
                await run_manager.on_llm_new_token(chunk.content, chunk=cg_chunk)
            yield cg_chunk

    async def _agenerate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[AsyncCallbackManagerForLLMRun] = None,
        stream: Optional[bool] = None,
        **kwargs: Any,
    ) -> ChatResult:
        should_stream = stream if stream is not None else self.streaming
        if should_stream:
            stream_iter = self._astream(
                messages=messages, stop=stop, run_manager=run_manager, **kwargs
            )
            return await agenerate_from_stream(stream_iter)

        message_dicts, params = self._create_message_dicts(messages, stop)
        params = {**params, **kwargs}
        response = await self.acompletion_with_retry(
            messages=message_dicts, run_manager=run_manager, **params
        )
        return self._create_chat_result(response)

    def bind_tools(
        self,
        tools: Sequence[Union[Dict[str, Any], Type[BaseModel], Callable, BaseTool]],
        tool_choice: Optional[
            Union[dict, str, Literal["auto", "none", "required", "any"], bool]
        ] = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        """Bind tool-like objects to this chat model.

        LiteLLM expects tools argument in OpenAI format.

        Args:
            tools: A list of tool definitions to bind to this chat model.
                Can be  a dictionary, pydantic model, callable, or BaseTool. Pydantic
                models, callables, and BaseTools will be automatically converted to
                their schema dictionary representation.
            tool_choice: Which tool to require the model to call. Options are:
                - str of the form ``"<<tool_name>>"``: calls <<tool_name>> tool.
                - ``"auto"``:
                    automatically selects a tool (including no tool).
                - ``"none"``:
                    does not call a tool.
                - ``"any"`` or ``"required"`` or ``True``:
                    forces least one tool to be called.
                - dict of the form:
                ``{"type": "function", "function": {"name": <<tool_name>>}}``
                - ``False`` or ``None``: no effect
            **kwargs: Any additional parameters to pass to the
                :class:`~langchain.runnable.Runnable` constructor.
        """

        formatted_tools = [convert_to_openai_tool(tool) for tool in tools]

        # In case of openai if tool_choice is `any` or if bool has been provided we
        # change it to `required` as that is supported by openai.
        if (
            (self.model is not None and "azure" in self.model)
            or (self.model_name is not None and "azure" in self.model_name)
            or (self.model is not None and self.model in _OPENAI_MODELS)
            or (self.model_name is not None and self.model_name in _OPENAI_MODELS)
        ) and (tool_choice == "any" or isinstance(tool_choice, bool)):
            tool_choice = "required"
        # If tool_choice is bool apart from openai we make it `any`
        elif isinstance(tool_choice, bool):
            tool_choice = "any"
        elif isinstance(tool_choice, dict):
            tool_names = [
                formatted_tool["function"]["name"] for formatted_tool in formatted_tools
            ]
            if not any(
                tool_name == tool_choice["function"]["name"] for tool_name in tool_names
            ):
                raise ValueError(
                    f"Tool choice {tool_choice} was specified, but the only "
                    f"provided tools were {tool_names}."
                )
        return super().bind(tools=formatted_tools, tool_choice=tool_choice, **kwargs)
    
    def with_structured_output(
        self,
        schema: Union[Dict[str, Any], type, BaseModel],
        *,
        method: Optional[Literal["json_schema", "function_calling"]] = "json_schema",
        include_raw: bool = False,
        strict: Optional[bool] = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, Union[Dict, BaseModel]]:
        # Remove unsupported parameters
        _ = kwargs.pop("tools", None)
        if kwargs:
            msg = f"Received unsupported arguments {kwargs}"
            raise ValueError(msg)         

        if method == "function_calling":
            # pydantic
            if isinstance(schema, type) and is_basemodel_subclass(schema):
                parser = PydanticToolsParser(
                    tools=[cast(TypeBaseModel, schema)], 
                    first_tool_only=True
                )
                llm = self.bind_tools([schema], tool_choice="required")
            # dict or typeddict
            elif is_typeddict(schema) or isinstance(schema, dict):
                tool_def = convert_to_openai_tool(schema)
                function_name = tool_def['function']['name']
                parser = JsonOutputKeyToolsParser(
                    key_name=function_name,
                    first_tool_only=True
                )
                llm = self.bind_tools([tool_def], tool_choice="required")
            else:                
                msg = f"Unsupported schema type {type(schema)}"
                raise ValueError(msg)

        elif method == "json_schema":

            if strict is None:
                strict_flag = True
            else:
                strict_flag = strict

            # Setup parser for JSON text
            if isinstance(schema, type) and is_basemodel_subclass(schema):
                parser = PydanticOutputParser(pydantic_object=schema)
            else:
                parser = JsonOutputParser()
            
            # Setup LLM with json_schema
            tool_def = convert_to_openai_tool(schema)
            raw_schema = tool_def["function"]["parameters"]
            json_schema = _ensure_additional_properties_false(raw_schema)
            
            # Safe schema name extraction
            schema_name = getattr(schema, '__name__', tool_def["function"]["name"])

            llm = self.bind(
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": schema_name,
                        "schema": json_schema,
                        "strict": strict_flag
                    }
                }
            )
    
        if include_raw:
            parser_with_fallback = RunnablePassthrough.assign(
                parsed=itemgetter("raw") | parser,
                parsing_error=lambda _: None
            ).with_fallbacks(
                [RunnablePassthrough.assign(parsed=lambda _: None)],
                exception_key="parsing_error",
            )
            return {"raw": llm} | parser_with_fallback
        
        return llm | parser

    @property
    def _identifying_params(self) -> Dict[str, Any]:
        """Get the identifying parameters."""
        set_model_value = self.model
        if self.model_name is not None:
            set_model_value = self.model_name
        return {
            "model": set_model_value,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "top_k": self.top_k,
            "n": self.n,
        }

    @property
    def _llm_type(self) -> str:
        return "litellm-chat"


def _create_usage_metadata(token_usage: Mapping[str, Any]) -> UsageMetadata:
    input_tokens = token_usage.get("prompt_tokens", 0)
    output_tokens = token_usage.get("completion_tokens", 0)
    return UsageMetadata(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=input_tokens + output_tokens,
    )

def _ensure_additional_properties_false(schema_dict: dict) -> dict:
    """Recursively ensure additionalProperties is set to false for all objects."""
    if isinstance(schema_dict, dict):
        result = schema_dict.copy()
        
        if result.get("type") == "object":
            result["additionalProperties"] = False
        
        for key, value in result.items():
            if isinstance(value, dict):
                result[key] = _ensure_additional_properties_false(value)
            elif isinstance(value, list):
                result[key] = [
                    _ensure_additional_properties_false(item) 
                    if isinstance(item, dict) else item 
                    for item in value
                ]
        
        return result
    return schema_dict