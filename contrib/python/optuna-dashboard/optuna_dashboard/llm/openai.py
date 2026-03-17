from __future__ import annotations

import openai
from typing import TYPE_CHECKING

from .provider import RateLimitExceeded
from .provider import InvalidAuthentication


if TYPE_CHECKING:
    from openai.types.shared import ResponsesModel
    from openai.types.chat import ChatCompletionMessageParam


__all__ = ["OpenAI", "AzureOpenAI"]


class OpenAI:
    """An LLMProvider implementation for OpenAI and its compatible APIs.

    Args:
        client: An instance of the ``openai.OpenAI`` client.
        model: The model name.
        use_chat_completions_api: If ``True``, Chat Completions API will be used instead
            of the Responses API. For a comparison between the two APIs, see the
            official documentation:
            https://platform.openai.com/docs/guides/responses-vs-chat-completions
    """

    def __init__(
        self,
        client: openai.OpenAI,
        *,
        model: ResponsesModel,
        use_chat_completions_api: bool = False,
    ) -> None:
        self._client = client
        self._model = model
        self._use_chat_completions_api = use_chat_completions_api

    def call(self, prompt: str) -> str:
        try:
            if self._use_chat_completions_api:
                return _call_chat_completions_api(self._client, self._model, prompt)
            else:
                return _call_responses_api(self._client, self._model, prompt)
        except openai.RateLimitError as e:
            raise RateLimitExceeded() from e
        except openai.AuthenticationError as e:
            raise InvalidAuthentication() from e


class AzureOpenAI:
    """An LLMProvider implementation for Microsoft Azure OpenAI.

    Args:
        client: An instance of the ``openai.OpenAI`` client.
        model: The model name.
        use_chat_completions_api: If ``True``, Chat Completions API will be used instead
            of the Responses API. For a comparison between the two APIs, see the
            official documentation:
            https://platform.openai.com/docs/guides/responses-vs-chat-completions
    """

    def __init__(
        self,
        client: openai.AzureOpenAI,
        *,
        model: ResponsesModel,
        use_chat_completions_api: bool = False,
    ) -> None:
        self._client = client
        self._model = model
        self._use_chat_completions_api = use_chat_completions_api

    def call(self, prompt: str) -> str:
        try:
            if self._use_chat_completions_api:
                return _call_chat_completions_api(self._client, self._model, prompt)
            else:
                return _call_responses_api(self._client, self._model, prompt)
        except openai.RateLimitError as e:
            raise RateLimitExceeded() from e
        except openai.AuthenticationError as e:
            raise InvalidAuthentication() from e


def _call_responses_api(
    client: openai.OpenAI | openai.AzureOpenAI,
    model: ResponsesModel,
    prompt: str,
) -> str:
    # According to the documentation, the `model` parameter is optional in OpenAI's Responses API.
    # However, it is actually required — otherwise, a 400 error is returned with the message:
    # "Missing required parameter: 'model'."
    response = client.responses.create(input=prompt, model=model)
    return response.output_text


def _call_chat_completions_api(
    client: openai.OpenAI | openai.AzureOpenAI,
    model: ResponsesModel,
    prompt: str,
) -> str:
    messages: list[ChatCompletionMessageParam] = [{"role": "user", "content": prompt}]
    completion = client.chat.completions.create(messages=messages, model=model)
    response = completion.choices[0].message.content
    assert response is not None, "response must not be empty if no exceptions raised"
    return response
