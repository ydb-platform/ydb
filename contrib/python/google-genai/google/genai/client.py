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

import asyncio
import os
from types import TracebackType
from typing import Optional, Union

import google.auth
import pydantic

from ._api_client import BaseApiClient
from ._base_url import get_base_url
from ._replay_api_client import ReplayApiClient
from .batches import AsyncBatches, Batches
from .caches import AsyncCaches, Caches
from .chats import AsyncChats, Chats
from .file_search_stores import AsyncFileSearchStores, FileSearchStores
from .files import AsyncFiles, Files
from .live import AsyncLive
from .models import AsyncModels, Models
from .operations import AsyncOperations, Operations
from .tokens import AsyncTokens, Tokens
from .tunings import AsyncTunings, Tunings
from .types import HttpOptions, HttpOptionsDict, HttpRetryOptions

import warnings
import httpx

from ._api_client import has_aiohttp

from . import _common

from ._interactions import AsyncGeminiNextGenAPIClient, DEFAULT_MAX_RETRIES, GeminiNextGenAPIClient
from . import _interactions

from ._interactions.resources import AsyncInteractionsResource as AsyncNextGenInteractionsResource, InteractionsResource as NextGenInteractionsResource
_interactions_experimental_warned = False

class AsyncGeminiNextGenAPIClientAdapter(_interactions.AsyncGeminiNextGenAPIClientAdapter):
  """Adapter for the Gemini NextGen API Client."""
  def __init__(self, api_client: BaseApiClient):
    self._api_client = api_client

  def is_vertex_ai(self) -> bool:
    return self._api_client.vertexai or False

  def get_project(self) -> str | None:
    return self._api_client.project

  def get_location(self) -> str | None:
    return self._api_client.location

  async def async_get_auth_headers(self) -> dict[str, str]:
    if self._api_client.api_key:
      return {"x-goog-api-key": self._api_client.api_key}
    access_token = await self._api_client._async_access_token()
    headers = {
      "Authorization": f"Bearer {access_token}",
    }
    if creds := self._api_client._credentials:
      if creds.quota_project_id:
        headers["x-goog-user-project"] = creds.quota_project_id
    return headers


class GeminiNextGenAPIClientAdapter(_interactions.GeminiNextGenAPIClientAdapter):
  """Adapter for the Gemini NextGen API Client."""
  def __init__(self, api_client: BaseApiClient):
    self._api_client = api_client

  def is_vertex_ai(self) -> bool:
    return self._api_client.vertexai or False

  def get_project(self) -> str | None:
    return self._api_client.project

  def get_location(self) -> str | None:
    return self._api_client.location

  def get_auth_headers(self) -> dict[str, str]:
    if self._api_client.api_key:
      return {"x-goog-api-key": self._api_client.api_key}
    access_token = self._api_client._access_token()
    headers = {
      "Authorization": f"Bearer {access_token}",
    }
    if creds := self._api_client._credentials:
      if creds.quota_project_id:
        headers["x-goog-user-project"] = creds.quota_project_id
    return headers


class AsyncClient:
  """Client for making asynchronous (non-blocking) requests."""

  def __init__(self, api_client: BaseApiClient):

    self._api_client = api_client
    self._models = AsyncModels(self._api_client)
    self._tunings = AsyncTunings(self._api_client)
    self._caches = AsyncCaches(self._api_client)
    self._batches = AsyncBatches(self._api_client)
    self._files = AsyncFiles(self._api_client)
    self._file_search_stores = AsyncFileSearchStores(self._api_client)
    self._live = AsyncLive(self._api_client)
    self._tokens = AsyncTokens(self._api_client)
    self._operations = AsyncOperations(self._api_client)
    self._nextgen_client_instance: Optional[AsyncGeminiNextGenAPIClient] = None

  @property
  def _nextgen_client(self) -> AsyncGeminiNextGenAPIClient:
    if self._nextgen_client_instance is not None:
      return self._nextgen_client_instance

    http_opts = self._api_client._http_options

    if http_opts.extra_body:
      warnings.warn(
          'extra_body properties are not supported in `.interactions` yet',
          category=UserWarning,
          stacklevel=5,
      )

    retry_opts = http_opts.retry_options
    if retry_opts is not None and (
        retry_opts.initial_delay is not None
        or retry_opts.max_delay is not None
        or retry_opts.exp_base is not None
        or retry_opts.jitter is not None
        or retry_opts.http_status_codes is not None
    ):
      warnings.warn(
          'Granular retry options are not supported in `.interactions` yet',
          category=UserWarning,
          stacklevel=5,
      )

    http_client: httpx.AsyncClient = self._api_client._async_httpx_client

    async_client_args = self._api_client._http_options.async_client_args or {}
    has_custom_transport = 'transport' in async_client_args

    if has_aiohttp and not has_custom_transport:
      warnings.warn(
          'Async interactions client cannot use aiohttp, fallingback to httpx.',
          category=UserWarning,
          stacklevel=5,
      )

    if retry_opts is not None and retry_opts.attempts is not None:
      max_retries = retry_opts.attempts
    else:
      max_retries = DEFAULT_MAX_RETRIES + 1

    self._nextgen_client_instance = AsyncGeminiNextGenAPIClient(
        base_url=http_opts.base_url,
        api_key=self._api_client.api_key,
        api_version=http_opts.api_version,
        default_headers=http_opts.headers,
        http_client=http_client,
        # uSDk expects ms, nextgen uses a httpx Timeout -> expects seconds.
        timeout=http_opts.timeout / 1000 if http_opts.timeout else None,
        max_retries=max_retries,
        client_adapter=AsyncGeminiNextGenAPIClientAdapter(self._api_client)
    )

    client = self._nextgen_client_instance
    if self._api_client.vertexai:
      client._is_vertex = True
      client._vertex_project = self._api_client.project
      client._vertex_location = self._api_client.location

    return self._nextgen_client_instance

  @property
  def interactions(self) -> AsyncNextGenInteractionsResource:
    global _interactions_experimental_warned
    if not _interactions_experimental_warned:
      _interactions_experimental_warned = True
      warnings.warn(
          'Interactions usage is experimental and may change in future versions.',
          category=UserWarning,
          stacklevel=1,
      )
    return self._nextgen_client.interactions

  @property
  def _has_nextgen_client(self) -> bool:
    return (
        hasattr(self, '_nextgen_client_instance') and
        self._nextgen_client_instance is not None
    )

  @property
  def models(self) -> AsyncModels:
    return self._models

  @property
  def tunings(self) -> AsyncTunings:
    return self._tunings

  @property
  def caches(self) -> AsyncCaches:
    return self._caches

  @property
  def file_search_stores(self) -> AsyncFileSearchStores:
    return self._file_search_stores

  @property
  def batches(self) -> AsyncBatches:
    return self._batches

  @property
  def chats(self) -> AsyncChats:
    return AsyncChats(modules=self.models)

  @property
  def files(self) -> AsyncFiles:
    return self._files

  @property
  def live(self) -> AsyncLive:
    return self._live

  @property
  def auth_tokens(self) -> AsyncTokens:
    return self._tokens

  @property
  def operations(self) -> AsyncOperations:
    return self._operations

  async def aclose(self) -> None:
    """Closes the async client explicitly.

    However, it doesn't close the sync client, which can be closed using the
    Client.close() method or using the context manager.

    Usage:
    .. code-block:: python

      from google.genai import Client

      async_client = Client(
          vertexai=True, project='my-project-id', location='us-central1'
      ).aio
      response_1 = await async_client.models.generate_content(
          model='gemini-2.0-flash',
          contents='Hello World',
      )
      response_2 = await async_client.models.generate_content(
          model='gemini-2.0-flash',
          contents='Hello World',
      )
      # Close the client to release resources.
      await async_client.aclose()
    """
    await self._api_client.aclose()

    if self._has_nextgen_client:
      await self._nextgen_client.close()

  async def __aenter__(self) -> 'AsyncClient':
    return self

  async def __aexit__(
      self,
      exc_type: Optional[Exception],
      exc_value: Optional[Exception],
      traceback: Optional[TracebackType],
  ) -> None:
    await self.aclose()

  def __del__(self) -> None:
    try:
      asyncio.get_running_loop().create_task(self.aclose())
    except Exception:
      pass


class DebugConfig(pydantic.BaseModel):
  """Configuration options that change client network behavior when testing."""

  client_mode: Optional[str] = pydantic.Field(
      default_factory=lambda: os.getenv('GOOGLE_GENAI_CLIENT_MODE', None)
  )

  replays_directory: Optional[str] = pydantic.Field(
      default_factory=lambda: os.getenv('GOOGLE_GENAI_REPLAYS_DIRECTORY', None)
  )

  replay_id: Optional[str] = pydantic.Field(
      default_factory=lambda: os.getenv('GOOGLE_GENAI_REPLAY_ID', None)
  )



class Client:
  """Client for making synchronous requests.

  Use this client to make a request to the Gemini Developer API or Vertex AI
  API and then wait for the response.

  To initialize the client, provide the required arguments either directly
  or by using environment variables. Gemini API users and Vertex AI users in
  express mode can provide API key by providing input argument
  `api_key="your-api-key"` or by defining `GOOGLE_API_KEY="your-api-key"` as an
  environment variable

  Vertex AI API users can provide inputs argument as `vertexai=True,
  project="your-project-id", location="us-central1"` or by defining
  `GOOGLE_GENAI_USE_VERTEXAI=true`, `GOOGLE_CLOUD_PROJECT` and
  `GOOGLE_CLOUD_LOCATION` environment variables.

  Attributes:
    api_key: The `API key <https://ai.google.dev/gemini-api/docs/api-key>`_ to
      use for authentication. Applies to the Gemini Developer API only.
    vertexai: Indicates whether the client should use the Vertex AI API
      endpoints. Defaults to False (uses Gemini Developer API endpoints).
      Applies to the Vertex AI API only.
    credentials: The credentials to use for authentication when calling the
      Vertex AI APIs. Credentials can be obtained from environment variables and
      default credentials. For more information, see `Set up Application Default
      Credentials
      <https://cloud.google.com/docs/authentication/provide-credentials-adc>`_.
      Applies to the Vertex AI API only.
    project: The `Google Cloud project ID
      <https://cloud.google.com/vertex-ai/docs/start/cloud-environment>`_ to use
      for quota. Can be obtained from environment variables (for example,
      ``GOOGLE_CLOUD_PROJECT``). Applies to the Vertex AI API only.
      Find your `Google Cloud project ID <https://cloud.google.com/resource-manager/docs/creating-managing-projects#identifying_projects>`_.
    location: The `location
      <https://cloud.google.com/vertex-ai/generative-ai/docs/learn/locations>`_
      to send API requests to (for example, ``us-central1``). Can be obtained
      from environment variables. Applies to the Vertex AI API only.
    debug_config: Config settings that control network behavior of the client.
      This is typically used when running test code.
    http_options: Http options to use for the client. These options will be
      applied to all requests made by the client. Example usage: `client =
      genai.Client(http_options=types.HttpOptions(api_version='v1'))`.

  Usage for the Gemini Developer API:

  .. code-block:: python

    from google import genai

    client = genai.Client(api_key='my-api-key')

  Usage for the Vertex AI API:

  .. code-block:: python

    from google import genai

    client = genai.Client(
        vertexai=True, project='my-project-id', location='us-central1'
    )
  """

  def __init__(
      self,
      *,
      vertexai: Optional[bool] = None,
      api_key: Optional[str] = None,
      credentials: Optional[google.auth.credentials.Credentials] = None,
      project: Optional[str] = None,
      location: Optional[str] = None,
      debug_config: Optional[DebugConfig] = None,
      http_options: Optional[Union[HttpOptions, HttpOptionsDict]] = None,
  ):
    """Initializes the client.

    Args:
       vertexai (bool): Indicates whether the client should use the Vertex AI
         API endpoints. Defaults to False (uses Gemini Developer API endpoints).
         Applies to the Vertex AI API only.
       api_key (str): The `API key
         <https://ai.google.dev/gemini-api/docs/api-key>`_ to use for
         authentication. Applies to the Gemini Developer API only.
       credentials (google.auth.credentials.Credentials): The credentials to use
         for authentication when calling the Vertex AI APIs. Credentials can be
         obtained from environment variables and default credentials. For more
         information, see `Set up Application Default Credentials
         <https://cloud.google.com/docs/authentication/provide-credentials-adc>`_.
         Applies to the Vertex AI API only.
       project (str): The `Google Cloud project ID
         <https://cloud.google.com/vertex-ai/docs/start/cloud-environment>`_ to
         use for quota. Can be obtained from environment variables (for example,
         ``GOOGLE_CLOUD_PROJECT``). Applies to the Vertex AI API only.
       location (str): The `location
         <https://cloud.google.com/vertex-ai/generative-ai/docs/learn/locations>`_
         to send API requests to (for example, ``us-central1``). Can be obtained
         from environment variables. Applies to the Vertex AI API only.
       debug_config (DebugConfig): Config settings that control network behavior
         of the client. This is typically used when running test code.
       http_options (Union[HttpOptions, HttpOptionsDict]): Http options to use
         for the client.
    """

    self._debug_config = debug_config or DebugConfig()
    if isinstance(http_options, dict):
      http_options = HttpOptions(**http_options)

    base_url = get_base_url(vertexai or False, http_options)
    if base_url:
      if http_options:
        http_options.base_url = base_url
      else:
        http_options = HttpOptions(base_url=base_url)

    self._api_client = self._get_api_client(
        vertexai=vertexai,
        api_key=api_key,
        credentials=credentials,
        project=project,
        location=location,
        debug_config=self._debug_config,
        http_options=http_options,
    )

    self._aio = AsyncClient(self._api_client)
    self._models = Models(self._api_client)
    self._tunings = Tunings(self._api_client)
    self._caches = Caches(self._api_client)
    self._file_search_stores = FileSearchStores(self._api_client)
    self._batches = Batches(self._api_client)
    self._files = Files(self._api_client)
    self._tokens = Tokens(self._api_client)
    self._operations = Operations(self._api_client)
    self._nextgen_client_instance: Optional[GeminiNextGenAPIClient] = None

  @staticmethod
  def _get_api_client(
      vertexai: Optional[bool] = None,
      api_key: Optional[str] = None,
      credentials: Optional[google.auth.credentials.Credentials] = None,
      project: Optional[str] = None,
      location: Optional[str] = None,
      debug_config: Optional[DebugConfig] = None,
      http_options: Optional[HttpOptions] = None,
  ) -> BaseApiClient:
    if debug_config and debug_config.client_mode in [
        'record',
        'replay',
        'auto',
    ]:
      return ReplayApiClient(
          mode=debug_config.client_mode,  # type: ignore[arg-type]
          replay_id=debug_config.replay_id,  # type: ignore[arg-type]
          replays_directory=debug_config.replays_directory,
          vertexai=vertexai,  # type: ignore[arg-type]
          api_key=api_key,
          credentials=credentials,
          project=project,
          location=location,
          http_options=http_options,
      )

    return BaseApiClient(
        vertexai=vertexai,
        api_key=api_key,
        credentials=credentials,
        project=project,
        location=location,
        http_options=http_options,
    )

  @property
  def _nextgen_client(self) -> GeminiNextGenAPIClient:
    if self._nextgen_client_instance is not None:
      return self._nextgen_client_instance

    http_opts = self._api_client._http_options

    if http_opts.extra_body:
      warnings.warn(
          'extra_body properties are not supported in `.interactions` yet',
          category=UserWarning,
          stacklevel=5,
      )

    retry_opts = http_opts.retry_options
    if retry_opts is not None and (
        retry_opts.initial_delay is not None
        or retry_opts.max_delay is not None
        or retry_opts.exp_base is not None
        or retry_opts.jitter is not None
        or retry_opts.http_status_codes is not None
    ):
      warnings.warn(
          'Granular retry options are not supported in `.interactions` yet',
          category=UserWarning,
          stacklevel=5,
      )

    if retry_opts is not None and retry_opts.attempts is not None:
      max_retries = retry_opts.attempts
    else:
      max_retries = DEFAULT_MAX_RETRIES + 1

    self._nextgen_client_instance = GeminiNextGenAPIClient(
        base_url=http_opts.base_url,
        api_key=self._api_client.api_key,
        api_version=http_opts.api_version,
        default_headers=http_opts.headers,
        http_client=self._api_client._httpx_client,
        # uSDk expects ms, nextgen uses a httpx Timeout -> expects seconds.
        timeout=http_opts.timeout / 1000 if http_opts.timeout else None,
        max_retries=max_retries,
        client_adapter=GeminiNextGenAPIClientAdapter(self._api_client),
    )

    client = self._nextgen_client_instance
    if self._api_client.vertexai:
      client._is_vertex = True
      client._vertex_project = self._api_client.project
      client._vertex_location = self._api_client.location

    return self._nextgen_client_instance

  @property
  def interactions(self) -> NextGenInteractionsResource:
    global _interactions_experimental_warned
    if not _interactions_experimental_warned:
      _interactions_experimental_warned = True
      warnings.warn(
        'Interactions usage is experimental and may change in future versions.',
        category=UserWarning,
        stacklevel=2,
      )
    return self._nextgen_client.interactions

  @property
  def _has_nextgen_client(self) -> bool:
    return (
        hasattr(self, '_nextgen_client_instance') and
        self._nextgen_client_instance is not None
    )

  @property
  def chats(self) -> Chats:
    return Chats(modules=self.models)

  @property
  def aio(self) -> AsyncClient:
    return self._aio

  @property
  def models(self) -> Models:
    return self._models

  @property
  def tunings(self) -> Tunings:
    return self._tunings

  @property
  def caches(self) -> Caches:
    return self._caches

  @property
  def file_search_stores(self) -> FileSearchStores:
    return self._file_search_stores

  @property
  def batches(self) -> Batches:
    return self._batches

  @property
  def files(self) -> Files:
    return self._files

  @property
  def auth_tokens(self) -> Tokens:
    return self._tokens

  @property
  def operations(self) -> Operations:
    return self._operations

  @property
  def vertexai(self) -> bool:
    """Returns whether the client is using the Vertex AI API."""
    return self._api_client.vertexai or False

  def close(self) -> None:
    """Closes the synchronous client explicitly.

    However, it doesn't close the async client, which can be closed using the
    Client.aio.aclose() method or using the async context manager.

    Usage:
    .. code-block:: python

      from google.genai import Client

      client = Client(
          vertexai=True, project='my-project-id', location='us-central1'
      )
      response_1 = client.models.generate_content(
          model='gemini-2.0-flash',
          contents='Hello World',
      )
      response_2 = client.models.generate_content(
          model='gemini-2.0-flash',
          contents='Hello World',
      )
      # Close the client to release resources.
      client.close()
    """
    self._api_client.close()

    if self._has_nextgen_client:
      self._nextgen_client.close()

  def __enter__(self) -> 'Client':
    return self

  def __exit__(
      self,
      exc_type: Optional[Exception],
      exc_value: Optional[Exception],
      traceback: Optional[TracebackType],
  ) -> None:
    self.close()

  def __del__(self) -> None:
    try:
      self.close()
    except Exception:
      pass
