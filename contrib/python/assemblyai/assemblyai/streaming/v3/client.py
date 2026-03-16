import json
import logging
import queue
import sys
import threading
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Union
from urllib.parse import urlencode

import httpx
import websockets
from pydantic import BaseModel
from websockets.sync.client import connect as websocket_connect

from assemblyai import __version__

from .models import (
    BeginEvent,
    ErrorEvent,
    EventMessage,
    ForceEndpoint,
    LLMGatewayResponseEvent,
    OperationMessage,
    StreamingClientOptions,
    StreamingError,
    StreamingErrorCodes,
    StreamingEvents,
    StreamingParameters,
    StreamingSessionParameters,
    TerminateSession,
    TerminationEvent,
    TurnEvent,
    UpdateConfiguration,
)

logger = logging.getLogger(__name__)


def _dump_model(model: BaseModel):
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_none=True)
    return model.dict(exclude_none=True)


def _dump_model_json(model: BaseModel):
    if hasattr(model, "model_dump_json"):
        return model.model_dump_json(exclude_none=True)
    return model.json(exclude_none=True)


def _user_agent() -> str:
    vi = sys.version_info
    python_version = f"{vi.major}.{vi.minor}.{vi.micro}"
    return (
        f"AssemblyAI/1.0 (sdk=Python/{__version__} runtime_env=Python/{python_version})"
    )


class StreamingClient:
    def __init__(self, options: StreamingClientOptions):
        self._options = options

        self._client = _HTTPClient(api_host=options.api_host, api_key=options.api_key)

        self._handlers: Dict[StreamingEvents, List[Callable]] = {}

        for event in StreamingEvents.__members__.values():
            self._handlers[event] = []

        self._write_queue: queue.Queue[OperationMessage] = queue.Queue()
        self._write_thread = threading.Thread(target=self._write_message)
        self._read_thread = threading.Thread(target=self._read_message)
        self._stop_event = threading.Event()

    def connect(self, params: StreamingParameters) -> None:
        params_dict = _dump_model(params)

        # JSON-encode list and dict parameters for proper API compatibility (e.g., keyterms_prompt, llm_gateway)
        for key, value in params_dict.items():
            if isinstance(value, list):
                params_dict[key] = json.dumps(value)
            elif isinstance(value, dict):
                params_dict[key] = json.dumps(value)

        params_encoded = urlencode(params_dict)

        uri = f"wss://{self._options.api_host}/v3/ws?{params_encoded}"
        headers = {
            "Authorization": self._options.token
            if self._options.token
            else self._options.api_key,
            "User-Agent": _user_agent(),
            "AssemblyAI-Version": "2025-05-12",
        }

        try:
            self._websocket = websocket_connect(
                uri,
                additional_headers=headers,
                open_timeout=15,
            )
        except websockets.exceptions.ConnectionClosed as exc:
            self._handle_error(exc)
            return

        self._write_thread.start()
        self._read_thread.start()

        logger.debug("Connected to WebSocket server")

    def disconnect(self, terminate: bool = False) -> None:
        if terminate and not self._stop_event.is_set():
            self._write_queue.put(TerminateSession())

        try:
            self._read_thread.join()
            self._write_thread.join()

            if self._websocket:
                self._websocket.close()
        except Exception:
            pass

    def stream(
        self, data: Union[bytes, Generator[bytes, None, None], Iterable[bytes]]
    ) -> None:
        if isinstance(data, bytes):
            self._write_queue.put(data)
            return

        for chunk in data:
            self._write_queue.put(chunk)

    def set_params(self, params: StreamingSessionParameters):
        message = UpdateConfiguration(**_dump_model(params))
        self._write_queue.put(message)

    def force_endpoint(self):
        message = ForceEndpoint()
        self._write_queue.put(message)

    def on(self, event: StreamingEvents, handler: Callable) -> None:
        if event in StreamingEvents.__members__.values() and callable(handler):
            self._handlers[event].append(handler)

    def _write_message(self) -> None:
        while not self._stop_event.is_set():
            if not self._websocket:
                raise ValueError("Not connected to the WebSocket server")

            try:
                data = self._write_queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                if isinstance(data, bytes):
                    self._websocket.send(data)
                elif isinstance(data, BaseModel):
                    self._websocket.send(_dump_model_json(data))
                else:
                    raise ValueError(f"Attempted to send invalid message: {type(data)}")
            except websockets.exceptions.ConnectionClosed as exc:
                self._handle_error(exc)
                return

    def _read_message(self) -> None:
        while not self._stop_event.is_set():
            if not self._websocket:
                raise ValueError("Not connected to the WebSocket server")

            try:
                message_data = self._websocket.recv(timeout=1)
            except TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed as exc:
                self._handle_error(exc)
                return

            try:
                message_json = json.loads(message_data)
            except json.JSONDecodeError as exc:
                logger.warning(f"Failed to decode message: {exc}")
                continue

            message = self._parse_message(message_json)

            if isinstance(message, ErrorEvent):
                self._handle_error(message)
            elif message:
                self._handle_message(message)
            else:
                logger.warning(f"Unsupported event type: {message_json['type']}")

    def _handle_message(self, message: EventMessage) -> None:
        if isinstance(message, TerminationEvent):
            self._stop_event.set()

        event_type = StreamingEvents[message.type]

        for handler in self._handlers[event_type]:
            handler(self, message)

    def _parse_message(self, data: Dict[str, Any]) -> Optional[EventMessage]:
        if "type" in data:
            message_type = data.get("type")

            event_type = self._parse_event_type(message_type)

            if event_type == StreamingEvents.Begin:
                return BeginEvent.model_validate(data)
            elif event_type == StreamingEvents.Termination:
                return TerminationEvent.model_validate(data)
            elif event_type == StreamingEvents.Turn:
                return TurnEvent.model_validate(data)
            elif event_type == StreamingEvents.LLMGatewayResponse:
                return LLMGatewayResponseEvent.model_validate(data)
            else:
                return None
        elif "error" in data:
            return ErrorEvent.model_validate(data)

        return None

    @staticmethod
    def _parse_event_type(message_type: Optional[Any]) -> Optional[StreamingEvents]:
        if not isinstance(message_type, str):
            return None

        try:
            return StreamingEvents[message_type]
        except KeyError:
            return None

    def _handle_error(
        self,
        error: Union[
            ErrorEvent,
            websockets.exceptions.ConnectionClosed,
        ],
    ):
        parsed_error = self._parse_error(error)

        for handler in self._handlers[StreamingEvents.Error]:
            handler(self, parsed_error)

        self.disconnect()

    def _parse_error(
        self,
        error: Union[
            ErrorEvent,
            websockets.exceptions.ConnectionClosed,
        ],
    ) -> StreamingError:
        if isinstance(error, ErrorEvent):
            return StreamingError(
                message=error.error,
            )
        elif isinstance(error, websockets.exceptions.ConnectionClosed):
            if (
                error.code >= 4000
                and error.code <= 4999
                and error.code in StreamingErrorCodes
            ):
                error_message = StreamingErrorCodes[error.code]
            else:
                error_message = error.reason

            if error.code != 1000:
                return StreamingError(message=error_message, code=error.code)

        return StreamingError(
            message=f"Unknown error: {error}",
        )

    def create_temporary_token(
        self,
        expires_in_seconds: int,
        max_session_duration_seconds: Optional[int] = None,
    ) -> str:
        return self._client.create_temporary_token(
            expires_in_seconds=expires_in_seconds,
            max_session_duration_seconds=max_session_duration_seconds,
        )


class _HTTPClient:
    def __init__(self, api_host: str, api_key: Optional[str] = None):
        vi = sys.version_info
        python_version = f"{vi.major}.{vi.minor}.{vi.micro}"
        user_agent = f"{httpx._client.USER_AGENT} AssemblyAI/1.0 (sdk=Python/{__version__} runtime_env=Python/{python_version})"

        headers = {"User-Agent": user_agent}

        if api_key:
            headers["Authorization"] = api_key

        self._http_client = httpx.Client(
            base_url="https://" + api_host,
            headers=headers,
        )

    def create_temporary_token(
        self,
        expires_in_seconds: int,
        max_session_duration_seconds: Optional[int] = None,
    ) -> str:
        params: Dict[str, Any] = {}

        if expires_in_seconds:
            params["expires_in_seconds"] = expires_in_seconds

        if max_session_duration_seconds:
            params["max_session_duration_seconds"] = max_session_duration_seconds

        response = self._http_client.get(
            "/v3/token",
            params=params,
        )

        response.raise_for_status()
        return response.json()["token"]
