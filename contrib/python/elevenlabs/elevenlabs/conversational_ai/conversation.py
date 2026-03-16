from abc import ABC, abstractmethod
import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
import json
import logging
import threading
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional, Tuple, Union
import urllib.parse

import websockets
from websockets.exceptions import ConnectionClosedOK
from websockets.sync.client import Connection, connect

from ..base_client import BaseElevenLabs
from ..version import __version__


logger = logging.getLogger(__name__)


class ClientToOrchestratorEvent(str, Enum):
    """Event types that can be sent from client to orchestrator."""

    # Response to a ping request.
    PONG = "pong"
    CLIENT_TOOL_RESULT = "client_tool_result"
    CONVERSATION_INITIATION_CLIENT_DATA = "conversation_initiation_client_data"
    FEEDBACK = "feedback"
    # Non-interrupting content that is sent to the server to update the conversation state.
    CONTEXTUAL_UPDATE = "contextual_update"
    # User text message.
    USER_MESSAGE = "user_message"
    USER_ACTIVITY = "user_activity"


class AgentChatResponsePartType(str, Enum):
    START = "start"
    DELTA = "delta"
    STOP = "stop"


class UserMessageClientToOrchestratorEvent:
    """Event for sending user text messages."""

    def __init__(self, text: Optional[str] = None):
        self.type: Literal[ClientToOrchestratorEvent.USER_MESSAGE] = ClientToOrchestratorEvent.USER_MESSAGE
        self.text = text

    def to_dict(self) -> dict:
        return {"type": self.type, "text": self.text}


class UserActivityClientToOrchestratorEvent:
    """Event for registering user activity (ping to prevent timeout)."""

    def __init__(self) -> None:
        self.type: Literal[ClientToOrchestratorEvent.USER_ACTIVITY] = ClientToOrchestratorEvent.USER_ACTIVITY

    def to_dict(self) -> dict:
        return {"type": self.type}


class ContextualUpdateClientToOrchestratorEvent:
    """Event for sending non-interrupting contextual updates to the conversation state."""

    def __init__(self, text: str):
        self.type: Literal[ClientToOrchestratorEvent.CONTEXTUAL_UPDATE] = ClientToOrchestratorEvent.CONTEXTUAL_UPDATE
        self.text = text

    def to_dict(self) -> dict:
        return {"type": self.type, "text": self.text}


class AudioInterface(ABC):
    """AudioInterface provides an abstraction for handling audio input and output."""

    @abstractmethod
    def start(self, input_callback: Callable[[bytes], None]):
        """Starts the audio interface.

        Called one time before the conversation starts.
        The `input_callback` should be called regularly with input audio chunks from
        the user. The audio should be in 16-bit PCM mono format at 16kHz. Recommended
        chunk size is 4000 samples (250 milliseconds).
        """
        pass

    @abstractmethod
    def stop(self):
        """Stops the audio interface.

        Called one time after the conversation ends. Should clean up any resources
        used by the audio interface and stop any audio streams. Do not call the
        `input_callback` from `start` after this method is called.
        """
        pass

    @abstractmethod
    def output(self, audio: bytes):
        """Output audio to the user.

        The `audio` input is in 16-bit PCM mono format at 16kHz. Implementations can
        choose to do additional buffering. This method should return quickly and not
        block the calling thread.
        """
        pass

    @abstractmethod
    def interrupt(self):
        """Interruption signal to stop any audio output.

        User has interrupted the agent and all previosly buffered audio output should
        be stopped.
        """
        pass


class AsyncAudioInterface(ABC):
    """AsyncAudioInterface provides an async abstraction for handling audio input and output."""

    @abstractmethod
    async def start(self, input_callback: Callable[[bytes], Awaitable[None]]):
        """Starts the audio interface.

        Called one time before the conversation starts.
        The `input_callback` should be called regularly with input audio chunks from
        the user. The audio should be in 16-bit PCM mono format at 16kHz. Recommended
        chunk size is 4000 samples (250 milliseconds).
        """
        pass

    @abstractmethod
    async def stop(self):
        """Stops the audio interface.

        Called one time after the conversation ends. Should clean up any resources
        used by the audio interface and stop any audio streams. Do not call the
        `input_callback` from `start` after this method is called.
        """
        pass

    @abstractmethod
    async def output(self, audio: bytes):
        """Output audio to the user.

        The `audio` input is in 16-bit PCM mono format at 16kHz. Implementations can
        choose to do additional buffering. This method should return quickly and not
        block the calling thread.
        """
        pass

    @abstractmethod
    async def interrupt(self):
        """Interruption signal to stop any audio output.

        User has interrupted the agent and all previosly buffered audio output should
        be stopped.
        """
        pass


class ClientTools:
    """Handles registration and execution of client-side tools that can be called by the agent.

    Supports both synchronous and asynchronous tools running in a dedicated event loop,
    ensuring non-blocking operation of the main conversation thread.

    Args:
        loop: Optional custom asyncio event loop to use for tool execution. If not provided,
              a new event loop will be created and run in a separate thread. Using a custom
              loop prevents "different event loop" runtime errors and allows for better
              context propagation and resource management.
    """

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.tools: Dict[str, Tuple[Union[Callable[[dict], Any], Callable[[dict], Awaitable[Any]]], bool]] = {}
        self.lock = threading.Lock()
        self._custom_loop = loop
        self._loop = None
        self._thread = None
        self._running = threading.Event()
        self.thread_pool = ThreadPoolExecutor()

    def start(self):
        """Start the event loop in a separate thread for handling async operations."""
        if self._running.is_set():
            return

        if self._custom_loop is not None:
            # Use the provided custom event loop
            self._loop = self._custom_loop
            self._running.set()
        else:
            # Create and run our own event loop in a separate thread
            def run_event_loop():
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
                self._running.set()
                try:
                    self._loop.run_forever()
                finally:
                    self._running.clear()
                    self._loop.close()
                    self._loop = None

            self._thread = threading.Thread(target=run_event_loop, daemon=True, name="ClientTools-EventLoop")
            self._thread.start()
            # Wait for loop to be ready
            self._running.wait()

    def stop(self):
        """Gracefully stop the event loop and clean up resources."""
        if self._loop and self._running.is_set():
            if self._custom_loop is not None:
                # For custom loops, we don't stop the loop itself, just clear our running flag
                self._running.clear()
            else:
                # For our own loop, stop it and join the thread
                self._loop.call_soon_threadsafe(self._loop.stop)
                if self._thread:
                    self._thread.join()
            self.thread_pool.shutdown(wait=False)

    def register(
        self,
        tool_name: str,
        handler: Union[Callable[[dict], Any], Callable[[dict], Awaitable[Any]]],
        is_async: bool = False,
    ) -> None:
        """Register a new tool that can be called by the AI agent.

        Args:
            tool_name: Unique identifier for the tool
            handler: Function that implements the tool's logic
            is_async: Whether the handler is an async function
        """
        with self.lock:
            if not callable(handler):
                raise ValueError("Handler must be callable")
            if tool_name in self.tools:
                raise ValueError(f"Tool '{tool_name}' is already registered")
            self.tools[tool_name] = (handler, is_async)

    async def handle(self, tool_name: str, parameters: dict) -> Any:
        """Execute a registered tool with the given parameters.

        Returns the result of the tool execution.
        """
        with self.lock:
            if tool_name not in self.tools:
                raise ValueError(f"Tool '{tool_name}' is not registered")
            handler, is_async = self.tools[tool_name]

        if is_async:
            return await handler(parameters)
        else:
            return await asyncio.get_event_loop().run_in_executor(self.thread_pool, handler, parameters)

    def execute_tool(self, tool_name: str, parameters: dict, callback: Callable[[dict], None]):
        """Execute a tool and send its result via the provided callback.

        This method is non-blocking and handles both sync and async tools.
        """
        if not self._running.is_set():
            raise RuntimeError("ClientTools event loop is not running")

        if self._loop is None:
            raise RuntimeError("Event loop is not available")

        async def _execute_and_callback():
            try:
                result = await self.handle(tool_name, parameters)
                response = {
                    "type": "client_tool_result",
                    "tool_call_id": parameters.get("tool_call_id"),
                    "result": result or f"Client tool: {tool_name} called successfully.",
                    "is_error": False,
                }
            except Exception as e:
                response = {
                    "type": "client_tool_result",
                    "tool_call_id": parameters.get("tool_call_id"),
                    "result": str(e),
                    "is_error": True,
                }
            callback(response)

        self._schedule_coroutine(_execute_and_callback())

    def _schedule_coroutine(self, coro):
        """Schedule a coroutine on the appropriate event loop."""
        if self._custom_loop is not None:
            return self._loop.create_task(coro)
        else:
            return asyncio.run_coroutine_threadsafe(coro, self._loop)


class ConversationInitiationData:
    """Configuration options for the Conversation."""

    def __init__(
        self,
        extra_body: Optional[dict] = None,
        conversation_config_override: Optional[dict] = None,
        dynamic_variables: Optional[dict] = None,
        user_id: Optional[str] = None,
    ):
        self.extra_body = extra_body or {}
        self.conversation_config_override = conversation_config_override or {}
        self.dynamic_variables = dynamic_variables or {}
        self.user_id = user_id


class OnPremInitiationData:
    """Configuration options for the Conversation in on-prem mode."""

    def __init__(
        self,
        on_prem_conversation_url: str,
        post_call_transcription_webhook_url: Optional[str] = None,
        post_call_audio_webhook_url: Optional[str] = None,
        agent_config_dict: Optional[dict] = None,
        override_agent_config_list: Optional[List[dict]] = None,
        tools_config_list: Optional[List[dict]] = None,
        prompt_knowledge_base: Optional[List[str]] = None,
    ):
        self.on_prem_conversation_url = on_prem_conversation_url
        self.post_call_transcription_webhook_url = post_call_transcription_webhook_url
        self.post_call_audio_webhook_url = post_call_audio_webhook_url
        self.agent_config_dict = agent_config_dict
        self.override_agent_config_list = override_agent_config_list
        self.tools_config_list = tools_config_list
        self.prompt_knowledge_base = prompt_knowledge_base


@dataclass
class AudioEventAlignment:
    """Audio alignment data containing character-level timing information. d"""

    chars: List[str]
    char_start_times_ms: List[int]
    char_durations_ms: List[int]


class BaseConversation:
    """Base class for conversation implementations with shared parameters and logic."""

    def __init__(
        self,
        client: BaseElevenLabs,
        agent_id: str,
        user_id: Optional[str] = None,
        *,
        requires_auth: bool,
        config: Optional[ConversationInitiationData] = None,
        client_tools: Optional[ClientTools] = None,
        on_prem_config: Optional[OnPremInitiationData] = None,
    ):
        self.client = client
        self.agent_id = agent_id
        self.user_id = user_id
        self.requires_auth = requires_auth
        self.config = config or ConversationInitiationData()
        self.client_tools = client_tools or ClientTools()
        self.on_prem_config = on_prem_config

        self.client_tools.start()

        self._conversation_id = None
        self._last_interrupt_id = 0

    def _get_wss_url(self):
        if self.on_prem_config:
            return self.on_prem_config.on_prem_conversation_url

        base_http_url = self.client._client_wrapper.get_base_url()
        base_ws_url = (
            urllib.parse.urlparse(base_http_url)
            ._replace(scheme="wss" if base_http_url.startswith("https") else "ws")
            .geturl()
        )
        # Ensure base URL ends with '/' for proper joining
        if not base_ws_url.endswith("/"):
            base_ws_url += "/"
        return f"{base_ws_url}v1/convai/conversation?agent_id={self.agent_id}&source=python_sdk&version={__version__}"

    def _get_signed_url(self):
        response = self.client.conversational_ai.conversations.get_signed_url(agent_id=self.agent_id)
        signed_url = response.signed_url
        # Append source and version query parameters to the signed URL
        separator = "&" if "?" in signed_url else "?"
        return f"{signed_url}{separator}source=python_sdk&version={__version__}"

    def _create_on_prem_initiation_message(self):
        return json.dumps(
            {
                "type": "enclave_setup_config",
                "agent_config_dict": self.on_prem_config.agent_config_dict,
                "override_agent_config_list": self.on_prem_config.override_agent_config_list,
                "tools_config_list": self.on_prem_config.tools_config_list,
                "post_call_transcription_webhook_url": self.on_prem_config.post_call_transcription_webhook_url,
                "post_call_audio_webhook_url": self.on_prem_config.post_call_audio_webhook_url,
                "prompt_knowledge_base": self.on_prem_config.prompt_knowledge_base,
            }
        )

    def _create_initiation_message(self):
        return json.dumps(
            {
                "type": "conversation_initiation_client_data",
                "custom_llm_extra_body": self.config.extra_body,
                "conversation_config_override": self.config.conversation_config_override,
                "dynamic_variables": self.config.dynamic_variables,
                "source_info": {
                    "source": "python_sdk",
                    "version": __version__,
                },
                **({"user_id": self.user_id} if self.user_id else {}),
            }
        )

    def _handle_message_core(self, message, message_handler):
        """Core message handling logic shared between sync and async implementations.

        Args:
            message: The parsed message dictionary
            message_handler: Handler object with methods for different operations
        """
        if message["type"] == "conversation_initiation_metadata":
            event = message["conversation_initiation_metadata_event"]
            assert self._conversation_id is None
            self._conversation_id = event["conversation_id"]

        elif message["type"] == "audio":
            event = message["audio_event"]
            if int(event["event_id"]) <= self._last_interrupt_id:
                return
            audio = base64.b64decode(event["audio_base_64"])
            message_handler.handle_audio_output(audio)

            if message_handler.callback_audio_alignment and "alignment" in event:
                alignment_data = event["alignment"]
                alignment = AudioEventAlignment(
                    chars=alignment_data.get("chars", []),
                    char_start_times_ms=alignment_data.get("char_start_times_ms", []),
                    char_durations_ms=alignment_data.get("char_durations_ms", []),
                )
                message_handler.handle_audio_alignment(alignment)

        elif message["type"] == "agent_response":
            if message_handler.callback_agent_response:
                event = message["agent_response_event"]
                message_handler.handle_agent_response(event["agent_response"].strip())

        elif message["type"] == "agent_chat_response_part":
            if message_handler.callback_agent_chat_response_part:
                event = message.get("text_response_part", {})
                text = event.get("text", "")
                part_type_str = event.get("type", "delta")
                try:
                    part_type = AgentChatResponsePartType(part_type_str)
                except ValueError:
                    part_type = AgentChatResponsePartType.DELTA
                message_handler.handle_agent_chat_response_part(text, part_type)

        elif message["type"] == "agent_response_correction":
            if message_handler.callback_agent_response_correction:
                event = message["agent_response_correction_event"]
                message_handler.handle_agent_response_correction(
                    event["original_agent_response"].strip(), event["corrected_agent_response"].strip()
                )

        elif message["type"] == "user_transcript":
            if message_handler.callback_user_transcript:
                event = message["user_transcription_event"]
                message_handler.handle_user_transcript(event["user_transcript"].strip())

        elif message["type"] == "interruption":
            event = message["interruption_event"]
            self._last_interrupt_id = int(event["event_id"])
            message_handler.handle_interruption()

        elif message["type"] == "ping":
            event = message["ping_event"]
            message_handler.handle_ping(event)
            if message_handler.callback_latency_measurement and event["ping_ms"]:
                message_handler.handle_latency_measurement(int(event["ping_ms"]))

        elif message["type"] == "client_tool_call":
            tool_call = message.get("client_tool_call", {})
            tool_name = tool_call.get("tool_name")
            parameters = {"tool_call_id": tool_call["tool_call_id"], **tool_call.get("parameters", {})}
            message_handler.handle_client_tool_call(tool_name, parameters)
        else:
            pass  # Ignore all other message types.

    async def _handle_message_core_async(self, message, message_handler):
        """Async wrapper for core message handling logic."""
        if message["type"] == "conversation_initiation_metadata":
            event = message["conversation_initiation_metadata_event"]
            assert self._conversation_id is None
            self._conversation_id = event["conversation_id"]

        elif message["type"] == "audio":
            event = message["audio_event"]
            if int(event["event_id"]) <= self._last_interrupt_id:
                return
            audio = base64.b64decode(event["audio_base_64"])
            await message_handler.handle_audio_output(audio)

            if message_handler.callback_audio_alignment and "alignment" in event:
                alignment_data = event["alignment"]
                alignment = AudioEventAlignment(
                    chars=alignment_data.get("chars", []),
                    char_start_times_ms=alignment_data.get("char_start_times_ms", []),
                    char_durations_ms=alignment_data.get("char_durations_ms", []),
                )
                await message_handler.handle_audio_alignment(alignment)

        elif message["type"] == "agent_response":
            if message_handler.callback_agent_response:
                event = message["agent_response_event"]
                await message_handler.handle_agent_response(event["agent_response"].strip())

        elif message["type"] == "agent_chat_response_part":
            if message_handler.callback_agent_chat_response_part:
                event = message.get("text_response_part", {})
                text = event.get("text", "")
                part_type_str = event.get("type", "delta")
                try:
                    part_type = AgentChatResponsePartType(part_type_str)
                except ValueError:
                    part_type = AgentChatResponsePartType.DELTA
                await message_handler.handle_agent_chat_response_part(text, part_type)

        elif message["type"] == "agent_response_correction":
            if message_handler.callback_agent_response_correction:
                event = message["agent_response_correction_event"]
                await message_handler.handle_agent_response_correction(
                    event["original_agent_response"].strip(), event["corrected_agent_response"].strip()
                )

        elif message["type"] == "user_transcript":
            if message_handler.callback_user_transcript:
                event = message["user_transcription_event"]
                await message_handler.handle_user_transcript(event["user_transcript"].strip())

        elif message["type"] == "interruption":
            event = message["interruption_event"]
            self._last_interrupt_id = int(event["event_id"])
            await message_handler.handle_interruption()

        elif message["type"] == "ping":
            event = message["ping_event"]
            await message_handler.handle_ping(event)
            if message_handler.callback_latency_measurement and event["ping_ms"]:
                await message_handler.handle_latency_measurement(int(event["ping_ms"]))

        elif message["type"] == "client_tool_call":
            tool_call = message.get("client_tool_call", {})
            tool_name = tool_call.get("tool_name")
            parameters = {"tool_call_id": tool_call["tool_call_id"], **tool_call.get("parameters", {})}
            message_handler.handle_client_tool_call(tool_name, parameters)
        else:
            pass  # Ignore all other message types.


class Conversation(BaseConversation):
    audio_interface: AudioInterface
    callback_agent_response: Optional[Callable[[str], None]]
    callback_agent_response_correction: Optional[Callable[[str, str], None]]
    callback_agent_chat_response_part: Optional[Callable[[str, AgentChatResponsePartType], None]]
    callback_user_transcript: Optional[Callable[[str], None]]
    callback_latency_measurement: Optional[Callable[[int], None]]
    callback_audio_alignment: Optional[Callable[[AudioEventAlignment], None]]
    callback_end_session: Optional[Callable]

    _thread: Optional[threading.Thread]
    _should_stop: threading.Event
    _ws: Optional[Connection]

    def __init__(
        self,
        client: BaseElevenLabs,
        agent_id: str,
        user_id: Optional[str] = None,
        *,
        requires_auth: bool,
        audio_interface: AudioInterface,
        config: Optional[ConversationInitiationData] = None,
        client_tools: Optional[ClientTools] = None,
        callback_agent_response: Optional[Callable[[str], None]] = None,
        callback_agent_response_correction: Optional[Callable[[str, str], None]] = None,
        callback_agent_chat_response_part: Optional[Callable[[str, AgentChatResponsePartType], None]] = None,
        callback_user_transcript: Optional[Callable[[str], None]] = None,
        callback_latency_measurement: Optional[Callable[[int], None]] = None,
        callback_audio_alignment: Optional[Callable[[AudioEventAlignment], None]] = None,
        callback_end_session: Optional[Callable] = None,
        on_prem_config: Optional[OnPremInitiationData] = None,
    ):
        """Conversational AI session.

        BETA: This API is subject to change without regard to backwards compatibility.

        Args:
            client: The ElevenLabs client to use for the conversation.
            agent_id: The ID of the agent to converse with.
            user_id: The ID of the user conversing with the agent.
            requires_auth: Whether the agent requires authentication.
            audio_interface: The audio interface to use for input and output.
            client_tools: The client tools to use for the conversation.
            callback_agent_response: Callback for agent responses.
            callback_agent_response_correction: Callback for agent response corrections.
                First argument is the original response (previously given to
                callback_agent_response), second argument is the corrected response.
            callback_agent_chat_response_part: Callback for streaming text response chunks.
                First argument is the text chunk, second argument is the type (START, DELTA, STOP).
            callback_user_transcript: Callback for user transcripts.
            callback_latency_measurement: Callback for latency measurements (in milliseconds).
            callback_audio_alignment: Callback for audio alignment data with character-level timing.
        """

        super().__init__(
            client=client,
            agent_id=agent_id,
            user_id=user_id,
            requires_auth=requires_auth,
            config=config,
            client_tools=client_tools,
            on_prem_config=on_prem_config,
        )

        self.audio_interface = audio_interface
        self.callback_agent_response = callback_agent_response
        self.callback_agent_response_correction = callback_agent_response_correction
        self.callback_agent_chat_response_part = callback_agent_chat_response_part
        self.callback_user_transcript = callback_user_transcript
        self.callback_latency_measurement = callback_latency_measurement
        self.callback_audio_alignment = callback_audio_alignment
        self.callback_end_session = callback_end_session

        self._thread = None
        self._ws: Optional[Connection] = None
        self._should_stop = threading.Event()

    def start_session(self):
        """Starts the conversation session.

        Will run in background thread until `end_session` is called.
        """
        ws_url = self._get_signed_url() if self.requires_auth else self._get_wss_url()
        self._thread = threading.Thread(target=self._run, args=(ws_url,))
        self._thread.start()

    def end_session(self):
        """Ends the conversation session and cleans up resources."""
        self.audio_interface.stop()
        self.client_tools.stop()
        self._ws = None
        self._should_stop.set()

        if self.callback_end_session:
            self.callback_end_session()

    def wait_for_session_end(self) -> Optional[str]:
        """Waits for the conversation session to end.

        You must call `end_session` before calling this method, otherwise it will block.

        Returns the conversation ID, if available.
        """
        if not self._thread:
            raise RuntimeError("Session not started.")
        self._thread.join()
        return self._conversation_id

    def send_user_message(self, text: str):
        """Send a text message from the user to the agent.

        Args:
            text: The text message to send to the agent.

        Raises:
            RuntimeError: If the session is not active or websocket is not connected.
        """
        if not self._ws:
            raise RuntimeError("Session not started or websocket not connected.")

        event = UserMessageClientToOrchestratorEvent(text=text)
        try:
            self._ws.send(json.dumps(event.to_dict()))
        except Exception as e:
            logger.error(f"Error sending user message: {e}")
            raise

    def register_user_activity(self):
        """Register user activity to prevent session timeout.

        This sends a ping to the orchestrator to reset the timeout timer.

        Raises:
            RuntimeError: If the session is not active or websocket is not connected.
        """
        if not self._ws:
            raise RuntimeError("Session not started or websocket not connected.")

        event = UserActivityClientToOrchestratorEvent()
        try:
            self._ws.send(json.dumps(event.to_dict()))
        except Exception as e:
            logger.error(f"Error registering user activity: {e}")
            raise

    def send_contextual_update(self, text: str):
        """Send a contextual update to the conversation.

        Contextual updates are non-interrupting content that is sent to the server
        to update the conversation state without directly prompting the agent.

        Args:
            content: The contextual information to send to the conversation.

        Raises:
            RuntimeError: If the session is not active or websocket is not connected.
        """
        if not self._ws:
            raise RuntimeError("Session not started or websocket not connected.")

        event = ContextualUpdateClientToOrchestratorEvent(text=text)
        try:
            self._ws.send(json.dumps(event.to_dict()))
        except Exception as e:
            logger.error(f"Error sending contextual update: {e}")
            raise

    def _run(self, ws_url: str):
        with connect(ws_url, max_size=16 * 1024 * 1024) as ws:
            self._ws = ws
            if self.on_prem_config:
                ws.send(self._create_on_prem_initiation_message())
            ws.send(self._create_initiation_message())
            self._ws = ws

            def input_callback(audio):
                try:
                    ws.send(
                        json.dumps(
                            {
                                "user_audio_chunk": base64.b64encode(audio).decode(),
                            }
                        )
                    )
                except ConnectionClosedOK:
                    self.end_session()
                except Exception as e:
                    logger.error(f"Error sending user audio chunk: {e}")
                    self.end_session()

            self.audio_interface.start(input_callback)
            while not self._should_stop.is_set():
                try:
                    message = json.loads(ws.recv(timeout=0.5))
                    if self._should_stop.is_set():
                        return
                    self._handle_message(message, ws)
                except ConnectionClosedOK as e:
                    self.end_session()
                except TimeoutError:
                    pass
                except Exception as e:
                    logger.error(f"Error receiving message: {e}")
                    self.end_session()

            self._ws = None

    def _handle_message(self, message, ws):
        class SyncMessageHandler:
            def __init__(self, conversation, ws):
                self.conversation = conversation
                self.ws = ws
                self.callback_agent_response = conversation.callback_agent_response
                self.callback_agent_response_correction = conversation.callback_agent_response_correction
                self.callback_agent_chat_response_part = conversation.callback_agent_chat_response_part
                self.callback_user_transcript = conversation.callback_user_transcript
                self.callback_latency_measurement = conversation.callback_latency_measurement
                self.callback_audio_alignment = conversation.callback_audio_alignment

            def handle_audio_output(self, audio):
                self.conversation.audio_interface.output(audio)

            def handle_audio_alignment(self, alignment):
                self.conversation.callback_audio_alignment(alignment)

            def handle_agent_response(self, response):
                self.conversation.callback_agent_response(response)

            def handle_agent_response_correction(self, original, corrected):
                self.conversation.callback_agent_response_correction(original, corrected)

            def handle_agent_chat_response_part(self, text, part_type):
                self.conversation.callback_agent_chat_response_part(text, part_type)

            def handle_user_transcript(self, transcript):
                self.conversation.callback_user_transcript(transcript)

            def handle_interruption(self):
                self.conversation.audio_interface.interrupt()

            def handle_ping(self, event):
                self.ws.send(
                    json.dumps(
                        {
                            "type": "pong",
                            "event_id": event["event_id"],
                        }
                    )
                )

            def handle_latency_measurement(self, latency):
                self.conversation.callback_latency_measurement(latency)

            def handle_client_tool_call(self, tool_name, parameters):
                def send_response(response):
                    if not self.conversation._should_stop.is_set():
                        self.ws.send(json.dumps(response))

                self.conversation.client_tools.execute_tool(tool_name, parameters, send_response)

        handler = SyncMessageHandler(self, ws)
        self._handle_message_core(message, handler)


class AsyncConversation(BaseConversation):
    audio_interface: AsyncAudioInterface
    callback_agent_response: Optional[Callable[[str], Awaitable[None]]]
    callback_agent_response_correction: Optional[Callable[[str, str], Awaitable[None]]]
    callback_agent_chat_response_part: Optional[Callable[[str, AgentChatResponsePartType], Awaitable[None]]]
    callback_user_transcript: Optional[Callable[[str], Awaitable[None]]]
    callback_latency_measurement: Optional[Callable[[int], Awaitable[None]]]
    callback_audio_alignment: Optional[Callable[[AudioEventAlignment], Awaitable[None]]]
    callback_end_session: Optional[Callable[[], Awaitable[None]]]

    _task: Optional[asyncio.Task]
    _should_stop: asyncio.Event
    _ws: Optional[websockets.WebSocketClientProtocol]

    def __init__(
        self,
        client: BaseElevenLabs,
        agent_id: str,
        user_id: Optional[str] = None,
        *,
        requires_auth: bool,
        audio_interface: AsyncAudioInterface,
        config: Optional[ConversationInitiationData] = None,
        client_tools: Optional[ClientTools] = None,
        callback_agent_response: Optional[Callable[[str], Awaitable[None]]] = None,
        callback_agent_response_correction: Optional[Callable[[str, str], Awaitable[None]]] = None,
        callback_agent_chat_response_part: Optional[Callable[[str, AgentChatResponsePartType], Awaitable[None]]] = None,
        callback_user_transcript: Optional[Callable[[str], Awaitable[None]]] = None,
        callback_latency_measurement: Optional[Callable[[int], Awaitable[None]]] = None,
        callback_audio_alignment: Optional[Callable[[AudioEventAlignment], Awaitable[None]]] = None,
        callback_end_session: Optional[Callable[[], Awaitable[None]]] = None,
        on_prem_config: Optional[OnPremInitiationData] = None,
    ):
        """Async Conversational AI session.

        BETA: This API is subject to change without regard to backwards compatibility.

        Args:
            client: The ElevenLabs client to use for the conversation.
            agent_id: The ID of the agent to converse with.
            user_id: The ID of the user conversing with the agent.
            requires_auth: Whether the agent requires authentication.
            audio_interface: The async audio interface to use for input and output.
            client_tools: The client tools to use for the conversation.
            callback_agent_response: Async callback for agent responses.
            callback_agent_response_correction: Async callback for agent response corrections.
                First argument is the original response (previously given to
                callback_agent_response), second argument is the corrected response.
            callback_agent_chat_response_part: Async callback for streaming text response chunks.
                First argument is the text chunk, second argument is the type (START, DELTA, STOP).
            callback_user_transcript: Async callback for user transcripts.
            callback_latency_measurement: Async callback for latency measurements (in milliseconds).
            callback_audio_alignment: Async callback for audio alignment data with character-level timing.
            callback_end_session: Async callback for when session ends.
        """

        super().__init__(
            client=client,
            agent_id=agent_id,
            user_id=user_id,
            requires_auth=requires_auth,
            config=config,
            client_tools=client_tools,
            on_prem_config=on_prem_config,
        )

        self.audio_interface = audio_interface
        self.callback_agent_response = callback_agent_response
        self.callback_agent_response_correction = callback_agent_response_correction
        self.callback_agent_chat_response_part = callback_agent_chat_response_part
        self.callback_user_transcript = callback_user_transcript
        self.callback_latency_measurement = callback_latency_measurement
        self.callback_audio_alignment = callback_audio_alignment
        self.callback_end_session = callback_end_session

        self._task = None
        self._ws = None
        self._should_stop = asyncio.Event()

    async def start_session(self):
        """Starts the conversation session.

        Will run in background task until `end_session` is called.
        """
        ws_url = self._get_signed_url() if self.requires_auth else self._get_wss_url()
        self._task = asyncio.create_task(self._run(ws_url))

    async def end_session(self):
        """Ends the conversation session and cleans up resources."""
        await self.audio_interface.stop()
        self.client_tools.stop()
        self._ws = None
        self._should_stop.set()

        if self.callback_end_session:
            await self.callback_end_session()

    async def wait_for_session_end(self) -> Optional[str]:
        """Waits for the conversation session to end.

        You must call `end_session` before calling this method, otherwise it will block.

        Returns the conversation ID, if available.
        """
        if not self._task:
            raise RuntimeError("Session not started.")
        await self._task
        return self._conversation_id

    async def send_user_message(self, text: str):
        """Send a text message from the user to the agent.

        Args:
            text: The text message to send to the agent.

        Raises:
            RuntimeError: If the session is not active or websocket is not connected.
        """
        if not self._ws:
            raise RuntimeError("Session not started or websocket not connected.")

        event = UserMessageClientToOrchestratorEvent(text=text)
        try:
            await self._ws.send(json.dumps(event.to_dict()))
        except Exception as e:
            logger.error(f"Error sending user message: {e}")
            raise

    async def register_user_activity(self):
        """Register user activity to prevent session timeout.

        This sends a ping to the orchestrator to reset the timeout timer.

        Raises:
            RuntimeError: If the session is not active or websocket is not connected.
        """
        if not self._ws:
            raise RuntimeError("Session not started or websocket not connected.")

        event = UserActivityClientToOrchestratorEvent()
        try:
            await self._ws.send(json.dumps(event.to_dict()))
        except Exception as e:
            logger.error(f"Error registering user activity: {e}")
            raise

    async def send_contextual_update(self, text: str):
        """Send a contextual update to the conversation.

        Contextual updates are non-interrupting content that is sent to the server
        to update the conversation state without directly prompting the agent.

        Args:
            text: The contextual information to send to the conversation.

        Raises:
            RuntimeError: If the session is not active or websocket is not connected.
        """
        if not self._ws:
            raise RuntimeError("Session not started or websocket not connected.")

        event = ContextualUpdateClientToOrchestratorEvent(text=text)
        try:
            await self._ws.send(json.dumps(event.to_dict()))
        except Exception as e:
            logger.error(f"Error sending contextual update: {e}")
            raise

    async def _run(self, ws_url: str):
        async with websockets.connect(ws_url, max_size=16 * 1024 * 1024) as ws:
            self._ws = ws
            if self.on_prem_config:
                await ws.send(self._create_on_prem_initiation_message())
            await ws.send(self._create_initiation_message())

            async def input_callback(audio):
                try:
                    await ws.send(
                        json.dumps(
                            {
                                "user_audio_chunk": base64.b64encode(audio).decode(),
                            }
                        )
                    )
                except ConnectionClosedOK:
                    await self.end_session()
                except Exception as e:
                    logger.error(f"Error sending user audio chunk: {e}")
                    await self.end_session()

            await self.audio_interface.start(input_callback)

            try:
                while not self._should_stop.is_set():
                    try:
                        message_str = await asyncio.wait_for(ws.recv(), timeout=0.5)
                        if self._should_stop.is_set():
                            return
                        message = json.loads(message_str)
                        await self._handle_message(message, ws)
                    except asyncio.TimeoutError:
                        pass
                    except ConnectionClosedOK:
                        await self.end_session()
                        break
                    except Exception as e:
                        logger.error(f"Error receiving message: {e}")
                        await self.end_session()
                        break
            finally:
                self._ws = None

    async def _handle_message(self, message, ws):
        class AsyncMessageHandler:
            def __init__(self, conversation, ws):
                self.conversation = conversation
                self.ws = ws
                self.callback_agent_response = conversation.callback_agent_response
                self.callback_agent_response_correction = conversation.callback_agent_response_correction
                self.callback_agent_chat_response_part = conversation.callback_agent_chat_response_part
                self.callback_user_transcript = conversation.callback_user_transcript
                self.callback_latency_measurement = conversation.callback_latency_measurement
                self.callback_audio_alignment = conversation.callback_audio_alignment

            async def handle_audio_output(self, audio):
                await self.conversation.audio_interface.output(audio)

            async def handle_audio_alignment(self, alignment):
                await self.conversation.callback_audio_alignment(alignment)

            async def handle_agent_response(self, response):
                await self.conversation.callback_agent_response(response)

            async def handle_agent_response_correction(self, original, corrected):
                await self.conversation.callback_agent_response_correction(original, corrected)

            async def handle_agent_chat_response_part(self, text, part_type):
                await self.conversation.callback_agent_chat_response_part(text, part_type)

            async def handle_user_transcript(self, transcript):
                await self.conversation.callback_user_transcript(transcript)

            async def handle_interruption(self):
                await self.conversation.audio_interface.interrupt()

            async def handle_ping(self, event):
                await self.ws.send(
                    json.dumps(
                        {
                            "type": "pong",
                            "event_id": event["event_id"],
                        }
                    )
                )

            async def handle_latency_measurement(self, latency):
                await self.conversation.callback_latency_measurement(latency)

            def handle_client_tool_call(self, tool_name, parameters):
                def send_response(response):
                    if not self.conversation._should_stop.is_set():
                        asyncio.create_task(self.ws.send(json.dumps(response)))

                self.conversation.client_tools.execute_tool(tool_name, parameters, send_response)

        handler = AsyncMessageHandler(self, ws)

        # Use the shared core message handling logic with async wrapper
        await self._handle_message_core_async(message, handler)
