import asyncio
import json
import subprocess
import typing
from enum import Enum

if typing.TYPE_CHECKING:
    from websockets.asyncio.client import ClientConnection


class RealtimeEvents(str, Enum):
    """Events emitted by the RealtimeConnection"""
    OPEN = "open"
    CLOSE = "close"
    SESSION_STARTED = "session_started"
    PARTIAL_TRANSCRIPT = "partial_transcript"
    COMMITTED_TRANSCRIPT = "committed_transcript"
    COMMITTED_TRANSCRIPT_WITH_TIMESTAMPS = "committed_transcript_with_timestamps"
    ERROR = "error"
    AUTH_ERROR = "auth_error"
    QUOTA_EXCEEDED = "quota_exceeded"
    COMMIT_THROTTLED = "commit_throttled"
    TRANSCRIBER_ERROR = "transcriber_error"
    UNACCEPTED_TERMS_ERROR = "unaccepted_terms_error"
    RATE_LIMITED = "rate_limited"
    INPUT_ERROR = "input_error"
    QUEUE_OVERFLOW = "queue_overflow"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    SESSION_TIME_LIMIT_EXCEEDED = "session_time_limit_exceeded"
    CHUNK_SIZE_EXCEEDED = "chunk_size_exceeded"
    INSUFFICIENT_AUDIO_ACTIVITY = "insufficient_audio_activity"



class RealtimeConnection:
    """
    A WebSocket connection for real-time speech-to-text transcription.

    This class handles bidirectional WebSocket communication with the ElevenLabs
    speech-to-text API, managing audio streaming and receiving transcription results.

    Example:
        ```python
        connection = await client.speech_to_text.realtime.connect({
            "audio_format": AudioFormat.PCM_16000,
            "sample_rate": 16000
        })

        connection.on(RealtimeEvents.PARTIAL_TRANSCRIPT, lambda data: print(data))
        connection.on(RealtimeEvents.COMMITTED_TRANSCRIPT, lambda data: print(data))

        # Send audio
        connection.send({"audioBase64": audio_chunk})

        # When done
        connection.commit()
        await connection.close()
        ```
    """

    def __init__(self, websocket: "ClientConnection", current_sample_rate: int, ffmpeg_process: typing.Optional[subprocess.Popen] = None):
        self.websocket = websocket
        self.current_sample_rate = current_sample_rate
        self.ffmpeg_process = ffmpeg_process
        self._event_handlers: typing.Dict[str, typing.List[typing.Callable]] = {}
        self._message_task: typing.Optional[asyncio.Task] = None

    def on(self, event: str, callback: typing.Callable) -> None:
        """
        Register an event handler for a specific event type.

        Args:
            event: The event type to listen for (from RealtimeEvents enum)
            callback: The function to call when the event occurs

        Example:
            ```python
            def handle_transcript(data):
                print(f"Transcript: {data['transcript']}")

            connection.on(RealtimeEvents.PARTIAL_TRANSCRIPT, handle_transcript)
            ```
        """
        if event not in self._event_handlers:
            self._event_handlers[event] = []
        self._event_handlers[event].append(callback)

    def _emit(self, event: str, *args) -> None:
        """Emit an event to all registered handlers"""
        if event in self._event_handlers:
            for handler in self._event_handlers[event]:
                try:
                    handler(*args)
                except Exception as e:
                    print(f"Error in event handler for {event}: {e}")

    async def _start_message_handler(self) -> None:
        """Start handling incoming WebSocket messages"""
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    message_type = data.get("message_type")

                    # Try to match message_type to a known event
                    try:
                        event = RealtimeEvents(message_type)
                        self._emit(event, data)

                        # Also emit generic ERROR event for specific error types
                        error_events = {
                            RealtimeEvents.AUTH_ERROR,
                            RealtimeEvents.QUOTA_EXCEEDED,
                            RealtimeEvents.COMMIT_THROTTLED,
                            RealtimeEvents.TRANSCRIBER_ERROR,
                            RealtimeEvents.UNACCEPTED_TERMS_ERROR,
                            RealtimeEvents.RATE_LIMITED,
                            RealtimeEvents.INPUT_ERROR,
                            RealtimeEvents.QUEUE_OVERFLOW,
                            RealtimeEvents.RESOURCE_EXHAUSTED,
                            RealtimeEvents.SESSION_TIME_LIMIT_EXCEEDED,
                            RealtimeEvents.CHUNK_SIZE_EXCEEDED,
                            RealtimeEvents.INSUFFICIENT_AUDIO_ACTIVITY,
                        }
                        if event in error_events:
                            self._emit(RealtimeEvents.ERROR, data)
                    except ValueError:
                        # Unknown message type, ignore
                        pass
                except json.JSONDecodeError as e:
                    self._emit(RealtimeEvents.ERROR, {"error": f"Failed to parse message: {e}"})
        except Exception as e:
            self._emit(RealtimeEvents.ERROR, {"error": str(e)})
        finally:
            self._emit(RealtimeEvents.CLOSE)

    async def send(self, data: typing.Dict[str, typing.Any]) -> None:
        """
        Send an audio chunk to the server for transcription.

        Args:
            data: Dictionary containing the following keys:
                - audio_base_64 (str): Base64-encoded audio data to transcribe
                - previous_text (str, optional): Previous transcript text to provide context
                  for more accurate transcription

        Raises:
            RuntimeError: If the WebSocket connection is not open

        Example:
            ```python
            # Send audio chunk
            connection.send({
                "audio_base_64": base64_encoded_audio
            })

            # Send audio chunk with context - can only be sent with the first chunk of audio
            connection.send({
                "audio_base_64": base64_encoded_audio,
                "previous_text": "Previously transcribed text for context"
            })
            ```
        """
        if not self.websocket:
            raise RuntimeError("WebSocket is not connected")

        message = {
            "message_type": "input_audio_chunk",
            "audio_base_64": data.get("audio_base_64", ""),
            "commit": False,
            "sample_rate": self.current_sample_rate,
            "previous_text": data.get("previous_text"),
        }

        await self.websocket.send(json.dumps(message))

    async def commit(self) -> None:
        """
        Commits the segment, triggering a COMMITTED_TRANSCRIPT event and clearing the buffer.
        It's recommend to commit often when using CommitStrategy.MANUAL to keep latency low.

        Raises:
            RuntimeError: If the WebSocket connection is not open

        Remarks:
            Only needed when using CommitStrategy.MANUAL.
            When using CommitStrategy.VAD, commits are handled automatically by the server.

        Example:
            ```python
            # Send all audio chunks
            for chunk in audio_chunks:
                connection.send({"audioBase64": chunk})

            # Commit the audio segment
            await connection.commit()
            ```
        """
        if not self.websocket:
            raise RuntimeError("WebSocket is not connected")

        message = {
            "message_type": "input_audio_chunk",
            "audio_base_64": "",
            "commit": True,
            "sample_rate": self.current_sample_rate,
        }

        await self.websocket.send(json.dumps(message))

    async def close(self) -> None:
        """
        Closes the WebSocket connection and cleans up resources.
        This will terminate any ongoing transcription and stop ffmpeg processes if running.

        Remarks:
            After calling close(), this connection cannot be reused.
            Create a new connection if you need to start transcribing again.

        Example:
            ```python
            connection.on(RealtimeEvents.COMMITTED_TRANSCRIPT, async lambda data: (
                print("Committed:", data["transcript"]),
                await connection.close()
            ))
            ```
        """
        await self._cleanup()
        if self.websocket:
            await self.websocket.close(1000, "User ended conversation")
        if self._message_task and not self._message_task.done():
            self._message_task.cancel()
            try:
                await self._message_task
            except asyncio.CancelledError:
                pass

    async def _cleanup(self) -> None:
        """Clean up resources like ffmpeg processes"""
        if self.ffmpeg_process:
            self.ffmpeg_process.kill()
            try:
                self.ffmpeg_process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.ffmpeg_process.kill()
            self.ffmpeg_process = None

