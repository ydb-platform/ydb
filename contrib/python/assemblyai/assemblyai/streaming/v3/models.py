from datetime import datetime
from enum import Enum
from typing import Any, List, Literal, Optional, Union

from pydantic import BaseModel


class LLMGatewayMessage(BaseModel):
    role: str
    content: str


class LLMGatewayConfig(BaseModel):
    model: str
    messages: List["LLMGatewayMessage"]
    max_tokens: int


class Word(BaseModel):
    start: int
    end: int
    confidence: float
    text: str
    word_is_final: bool


class TurnEvent(BaseModel):
    type: Literal["Turn"]
    turn_order: int
    turn_is_formatted: bool
    end_of_turn: bool
    transcript: str
    end_of_turn_confidence: float
    words: List[Word]
    language_code: Optional[str] = None
    language_confidence: Optional[float] = None


class BeginEvent(BaseModel):
    type: Literal["Begin"] = "Begin"
    id: str
    expires_at: datetime


class TerminationEvent(BaseModel):
    type: Literal["Termination"] = "Termination"
    audio_duration_seconds: Optional[int] = None
    session_duration_seconds: Optional[int] = None


class ErrorEvent(BaseModel):
    error: str


class LLMGatewayResponseEvent(BaseModel):
    type: Literal["LLMGatewayResponse"] = "LLMGatewayResponse"
    turn_order: int
    transcript: str
    data: Any


EventMessage = Union[
    BeginEvent,
    TerminationEvent,
    TurnEvent,
    ErrorEvent,
    LLMGatewayResponseEvent,
]


class TerminateSession(BaseModel):
    type: Literal["Terminate"] = "Terminate"


class ForceEndpoint(BaseModel):
    type: Literal["ForceEndpoint"] = "ForceEndpoint"


class StreamingSessionParameters(BaseModel):
    end_of_turn_confidence_threshold: Optional[float] = None
    min_end_of_turn_silence_when_confident: Optional[int] = None
    max_turn_silence: Optional[int] = None
    vad_threshold: Optional[float] = None
    format_turns: Optional[bool] = None
    keyterms_prompt: Optional[List[str]] = None
    filter_profanity: Optional[bool] = None
    prompt: Optional[str] = None


class Encoding(str, Enum):
    pcm_s16le = "pcm_s16le"
    pcm_mulaw = "pcm_mulaw"

    def __str__(self):
        return self.value


class SpeechModel(str, Enum):
    universal_streaming_multilingual = "universal-streaming-multilingual"
    universal_streaming_english = "universal-streaming-english"
    u3_pro = "u3-pro"

    def __str__(self):
        return self.value


class StreamingParameters(StreamingSessionParameters):
    sample_rate: int
    encoding: Optional[Encoding] = None
    speech_model: Optional[SpeechModel] = None
    language_detection: Optional[bool] = None
    inactivity_timeout: Optional[int] = None
    webhook_url: Optional[str] = None
    webhook_auth_header_name: Optional[str] = None
    webhook_auth_header_value: Optional[str] = None
    llm_gateway: Optional[LLMGatewayConfig] = None


class UpdateConfiguration(StreamingSessionParameters):
    type: Literal["UpdateConfiguration"] = "UpdateConfiguration"


OperationMessage = Union[
    bytes,
    TerminateSession,
    ForceEndpoint,
    UpdateConfiguration,
]


class StreamingClientOptions(BaseModel):
    api_host: str = "streaming.assemblyai.com"
    api_key: Optional[str] = None
    token: Optional[str] = None


class StreamingError(Exception):
    def __init__(self, message: str, code: Optional[int] = None):
        super().__init__(message)
        self.code = code


StreamingErrorCodes = {
    4000: "Sample rate must be a positive integer",
    4001: "Not Authorized",
    4002: "Insufficient Funds",
    4003: """This feature is paid-only and requires you to add a credit card.
    Please visit https://app.assemblyai.com/ to add a credit card to your account""",
    4004: "Session Not Found",
    4008: "Session Expired",
    4010: "Session Previously Closed",
    4029: "Client sent audio too fast",
    4030: "Session is handled by another websocket",
    4031: "Session idle for too long",
    4032: "Audio duration is too short",
    4033: "Audio duration is too long",
    4034: "Audio too small to transcode",
    4100: "Endpoint received invalid JSON",
    4101: "Endpoint received a message with an invalid schema",
    4102: "This account has exceeded the number of allowed streams",
    4103: "The session has been reconnected. This websocket is no longer valid.",
    1013: "Temporary server condition forced blocking client's request",
}


class StreamingEvents(Enum):
    Begin = "Begin"
    Termination = "Termination"
    Turn = "Turn"
    Error = "Error"
    LLMGatewayResponse = "LLMGatewayResponse"
