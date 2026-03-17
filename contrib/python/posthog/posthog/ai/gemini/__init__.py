from .gemini import Client
from .gemini_async import AsyncClient
from .gemini_converter import (
    format_gemini_input,
    format_gemini_response,
    extract_gemini_tools,
)


# Create a genai-like module for perfect drop-in replacement
class _GenAI:
    Client = Client
    AsyncClient = AsyncClient


genai = _GenAI()

__all__ = [
    "Client",
    "AsyncClient",
    "genai",
    "format_gemini_input",
    "format_gemini_response",
    "extract_gemini_tools",
]
