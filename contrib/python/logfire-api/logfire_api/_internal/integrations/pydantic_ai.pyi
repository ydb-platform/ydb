from logfire import Logfire as Logfire
from pydantic_ai import Agent
from pydantic_ai.models import Model
from pydantic_ai.models.instrumented import InstrumentedModel
from typing import Any, Literal

def instrument_pydantic_ai(logfire_instance: Logfire, obj: Agent | Model | None, include_binary_content: bool | None, include_content: bool | None, version: Literal[1, 2, 3] | None, event_mode: Literal['attributes', 'logs'] | None, **kwargs: Any) -> None | InstrumentedModel: ...
