from __future__ import annotations

import inspect
from typing import Any, Literal

from pydantic_ai import Agent
from pydantic_ai.agent import InstrumentationSettings
from pydantic_ai.models import Model
from pydantic_ai.models.instrumented import InstrumentedModel

from logfire import Logfire


def instrument_pydantic_ai(
    logfire_instance: Logfire,
    obj: Agent | Model | None,
    include_binary_content: bool | None,
    include_content: bool | None,
    version: Literal[1, 2, 3] | None,
    event_mode: Literal['attributes', 'logs'] | None,
    **kwargs: Any,
) -> None | InstrumentedModel:
    # Correctly handling all past and future versions is tricky.
    # Since we provide these rather than the user, only include them if
    # InstrumentationSettings has such parameters to prevent errors/warnings.
    expected_kwarg_names = inspect.signature(InstrumentationSettings.__init__).parameters
    final_kwargs: dict[str, Any] = {
        k: v
        for k, v in dict(
            tracer_provider=logfire_instance.config.get_tracer_provider(),
            meter_provider=logfire_instance.config.get_meter_provider(),  # not in old versions
            logger_provider=logfire_instance.config.get_logger_provider(),
        ).items()
        if k in expected_kwarg_names
    }

    # Now add known parameters that the user provided explicitly.
    # None of these have `None` as a valid value, so we assume the user didn't pass that.
    # Include even if not in expected_kwarg_names, to let Pydantic AI produce an error or warning.
    final_kwargs.update(
        {
            k: v
            for k, v in dict(
                include_binary_content=include_binary_content,
                include_content=include_content,
                version=version,
                event_mode=event_mode,
            ).items()
            if v is not None
        }
    )

    # Finally, add any other kwargs the user provided. These are mainly for future compatibility.
    # They also provide an escape hatch to override the tracer/meter/event_logger providers.
    final_kwargs.update(kwargs)

    settings = InstrumentationSettings(**final_kwargs)
    if isinstance(obj, Agent):
        obj.instrument = settings
    elif isinstance(obj, Model):
        return InstrumentedModel(obj, settings)
    elif obj is None:
        Agent.instrument_all(settings)
    else:
        raise TypeError(f'Cannot instrument object of type {type(obj)}')
