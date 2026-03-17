from typing import Any

import logfire

try:
    from openinference.instrumentation.litellm import LiteLLMInstrumentor
except ImportError:
    raise RuntimeError(
        'The `logfire.instrument_litellm()` method '
        'requires the `openinference-instrumentation-litellm` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[litellm]'"
    )


def instrument_litellm(logfire_instance: logfire.Logfire, **kwargs: Any):
    LiteLLMInstrumentor().instrument(
        tracer_provider=logfire_instance.config.get_tracer_provider(),
        **kwargs,
    )
