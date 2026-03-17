from typing import Any

import logfire

try:
    from openinference.instrumentation.dspy import DSPyInstrumentor
except ImportError:
    raise RuntimeError(
        'The `logfire.instrument_dspy()` method '
        'requires the `openinference-instrumentation-dspy` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[dspy]'"
    )


def instrument_dspy(logfire_instance: logfire.Logfire, **kwargs: Any):
    DSPyInstrumentor().instrument(
        tracer_provider=logfire_instance.config.get_tracer_provider(),
        **kwargs,
    )
