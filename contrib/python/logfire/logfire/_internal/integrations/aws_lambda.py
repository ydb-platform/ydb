from __future__ import annotations

try:
    from opentelemetry.context import Context
    from opentelemetry.instrumentation.aws_lambda import AwsLambdaInstrumentor
    from opentelemetry.metrics import MeterProvider
    from opentelemetry.trace import TracerProvider
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_aws_lambda()` requires the `opentelemetry-instrumentation-aws-lambda` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[aws-lambda]'"
    )

from typing import Any, Callable

LambdaEvent = Any
LambdaHandler = Callable[[LambdaEvent, Any], Any]


def instrument_aws_lambda(
    lambda_handler: LambdaHandler,
    *,
    tracer_provider: TracerProvider,
    meter_provider: MeterProvider,
    event_context_extractor: Callable[[LambdaEvent], Context] | None = None,
    **kwargs: Any,
) -> None:
    """Instrument the AWS Lambda runtime so that spans are automatically created for each invocation.

    See the `Logfire.instrument_aws_lambda` method for details.
    """
    if event_context_extractor is not None:
        kwargs['event_context_extractor'] = event_context_extractor
    return AwsLambdaInstrumentor().instrument(tracer_provider=tracer_provider, meter_provider=meter_provider, **kwargs)
