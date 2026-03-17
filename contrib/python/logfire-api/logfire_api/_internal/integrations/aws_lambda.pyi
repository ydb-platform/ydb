from opentelemetry.context import Context as Context
from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import TracerProvider
from typing import Any, Callable

LambdaEvent = Any
LambdaHandler = Callable[[LambdaEvent, Any], Any]

def instrument_aws_lambda(lambda_handler: LambdaHandler, *, tracer_provider: TracerProvider, meter_provider: MeterProvider, event_context_extractor: Callable[[LambdaEvent], Context] | None = None, **kwargs: Any) -> None:
    """Instrument the AWS Lambda runtime so that spans are automatically created for each invocation.

    See the `Logfire.instrument_aws_lambda` method for details.
    """
