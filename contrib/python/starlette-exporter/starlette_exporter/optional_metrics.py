""" optional_metrics.py: Optional additional metrics

Additional metrics are defined here as string constants. The metric functionality
is located within the PrometheusMiddleware class and is enabled by passing one of these
constants to the `optional_metrics` constructor param.

Example:

```
from starlette_exporter import PrometheusMiddleware
from starlette_exporter.optional_metrics import response_body_size

app.add_middleware(PrometheusMiddleware, optional_metrics=[response_body_size, request_body_size])
```
"""


response_body_size = "response_body_size"
request_body_size = "request_body_size"

all_metrics = [
    response_body_size,
    request_body_size,
]
