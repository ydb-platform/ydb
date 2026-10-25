# OpenTelemetry Integration for Temporal Python SDK

This package provides OpenTelemetry tracing integration for Temporal workflows, activities, and other operations. It includes automatic span creation and propagation for distributed tracing across your Temporal applications.

## Overview

There are **two different approaches** for integrating OpenTelemetry with the Temporal Python SDK:

1. **🆕 New Approach (Recommended)**: `OpenTelemetryPlugin` - Provides accurate duration spans and direct OpenTelemetry usage within workflows
2. **📊 Legacy Approach**: `TracingInterceptor` - Provides immediate span visibility but with zero-duration workflow spans

## Quick Start

### New Approach (OpenTelemetryPlugin)

```python
import opentelemetry.trace
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from temporalio.contrib.opentelemetry import OpenTelemetryPlugin, create_tracer_provider

# Create a replay-safe tracer provider
provider = create_tracer_provider()
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
opentelemetry.trace.set_tracer_provider(provider)

# Register plugin on CLIENT (automatically applies to workers using this client)
client = await Client.connect(
    "localhost:7233",
    plugins=[OpenTelemetryPlugin()]
)

# Workers created with this client automatically get the plugin
worker = Worker(
    client,
    task_queue="my-task-queue",
    workflows=[MyWorkflow],
    activities=[my_activity]
    # NO NEED to specify plugins here - they come from the client
)
```

### Legacy Approach (TracingInterceptor)

```python
from temporalio.contrib.opentelemetry import TracingInterceptor

# Register interceptor on CLIENT (automatically applies to workers using this client)
client = await Client.connect(
    "localhost:7233",
    interceptors=[TracingInterceptor()]
)

# Workers created with this client automatically get the interceptor
worker = Worker(
    client,
    task_queue="my-task-queue", 
    workflows=[MyWorkflow],
    activities=[my_activity]
    # NO NEED to specify interceptors here - they come from the client
)
```

## Detailed Comparison

### New Approach: OpenTelemetryPlugin

#### ✅ Advantages:
- **Accurate Duration Spans**: Workflow spans have real durations reflecting actual execution time
- **Direct OpenTelemetry Usage**: Use `opentelemetry.trace.get_tracer()` directly within workflows
- **Better Span Hierarchy**: More accurate parent-child relationships within workflows
- **Workflow Context Access**: Access spans within workflows using `opentelemetry.trace.get_tracer()`

#### ⚠️ Considerations:
- **Experimental Status**: Subject to breaking changes in future versions
- **Delayed Span Visibility**: Workflow spans only appear after workflow completion
- **Different Trace Structure**: Migration from legacy approach may break dependencies on specific trace structures

#### Usage Example:
```python
@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self):
        # Direct OpenTelemetry usage works correctly
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span("workflow-operation"):
            # This span will have accurate duration
            await workflow.execute_activity(
                my_activity,
                start_to_close_timeout=timedelta(seconds=30)
            )
```

### Legacy Approach: TracingInterceptor

**File**: `temporalio/contrib/opentelemetry/_interceptor.py`

#### ✅ Advantages:
- **Immediate Span Visibility**: Spans appear as soon as they're created
- **Stable API**: Well-established interface, not subject to experimental changes
- **Workflow Progress Tracking**: Can see workflow spans even before workflow completes

#### ⚠️ Limitations:
- **Zero-Duration Workflow Spans**: All workflow spans are immediately ended with 0ms duration
- **No Direct OpenTelemetry Usage**: Cannot use standard OpenTelemetry APIs within workflows
- **Limited Workflow Span Creation**: Must use `temporalio.contrib.opentelemetry.workflow.completed_span()`

#### Usage Example:
```python
@workflow.defn  
class MyWorkflow:
    @workflow.run
    async def run(self):
        # Must use Temporal-specific span creation
        temporalio.contrib.opentelemetry.workflow.completed_span(
            "workflow-operation",
            attributes={"custom": "attribute"}
        )
        # Standard OpenTelemetry APIs don't work properly here
```

## When to Use Each Approach

### Choose OpenTelemetryPlugin When:
- You need accurate span durations for performance analysis
- You want to use standard OpenTelemetry APIs within workflows  
- You're building new applications
- You can tolerate experimental API changes

### Choose TracingInterceptor When:
- You need immediate visibility into workflow progress
- You have existing dependencies on the current trace structure
- You require a stable, non-experimental API
- You primarily need basic tracing without complex workflow span hierarchies

## Configuration Options

### OpenTelemetryPlugin Options

```python
plugin = OpenTelemetryPlugin(
    add_temporal_spans=False  # Whether to add additional Temporal-specific spans
)
```

### TracingInterceptor Options

```python
interceptor = TracingInterceptor(
    tracer=None,  # Custom tracer (defaults to global tracer)
    always_create_workflow_spans=False  # Create spans even without parent context
)
```

## Migration Guide

### From TracingInterceptor to OpenTelemetryPlugin

1. **Replace interceptor with plugin on client**:
   ```python
   # Old
   client = await Client.connect(
       "localhost:7233", 
       interceptors=[TracingInterceptor()]
   )
   
   # New
   provider = create_tracer_provider()
   opentelemetry.trace.set_tracer_provider(provider)
   client = await Client.connect(
       "localhost:7233",
       plugins=[OpenTelemetryPlugin()]
   )
   ```

2. **Update workflow span creation**:
   ```python
   # Old
   temporalio.contrib.opentelemetry.workflow.completed_span("my-span")
   
   # New - use standard OpenTelemetry
   tracer = get_tracer(__name__)
   with tracer.start_as_current_span("my-span"):
       # Your workflow logic
       pass
   ```

3. **Test trace structure changes**: Verify that any monitoring or analysis tools still work with the new trace structure.

## Advanced Usage

### Creating Custom Spans in Workflows (New Approach)

```python
from opentelemetry.trace import get_tracer

@workflow.defn
class MyWorkflow:
    @workflow.run  
    async def run(self):
        tracer = get_tracer(__name__)
        
        # Create spans with accurate durations
        with tracer.start_as_current_span("business-logic") as span:
            span.set_attribute("workflow.step", "processing")
            
            # Nested spans work correctly
            with tracer.start_as_current_span("data-validation"):
                await self.validate_input()
                
            await workflow.execute_activity(
                process_data,
                start_to_close_timeout=timedelta(seconds=60)
            )
```

### Custom Span Attributes

Both approaches support adding custom attributes to spans:

```python
# Legacy approach
temporalio.contrib.opentelemetry.workflow.completed_span(
    "my-operation",
    attributes={
        "business.unit": "payments",
        "request.id": "req-123"
    }
)

# New approach  
with tracer.start_as_current_span("my-operation") as span:
    span.set_attributes({
        "business.unit": "payments", 
        "request.id": "req-123"
    })
```

## Replay-Safe Metrics

For Temporal SDK metrics inside workflows, use `temporalio.workflow.metric_meter()`,
which is already replay-safe. However, third-party libraries (e.g. Google ADK) may
record OpenTelemetry metrics through the process-global meter provider from code
that runs inside workflows. Workflow code re-executes on every replay (cache
eviction, worker restart, redeploy), so a plain global meter provider re-records
those metrics on each replay, inflating counts.

`ReplaySafeMeterProvider` wraps your meter provider so synchronous instrument
recordings made from workflow code are dropped while the workflow is replaying
history events, mirroring what `create_tracer_provider()` does for spans:

```python
import opentelemetry.metrics
from opentelemetry.sdk.metrics import MeterProvider
from temporalio.contrib.opentelemetry import ReplaySafeMeterProvider

# set_meter_provider only takes effect once per process, so this wrapper must
# be the first and only global meter provider set, installed before any
# library records metrics.
opentelemetry.metrics.set_meter_provider(
    ReplaySafeMeterProvider(MeterProvider(metric_readers=[my_reader]))
)
```

Recordings are first-execution-only, matching `workflow.metric_meter()`: a
retried workflow task re-executes live and can record again. Queries and
update validators are live, once-per-request operations even when they run
while the workflow is replaying, so their recordings are kept. Observable
(asynchronous) instruments and recordings made outside workflows pass through
untouched.

## Replay-Safe Log Events

Libraries may also emit OpenTelemetry log records through the process-global
logger provider from workflow code (e.g. Google ADK's `gen_ai.*` events),
which duplicate on every replay the same way. `ReplaySafeLoggerProvider`
wraps your logger provider so records emitted from workflow code are dropped
while the workflow is replaying history events:

```python
import opentelemetry._logs
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from temporalio.contrib.opentelemetry import ReplaySafeLoggerProvider

# set_logger_provider only takes effect once per process, so this wrapper
# must be the first and only global logger provider set, installed before
# any library emits log records.
logger_provider = LoggerProvider()
logger_provider.add_log_record_processor(BatchLogRecordProcessor(my_log_exporter))
opentelemetry._logs.set_logger_provider(ReplaySafeLoggerProvider(logger_provider))
```

Emissions are first-execution-only: a retried workflow task re-executes live
and can emit again. Queries and update validators are live, once-per-request
operations even when they run while the workflow is replaying, so their
emissions are kept. Emissions outside workflows pass through untouched.

## Best Practices

1. **Register on Client**: Always register plugins/interceptors on the client, not the worker, to ensure proper context propagation

2. **Use create_tracer_provider()**: Always use the provided function to create replay-safe tracer providers when using the new approach

3. **Set Global Tracer Provider**: Ensure the tracer provider is set globally before creating clients

4. **Avoid Duplication**: Never register the same plugin/interceptor on both client and worker

## Troubleshooting

### Common Issues

1. **"ReplaySafeTracerProvider required" error**: Make sure you're using `create_tracer_provider()` when using OpenTelemetryPlugin

2. **Missing spans**: Verify that the tracer provider is set before creating clients, and that plugins/interceptors are registered on the client

3. **Duplicate spans**: Check that you haven't registered the same plugin/interceptor on both client and worker

4. **Zero-duration spans**: This is expected behavior with TracingInterceptor for workflow spans
