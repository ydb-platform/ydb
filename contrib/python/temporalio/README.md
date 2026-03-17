![Temporal Python SDK](https://assets.temporal.io/w/py-banner.svg)

[![Python 3.9+](https://img.shields.io/pypi/pyversions/temporalio.svg?style=for-the-badge)](https://pypi.org/project/temporalio)
[![PyPI](https://img.shields.io/pypi/v/temporalio.svg?style=for-the-badge)](https://pypi.org/project/temporalio)
[![MIT](https://img.shields.io/pypi/l/temporalio.svg?style=for-the-badge)](LICENSE)

**📣 News: Integration between OpenAI Agents SDK and Temporal is now in public preview. [Learn more](temporalio/contrib/openai_agents/README.md).**

[Temporal](https://temporal.io/) is a distributed, scalable, durable, and highly available orchestration engine used to
execute asynchronous, long-running business logic in a scalable and resilient way.

"Temporal Python SDK" is the framework for authoring workflows and activities using the Python programming language.

Also see:
* [Application Development Guide](https://docs.temporal.io/application-development?lang=python) - Once you've tried our
  [Quick Start](#quick-start), check out our guide on how to use Temporal in your Python applications, including
  information around Temporal core concepts.
* [Python Code Samples](https://github.com/temporalio/samples-python)
* [API Documentation](https://python.temporal.io) - Complete Temporal Python SDK Package reference.

In addition to features common across all Temporal SDKs, the Python SDK also has the following interesting features:

**Type Safe**

This library uses the latest typing and MyPy support with generics to ensure all calls can be typed. For example,
starting a workflow with an `int` parameter when it accepts a `str` parameter would cause MyPy to fail.

**Different Activity Types**

The activity worker has been developed to work with `async def`, threaded, and multiprocess activities. Threaded activities are the initial recommendation, and further guidance can be found in [the docs](https://docs.temporal.io/develop/python/python-sdk-sync-vs-async).

**Custom `asyncio` Event Loop**

The workflow implementation basically turns `async def` functions into workflows backed by a distributed, fault-tolerant
event loop. This means task management, sleep, cancellation, etc have all been developed to seamlessly integrate with
`asyncio` concepts.

See the [blog post](https://temporal.io/blog/durable-distributed-asyncio-event-loop) introducing the Python SDK for an
informal introduction to the features and their implementation.

---

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Contents**

- [Quick Start](#quick-start)
  - [Installation](#installation)
  - [Implementing a Workflow](#implementing-a-workflow)
  - [Running a Workflow](#running-a-workflow)
  - [Next Steps](#next-steps)
- [Usage](#usage)
    - [Client](#client)
      - [Data Conversion](#data-conversion)
        - [Pydantic Support](#pydantic-support)
        - [Custom Type Data Conversion](#custom-type-data-conversion)
    - [Workers](#workers)
    - [Workflows](#workflows)
      - [Definition](#definition)
      - [Running](#running)
      - [Invoking Activities](#invoking-activities)
      - [Invoking Child Workflows](#invoking-child-workflows)
      - [Timers](#timers)
      - [Conditions](#conditions)
      - [Asyncio and Determinism](#asyncio-and-determinism)
      - [Asyncio Cancellation](#asyncio-cancellation)
      - [Workflow Utilities](#workflow-utilities)
      - [Exceptions](#exceptions)
      - [Signal and update handlers](#signal-and-update-handlers)
      - [External Workflows](#external-workflows)
      - [Testing](#testing)
        - [Automatic Time Skipping](#automatic-time-skipping)
        - [Manual Time Skipping](#manual-time-skipping)
        - [Mocking Activities](#mocking-activities)
      - [Workflow Sandbox](#workflow-sandbox)
        - [How the Sandbox Works](#how-the-sandbox-works)
        - [Avoiding the Sandbox](#avoiding-the-sandbox)
        - [Customizing the Sandbox](#customizing-the-sandbox)
          - [Passthrough Modules](#passthrough-modules)
          - [Invalid Module Members](#invalid-module-members)
        - [Known Sandbox Issues](#known-sandbox-issues)
          - [Global Import/Builtins](#global-importbuiltins)
          - [Sandbox is not Secure](#sandbox-is-not-secure)
          - [Sandbox Performance](#sandbox-performance)
          - [Extending Restricted Classes](#extending-restricted-classes)
          - [Certain Standard Library Calls on Restricted Objects](#certain-standard-library-calls-on-restricted-objects)
          - [is_subclass of ABC-based Restricted Classes](#is_subclass-of-abc-based-restricted-classes)
    - [Activities](#activities)
      - [Definition](#definition-1)
      - [Types of Activities](#types-of-activities)
        - [Synchronous Activities](#synchronous-activities)
          - [Synchronous Multithreaded Activities](#synchronous-multithreaded-activities)
          - [Synchronous Multiprocess/Other Activities](#synchronous-multiprocessother-activities)
        - [Asynchronous Activities](#asynchronous-activities)
      - [Activity Context](#activity-context)
        - [Heartbeating and Cancellation](#heartbeating-and-cancellation)
        - [Worker Shutdown](#worker-shutdown)
      - [Testing](#testing-1)
    - [Interceptors](#interceptors)
    - [Nexus](#nexus)
    - [Plugins](#plugins)
      - [Client Plugins](#client-plugins)
      - [Worker Plugins](#worker-plugins)
    - [Workflow Replay](#workflow-replay)
    - [Observability](#observability)
      - [Metrics](#metrics)
      - [OpenTelemetry Tracing](#opentelemetry-tracing)
    - [Protobuf 3.x vs 4.x](#protobuf-3x-vs-4x)
    - [Known Compatibility Issues](#known-compatibility-issues)
      - [gevent Patching](#gevent-patching)
- [Development](#development)
    - [Building](#building)
      - [Prepare](#prepare)
      - [Build](#build)
      - [Use](#use)
    - [Local SDK development environment](#local-sdk-development-environment)
      - [Testing](#testing-2)
      - [Proto Generation and Testing](#proto-generation-and-testing)
    - [Style](#style)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Quick Start

We will guide you through the Temporal basics to create a "hello, world!" script on your machine. It is not intended as
one of the ways to use Temporal, but in reality it is very simplified and decidedly not "the only way" to use Temporal.
For more information, check out the docs references in "Next Steps" below the quick start.

## Installation

Install the `temporalio` package from [PyPI](https://pypi.org/project/temporalio).

These steps can be followed to use with a virtual environment and `pip`:

* [Create a virtual environment](https://packaging.python.org/en/latest/tutorials/installing-packages/#creating-virtual-environments)
* Update `pip` - `python -m pip install -U pip`
  * Needed because older versions of `pip` may not pick the right wheel
* Install Temporal SDK - `python -m pip install temporalio`

The SDK is now ready for use. To build from source, see "Building" near the end of this documentation.

**NOTE: This README is for the current branch and not necessarily what's released on `PyPI`.**

## Implementing a Workflow

Create the following in `activities.py`:

```python
from temporalio import activity

@activity.defn
def say_hello(name: str) -> str:
    return f"Hello, {name}!"
```

Create the following in `workflows.py`:

```python
from datetime import timedelta
from temporalio import workflow

# Import our activity, passing it through the sandbox
with workflow.unsafe.imports_passed_through():
    from .activities import say_hello

@workflow.defn
class SayHello:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            say_hello, name, schedule_to_close_timeout=timedelta(seconds=5)
        )
```

Create the following in `run_worker.py`:

```python
import asyncio
import concurrent.futures
from temporalio.client import Client
from temporalio.worker import Worker

# Import the activity and workflow from our other files
from .activities import say_hello
from .workflows import SayHello

async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Run the worker
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as activity_executor:
        worker = Worker(
          client,
          task_queue="my-task-queue",
          workflows=[SayHello],
          activities=[say_hello],
          activity_executor=activity_executor,
        )
        await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

Assuming you have a [Temporal server running on localhost](https://docs.temporal.io/docs/server/quick-install/), this
will run the worker:

    python run_worker.py

## Running a Workflow

Create the following script at `run_workflow.py`:

```python
import asyncio
from temporalio.client import Client

# Import the workflow from the previous code
from .workflows import SayHello

async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Execute a workflow
    result = await client.execute_workflow(SayHello.run, "my name", id="my-workflow-id", task_queue="my-task-queue")

    print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

Assuming you have `run_worker.py` running from before, this will run the workflow:

    python run_workflow.py

The output will be:

    Result: Hello, my-name!

## Next Steps

Temporal can be implemented in your code in many different ways, to suit your application's needs. The links below will
give you much more information about how Temporal works with Python:

* [Code Samples](https://github.com/temporalio/samples-python) - If you want to start with some code, we have provided
  some pre-built samples.
* [Application Development Guide](https://docs.temporal.io/application-development?lang=python) Our Python specific
  Developer's Guide will give you much more information on how to build with Temporal in your Python applications than
  our SDK README ever could (or should).
* [API Documentation](https://python.temporal.io) - Full Temporal Python SDK package documentation.

---

# Usage

From here, you will find reference documentation about specific pieces of the Temporal Python SDK that were built around
Temporal concepts. *This section is not intended as a how-to guide* -- For more how-to oriented information, check out
the links in the [Next Steps](#next-steps) section above.

### Client

A client can be created and used to start a workflow like so:

```python
from temporalio.client import Client

async def main():
    # Create client connected to server at the given address and namespace
    client = await Client.connect("localhost:7233", namespace="my-namespace")

    # Start a workflow
    handle = await client.start_workflow(MyWorkflow.run, "some arg", id="my-workflow-id", task_queue="my-task-queue")

    # Wait for result
    result = await handle.result()
    print(f"Result: {result}")
```

Some things to note about the above code:

* A `Client` does not have an explicit "close"
* To enable TLS, the `tls` argument to `connect` can be set to `True` or a `TLSConfig` object
* A single positional argument can be passed to `start_workflow`. If there are multiple arguments, only the
  non-type-safe form of `start_workflow` can be used (i.e. the one accepting a string workflow name) and it must be in
  the `args` keyword argument.
* The `handle` represents the workflow that was started and can be used for more than just getting the result
* Since we are just getting the handle and waiting on the result, we could have called `client.execute_workflow` which
  does the same thing
* Clients can have many more options not shown here (e.g. data converters and interceptors)
* A string can be used instead of the method reference to call a workflow by name (e.g. if defined in another language)
* Clients do not work across forks

Clients also provide a shallow copy of their config for use in making slightly different clients backed by the same
connection. For instance, given the `client` above, this is how to have a client in another namespace:

```python
config = client.config()
config["namespace"] = "my-other-namespace"
other_ns_client = Client(**config)
```

#### Data Conversion

Data converters are used to convert raw Temporal payloads to/from actual Python types. A custom data converter of type
`temporalio.converter.DataConverter` can be set via the `data_converter` parameter of the `Client` constructor. Data
converters are a combination of payload converters, payload codecs, and failure converters. Payload converters convert
Python values to/from serialized bytes. Payload codecs convert bytes to bytes (e.g. for compression or encryption).
Failure converters convert exceptions to/from serialized failures.

The default data converter supports converting multiple types including:

* `None`
* `bytes`
* `google.protobuf.message.Message` - As JSON when encoding, but has ability to decode binary proto from other languages
* Anything that can be converted to JSON including:
  * Anything that [`json.dump`](https://docs.python.org/3/library/json.html#json.dump) supports natively
  * [dataclasses](https://docs.python.org/3/library/dataclasses.html)
  * Iterables including ones JSON dump may not support by default, e.g. `set`
  * [IntEnum, StrEnum](https://docs.python.org/3/library/enum.html) based enumerates
  * [UUID](https://docs.python.org/3/library/uuid.html)
  * `datetime.datetime`

To use pydantic model instances, see [Pydantic Support](#pydantic-support).

`datetime.date` and `datetime.time` can only be used with the Pydantic data converter.

Although workflows, updates, signals, and queries can all be defined with multiple input parameters, users are strongly
encouraged to use a single `dataclass` or Pydantic model parameter, so that fields with defaults can be easily added
without breaking compatibility. Similar advice applies to return values.

Classes with generics may not have the generics properly resolved. The current implementation does not have generic
type resolution. Users should use concrete types.

##### Pydantic Support

To use Pydantic model instances, install Pydantic and set the Pydantic data converter when creating client instances:

```python
from temporalio.contrib.pydantic import pydantic_data_converter

client = Client(data_converter=pydantic_data_converter, ...)
```

This data converter supports conversion of all types supported by Pydantic to and from JSON.

In addition to Pydantic models, these include all `json.dump`-able types, various non-`json.dump`-able standard library
types such as dataclasses, types from the datetime module, sets, UUID, etc, and custom types composed of any of these.

Pydantic v1 is not supported by this data converter. If you are not yet able to upgrade from Pydantic v1, see
https://github.com/temporalio/samples-python/tree/main/pydantic_converter/v1 for limited v1 support.


##### Custom Type Data Conversion

For converting from JSON, the workflow/activity type hint is taken into account to convert to the proper type. Care has
been taken to support all common typings including `Optional`, `Union`, all forms of iterables and mappings, `NewType`,
etc in addition to the regular JSON values mentioned before.

Data converters contain a reference to a payload converter class that is used to convert to/from payloads/values. This
is a class and not an instance because it is instantiated on every workflow run inside the sandbox. The payload
converter is usually a `CompositePayloadConverter` which contains a multiple `EncodingPayloadConverter`s it uses to try
to serialize/deserialize payloads. Upon serialization, each `EncodingPayloadConverter` is tried until one succeeds. The
`EncodingPayloadConverter` provides an "encoding" string serialized onto the payload so that, upon deserialization, the
specific `EncodingPayloadConverter` for the given "encoding" is used.

The default data converter uses the `DefaultPayloadConverter` which is simply a `CompositePayloadConverter` with a known
set of default `EncodingPayloadConverter`s. To implement a custom encoding for a custom type, a new
`EncodingPayloadConverter` can be created for the new type. For example, to support `IPv4Address` types:

```python
class IPv4AddressEncodingPayloadConverter(EncodingPayloadConverter):
    @property
    def encoding(self) -> str:
        return "text/ipv4-address"

    def to_payload(self, value: Any) -> Optional[Payload]:
        if isinstance(value, ipaddress.IPv4Address):
            return Payload(
                metadata={"encoding": self.encoding.encode()},
                data=str(value).encode(),
            )
        else:
            return None

    def from_payload(self, payload: Payload, type_hint: Optional[Type] = None) -> Any:
        assert not type_hint or type_hint is ipaddress.IPv4Address
        return ipaddress.IPv4Address(payload.data.decode())

class IPv4AddressPayloadConverter(CompositePayloadConverter):
    def __init__(self) -> None:
        # Just add ours as first before the defaults
        super().__init__(
            IPv4AddressEncodingPayloadConverter(),
            *DefaultPayloadConverter.default_encoding_payload_converters,
        )

my_data_converter = dataclasses.replace(
    DataConverter.default,
    payload_converter_class=IPv4AddressPayloadConverter,
)
```

Imports are left off for brevity.

This is good for many custom types. However, sometimes you want to override the behavior of the just the existing JSON
encoding payload converter to support a new type. It is already the last encoding data converter in the list, so it's
the fall-through behavior for any otherwise unknown type. Customizing the existing JSON converter has the benefit of
making the type work in lists, unions, etc.

The `JSONPlainPayloadConverter` uses the Python [json](https://docs.python.org/3/library/json.html) library with an
advanced JSON encoder by default and a custom value conversion method to turn `json.load`ed values to their type hints.
The conversion can be customized for serialization with a custom `json.JSONEncoder` and deserialization with a custom
`JSONTypeConverter`. For example, to support `IPv4Address` types in existing JSON conversion:

```python
class IPv4AddressJSONEncoder(AdvancedJSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, ipaddress.IPv4Address):
            return str(o)
        return super().default(o)
class IPv4AddressJSONTypeConverter(JSONTypeConverter):
    def to_typed_value(
        self, hint: Type, value: Any
    ) -> Union[Optional[Any], _JSONTypeConverterUnhandled]:
        if issubclass(hint, ipaddress.IPv4Address):
            return ipaddress.IPv4Address(value)
        return JSONTypeConverter.Unhandled

class IPv4AddressPayloadConverter(CompositePayloadConverter):
    def __init__(self) -> None:
        # Replace default JSON plain with our own that has our encoder and type
        # converter
        json_converter = JSONPlainPayloadConverter(
            encoder=IPv4AddressJSONEncoder,
            custom_type_converters=[IPv4AddressJSONTypeConverter()],
        )
        super().__init__(
            *[
                c if not isinstance(c, JSONPlainPayloadConverter) else json_converter
                for c in DefaultPayloadConverter.default_encoding_payload_converters
            ]
        )

my_data_converter = dataclasses.replace(
    DataConverter.default,
    payload_converter_class=IPv4AddressPayloadConverter,
)
```

Now `IPv4Address` can be used in type hints including collections, optionals, etc.

### Workers

Workers host workflows and/or activities. Here's how to run a worker:

```python
import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker
# Import your own workflows and activities
from my_workflow_package import MyWorkflow, my_activity

async def run_worker(stop_event: asyncio.Event):
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233", namespace="my-namespace")

    # Run the worker until the event is set
    worker = Worker(client, task_queue="my-task-queue", workflows=[MyWorkflow], activities=[my_activity])
    async with worker:
        await stop_event.wait()
```

Some things to note about the above code:

* This creates/uses the same client that is used for starting workflows
* While this example accepts a stop event and uses `async with`, `run()` and `shutdown()` may be used instead
* Workers can have many more options not shown here (e.g. data converters and interceptors)

### Workflows

#### Definition

Workflows are defined as classes decorated with `@workflow.defn`. The method invoked for the workflow is decorated with
`@workflow.run`. Methods for signals, queries, and updates are decorated with `@workflow.signal`, `@workflow.query`
and `@workflow.update` respectively. Here's an example of a workflow:

```python
import asyncio
from datetime import timedelta
from temporalio import workflow

# Pass the activities through the sandbox
with workflow.unsafe.imports_passed_through():
    from .my_activities import GreetingInfo, create_greeting_activity

@workflow.defn
class GreetingWorkflow:
    def __init__(self) -> None:
        self._current_greeting = "<unset>"
        self._greeting_info = GreetingInfo()
        self._greeting_info_update = asyncio.Event()
        self._complete = asyncio.Event()

    @workflow.run
    async def run(self, name: str) -> str:
        self._greeting_info.name = name
        while True:
            # Store greeting
            self._current_greeting = await workflow.execute_activity(
                create_greeting_activity,
                self._greeting_info,
                start_to_close_timeout=timedelta(seconds=5),
            )
            workflow.logger.debug("Greeting set to %s", self._current_greeting)

            # Wait for salutation update or complete signal (this can be
            # cancelled)
            await asyncio.wait(
                [
                    asyncio.create_task(self._greeting_info_update.wait()),
                    asyncio.create_task(self._complete.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._complete.is_set():
                return self._current_greeting
            self._greeting_info_update.clear()

    @workflow.signal
    async def update_salutation(self, salutation: str) -> None:
        self._greeting_info.salutation = salutation
        self._greeting_info_update.set()

    @workflow.signal
    async def complete_with_greeting(self) -> None:
        self._complete.set()

    @workflow.query
    def current_greeting(self) -> str:
        return self._current_greeting

    @workflow.update
    def set_and_get_greeting(self, greeting: str) -> str:
      old = self._current_greeting
      self._current_greeting = greeting
      return old

```

This assumes there's an activity in `my_activities.py` like:

```python
from dataclasses import dataclass
from temporalio import workflow

@dataclass
class GreetingInfo:
    salutation: str = "Hello"
    name: str = "<unknown>"

@activity.defn
def create_greeting_activity(info: GreetingInfo) -> str:
    return f"{info.salutation}, {info.name}!"
```

Some things to note about the above workflow code:

* Workflows run in a sandbox by default.
  * Users are encouraged to define workflows in files with no side effects or other complicated code or unnecessary
    imports to other third party libraries.
  * Non-standard-library, non-`temporalio` imports should usually be "passed through" the sandbox. See the
    [Workflow Sandbox](#workflow-sandbox) section for more details.
* This workflow continually updates the queryable current greeting when signalled and can complete with the greeting on
  a different signal
* Workflows are always classes and must have a single `@workflow.run` which is an `async def` function
* Workflow code must be deterministic. This means no `set` iteration, threading, no randomness, no external calls to
  processes, no network IO, and no global state mutation. All code must run in the implicit `asyncio` event loop and be
  deterministic. Also see the [Asyncio and Determinism](#asyncio-and-determinism) section later.
* `@activity.defn` is explained in a later section. For normal simple string concatenation, this would just be done in
  the workflow. The activity is for demonstration purposes only.
* `workflow.execute_activity(create_greeting_activity, ...` is actually a typed signature, and MyPy will fail if the
  `self._greeting_info` parameter is not a `GreetingInfo`

Here are the decorators that can be applied:

* `@workflow.defn` - Defines a workflow class
  * Must be defined on the class given to the worker (ignored if present on a base class)
  * Can have a `name` param to customize the workflow name, otherwise it defaults to the unqualified class name
  * Can have `dynamic=True` which means all otherwise unhandled workflows fall through to this. If present, cannot have
    `name` argument, and run method must accept a single parameter of `Sequence[temporalio.common.RawValue]` type. The
    payload of the raw value can be converted via `workflow.payload_converter().from_payload`.
* `@workflow.run` - Defines the primary workflow run method
  * Must be defined on the same class as `@workflow.defn`, not a base class (but can _also_ be defined on the same
    method of a base class)
  * Exactly one method name must have this decorator, no more or less
  * Must be defined on an `async def` method
  * The method's arguments are the workflow's arguments
  * The first parameter must be `self`, followed by positional arguments. Best practice is to only take a single
    argument that is an object/dataclass of fields that can be added to as needed.
* `@workflow.init` - Specifies that the `__init__`  method accepts the workflow's arguments.
  * If present, may only be applied to the `__init__` method, the parameters of which must then be identical to those of
    the `@workflow.run` method.
  * The purpose of this decorator is to allow operations involving workflow arguments to be performed in the `__init__`
    method, before any signal or update handler has a chance to execute.
* `@workflow.signal` - Defines a method as a signal
  * Can be defined on an `async` or non-`async` method at any point in the class hierarchy, but if the decorated method
    is overridden, then the override must also be decorated.
  * The method's arguments are the signal's arguments.
  * Return value is ignored.
  * May mutate workflow state, and make calls to other workflow APIs like starting activities, etc.
  * Can have a `name` param to customize the signal name, otherwise it defaults to the unqualified method name.
  * Can have `dynamic=True` which means all otherwise unhandled signals fall through to this. If present, cannot have
    `name` argument, and method parameters must be `self`, a string signal name, and a
    `Sequence[temporalio.common.RawValue]`.
  * Non-dynamic method can only have positional arguments. Best practice is to only take a single argument that is an
    object/dataclass of fields that can be added to as needed.
  * See [Signal and update handlers](#signal-and-update-handlers) below
* `@workflow.update` - Defines a method as an update
  * Can be defined on an `async` or non-`async` method at any point in the class hierarchy, but if the decorated method
    is overridden, then the override must also be decorated.
  * May accept input and return a value
  * The method's arguments are the update's arguments.
  * May be `async` or non-`async`
  * May mutate workflow state, and make calls to other workflow APIs like starting activities, etc.
  * Also accepts the `name` and `dynamic` parameters like signal, with the same semantics.
  * Update handlers may optionally define a validator method by decorating it with `@update_handler_method.validator`.
    To reject an update before any events are written to history, throw an exception in a validator. Validators cannot
    be `async`, cannot mutate workflow state, and return nothing.
  * See [Signal and update handlers](#signal-and-update-handlers) below
* `@workflow.query` - Defines a method as a query
  * Should return a value
  * Should not be `async`
  * Temporal queries should never mutate anything in the workflow or call any calls that would mutate the workflow
  * Also accepts the `name` and `dynamic` parameters like signal and update, with the same semantics.

#### Running

To start a locally-defined workflow from a client, you can simply reference its method like so:

```python
from temporalio.client import Client
from my_workflow_package import GreetingWorkflow

async def create_greeting(client: Client) -> str:
    # Start the workflow
    handle = await client.start_workflow(GreetingWorkflow.run, "my name", id="my-workflow-id", task_queue="my-task-queue")
    # Change the salutation
    await handle.signal(GreetingWorkflow.update_salutation, "Aloha")
    # Tell it to complete
    await handle.signal(GreetingWorkflow.complete_with_greeting)
    # Wait and return result
    return await handle.result()
```

Some things to note about the above code:

* This uses the `GreetingWorkflow` from the previous section
* The result of calling this function is `"Aloha, my name!"`
* `id` and `task_queue` are required for running a workflow
* `client.start_workflow` is typed, so MyPy would fail if `"my name"` were something besides a string
* `handle.signal` is typed, so MyPy would fail if `"Aloha"` were something besides a string or if we provided a
  parameter to the parameterless `complete_with_greeting`
* `handle.result` is typed to the workflow itself, so MyPy would fail if we said this `create_greeting` returned
  something besides a string

#### Invoking Activities

* Activities are started with non-async `workflow.start_activity()` which accepts either an activity function reference
  or a string name.
* A single argument to the activity is positional. Multiple arguments are not supported in the type-safe form of
  start/execute activity and must be supplied via the `args` keyword argument.
* Activity options are set as keyword arguments after the activity arguments. At least one of `start_to_close_timeout`
  or `schedule_to_close_timeout` must be provided.
* The result is an activity handle which is an `asyncio.Task` and supports basic task features
* An async `workflow.execute_activity()` helper is provided which takes the same arguments as
  `workflow.start_activity()` and `await`s on the result. This should be used in most cases unless advanced task
  capabilities are needed.
* Local activities work very similarly except the functions are `workflow.start_local_activity()` and
  `workflow.execute_local_activity()`
  * ⚠️Local activities are currently experimental
* Activities can be methods of a class. Invokers should use `workflow.start_activity_method()`,
  `workflow.execute_activity_method()`, `workflow.start_local_activity_method()`, and
  `workflow.execute_local_activity_method()` instead.
* Activities can callable classes (i.e. that define `__call__`). Invokers should use `workflow.start_activity_class()`,
  `workflow.execute_activity_class()`, `workflow.start_local_activity_class()`, and
  `workflow.execute_local_activity_class()` instead.

#### Invoking Child Workflows

* Child workflows are started with async `workflow.start_child_workflow()` which accepts either a workflow run method
  reference or a string name. The arguments to the workflow are positional.
* A single argument to the child workflow is positional. Multiple arguments are not supported in the type-safe form of
  start/execute child workflow and must be supplied via the `args` keyword argument.
* Child workflow options are set as keyword arguments after the arguments. At least `id` must be provided.
* The `await` of the start does not complete until the start has been accepted by the server
* The result is a child workflow handle which is an `asyncio.Task` and supports basic task features. The handle also has
  some child info and supports signalling the child workflow
* An async `workflow.execute_child_workflow()` helper is provided which takes the same arguments as
  `workflow.start_child_workflow()` and `await`s on the result. This should be used in most cases unless advanced task
  capabilities are needed.

#### Timers

* A timer is represented by normal `asyncio.sleep()` or a `workflow.sleep()` call
* Timers are also implicitly started on any `asyncio` calls with timeouts (e.g. `asyncio.wait_for`)
* Timers are Temporal server timers, not local ones, so sub-second resolution rarely has value
* Calls that use a specific point in time, e.g. `call_at` or `timeout_at`, should be based on the current loop time
  (i.e. `workflow.time()`) and not an actual point in time. This is because fixed times are translated to relative ones
  by subtracting the current loop time which may not be the actual current time.

#### Conditions

* `workflow.wait_condition` is an async function that doesn't return until a provided callback returns true
* A `timeout` can optionally be provided which will throw a `asyncio.TimeoutError` if reached (internally backed by
  `asyncio.wait_for` which uses a timer)

#### Asyncio and Determinism

Workflows must be deterministic. Workflows are backed by a custom
[asyncio](https://docs.python.org/3/library/asyncio.html) event loop. This means many of the common `asyncio` calls work
as normal. Some asyncio features are disabled such as:

* Thread related calls such as `to_thread()`, `run_coroutine_threadsafe()`, `loop.run_in_executor()`, etc
* Calls that alter the event loop such as `loop.close()`, `loop.stop()`, `loop.run_forever()`,
  `loop.set_task_factory()`, etc
* Calls that use anything external such as networking, subprocesses, disk IO, etc

Also, there are some `asyncio` utilities that internally use `set()` which can make them non-deterministic from one
worker to the next. Therefore the following `asyncio` functions have `workflow`-module alternatives that are
deterministic:

* `asyncio.as_completed()` - use `workflow.as_completed()`
* `asyncio.wait()` - use `workflow.wait()`

#### Asyncio Cancellation

Cancellation is done using `asyncio` [task cancellation](https://docs.python.org/3/library/asyncio-task.html#task-cancellation).
This means that tasks are requested to be cancelled but can catch the
[`asyncio.CancelledError`](https://docs.python.org/3/library/asyncio-exceptions.html#asyncio.CancelledError), thus
allowing them to perform some cleanup before allowing the cancellation to proceed (i.e. re-raising the error), or to
deny the cancellation entirely. It also means that
[`asyncio.shield()`](https://docs.python.org/3/library/asyncio-task.html#shielding-from-cancellation) can be used to
protect tasks against cancellation.

The following tasks, when cancelled, perform a Temporal cancellation:

* Activities - when the task executing an activity is cancelled, a cancellation request is sent to the activity
* Child workflows - when the task starting or executing a child workflow is cancelled, a cancellation request is sent to
  cancel the child workflow
* Timers - when the task executing a timer is cancelled (whether started via sleep or timeout), the timer is cancelled

When the workflow itself is requested to cancel, `Task.cancel` is called on the main workflow task. Therefore,
`asyncio.CancelledError` can be caught in order to handle the cancel gracefully.

Workflows follow `asyncio` cancellation rules exactly which can cause confusion among Python developers. Cancelling a
task doesn't always cancel the thing it created. For example, given
`task = asyncio.create_task(workflow.start_child_workflow(...`, calling `task.cancel` does not cancel the child
workflow, it only cancels the starting of it, which has no effect if it has already started. However, cancelling the
result of `handle = await workflow.start_child_workflow(...` or
`task = asyncio.create_task(workflow.execute_child_workflow(...` _does_ cancel the child workflow.

Also, due to Temporal rules, a cancellation request is a state not an event. Therefore, repeated cancellation requests
are not delivered, only the first. If the workflow chooses swallow a cancellation, it cannot be requested again.

#### Workflow Utilities

While running in a workflow, in addition to features documented elsewhere, the following items are available from the
`temporalio.workflow` package:

* `continue_as_new()` - Async function to stop the workflow immediately and continue as new
* `info()` - Returns information about the current workflow
* `logger` - A logger for use in a workflow (properly skips logging on replay)
* `now()` - Returns the "current time" from the workflow's perspective

#### Exceptions

* Workflows/updates can raise exceptions to fail the workflow or the "workflow task" (i.e. suspend the workflow
  in a retrying state).
* Exceptions that are instances of `temporalio.exceptions.FailureError` will fail the workflow with that exception
  * For failing the workflow explicitly with a user exception, use `temporalio.exceptions.ApplicationError`. This can
    be marked non-retryable or include details as needed.
  * Other exceptions that come from activity execution, child execution, cancellation, etc are already instances of
    `FailureError` and will fail the workflow when uncaught.
* Update handlers are special: an instance of `temporalio.exceptions.FailureError` raised in an update handler will fail
  the update instead of failing the workflow.
* All other exceptions fail the "workflow task" which means the workflow will continually retry until the workflow is
  fixed. This is helpful for bad code or other non-predictable exceptions. To actually fail the workflow, use an
  `ApplicationError` as mentioned above.

This default can be changed by providing a list of exception types to `workflow_failure_exception_types` when creating a
`Worker` or `failure_exception_types` on the `@workflow.defn` decorator. If a workflow-thrown exception is an instance
of any type in either list, it will fail the workflow (or update) instead of the workflow task. This means a value of
`[Exception]` will cause every exception to fail the workflow instead of the workflow task. Also, as a special case, if
`temporalio.workflow.NondeterminismError` (or any superclass of it) is set, non-deterministic exceptions will fail the
workflow. WARNING: These settings are experimental.

#### Signal and update handlers

Signal and update handlers are defined using decorated methods as shown in the example [above](#definition). Client code
sends signals and updates using `workflow_handle.signal`, `workflow_handle.execute_update`, or
`workflow_handle.start_update`. When the workflow receives one of these requests, it starts an `asyncio.Task` executing
the corresponding handler method with the argument(s) from the request.

The handler methods may be `async def` and can do all the async operations described above (e.g. invoking activities and
child workflows, and waiting on timers and conditions). Notice that this means that handler tasks will be executing
concurrently with respect to each other and the main workflow task. Use
[asyncio.Lock](https://docs.python.org/3/library/asyncio-sync.html#lock) and
[asyncio.Semaphore](https://docs.python.org/3/library/asyncio-sync.html#semaphore) if necessary.

Your main workflow task may finish as a result of successful completion, cancellation, continue-as-new, or failure. You
should ensure that all in-progress signal and update handler tasks have finished before this happens; if you do not, you
will see a warning (the warning can be disabled via the `workflow.signal`/`workflow.update` decorators). One way to
ensure that handler tasks have finished is to wait on the `workflow.all_handlers_finished` condition:
```python
await workflow.wait_condition(workflow.all_handlers_finished)
```
#### External Workflows

* `workflow.get_external_workflow_handle()` inside a workflow returns a handle to interact with another workflow
* `workflow.get_external_workflow_handle_for()` can be used instead for a type safe handle
* `await handle.signal()` can be called on the handle to signal the external workflow
* `await handle.cancel()` can be called on the handle to send a cancel to the external workflow

#### Testing

Workflow testing can be done in an integration-test fashion against a real server, however it is hard to simulate
timeouts and other long time-based code. Using the time-skipping workflow test environment can help there.

The time-skipping `temporalio.testing.WorkflowEnvironment` can be created via the static async `start_time_skipping()`.
This internally downloads the Temporal time-skipping test server to a temporary directory if it doesn't already exist,
then starts the test server which has special APIs for skipping time.

**NOTE:** The time-skipping test environment does not work on ARM. The SDK will try to download the x64 binary on macOS
for use with the Intel emulator, but for Linux or Windows ARM there is no proper time-skipping test server at this time.

##### Automatic Time Skipping

Anytime a workflow result is waited on, the time-skipping server automatically advances to the next event it can. To
manually advance time before waiting on the result of a workflow, the `WorkflowEnvironment.sleep` method can be used.

Here's a simple example of a workflow that sleeps for 24 hours:

```python
import asyncio
from temporalio import workflow

@workflow.defn
class WaitADayWorkflow:
    @workflow.run
    async def run(self) -> str:
        await asyncio.sleep(24 * 60 * 60)
        return "all done"
```

An integration test of this workflow would be way too slow. However the time-skipping server automatically skips to the
next event when we wait on the result. Here's a test for that workflow:

```python
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

async def test_wait_a_day_workflow():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="tq1", workflows=[WaitADayWorkflow]):
            assert "all done" == await env.client.execute_workflow(WaitADayWorkflow.run, id="wf1", task_queue="tq1")
```

That test will run almost instantly. This is because by calling `execute_workflow` on our client, we have asked the
environment to automatically skip time as much as it can (basically until the end of the workflow or until an activity
is run).

To disable automatic time-skipping while waiting for a workflow result, run code inside a
`with env.auto_time_skipping_disabled():` block.

##### Manual Time Skipping

Until a workflow is waited on, all time skipping in the time-skipping environment is done manually via
`WorkflowEnvironment.sleep`.

Here's workflow that waits for a signal or times out:

```python
import asyncio
from temporalio import workflow

@workflow.defn
class SignalWorkflow:
    def __init__(self) -> None:
        self.signal_received = False

    @workflow.run
    async def run(self) -> str:
        # Wait for signal or timeout in 45 seconds
        try:
            await workflow.wait_condition(lambda: self.signal_received, timeout=45)
            return "got signal"
        except asyncio.TimeoutError:
            return "got timeout"

    @workflow.signal
    def some_signal(self) -> None:
        self.signal_received = True
```

To test a normal signal, you might:

```python
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

async def test_signal_workflow():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="tq1", workflows=[SignalWorkflow]):
            # Start workflow, send signal, check result
            handle = await env.client.start_workflow(SignalWorkflow.run, id="wf1", task_queue="tq1")
            await handle.signal(SignalWorkflow.some_signal)
            assert "got signal" == await handle.result()
```

But how would you test the timeout part? Like so:

```python
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

async def test_signal_workflow_timeout():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="tq1", workflows=[SignalWorkflow]):
            # Start workflow, advance time past timeout, check result
            handle = await env.client.start_workflow(SignalWorkflow.run, id="wf1", task_queue="tq1")
            await env.sleep(50)
            assert "got timeout" == await handle.result()
```

Also, the current time of the workflow environment can be obtained via the async `WorkflowEnvironment.get_current_time`
method.

##### Mocking Activities

Activities are just functions decorated with `@activity.defn`. Simply write different ones and pass those to the worker
to have different activities called during the test.

#### Workflow Sandbox

By default workflows are run in a sandbox to help avoid non-deterministic code. If a call that is known to be
non-deterministic is performed, an exception will be thrown in the workflow which will "fail the task" which means the
workflow will not progress until fixed.

The sandbox is not foolproof and non-determinism can still occur. It is simply a best-effort way to catch bad code
early. Users are encouraged to define their workflows in files with no other side effects.

The sandbox offers a mechanism to "pass through" modules from outside the sandbox. By default this already includes all
standard library modules and Temporal modules. **For performance and behavior reasons, users are encouraged to pass
through all modules whose calls will be deterministic.** In particular, this advice extends to modules containing the
activities to be referenced in workflows, and modules containing dataclasses and Pydantic models, which can be
particularly expensive to import. See "Passthrough Modules" below on how to do this.


##### How the Sandbox Works

The sandbox is made up of two components that work closely together:

* Global state isolation
* Restrictions preventing known non-deterministic library calls

Global state isolation is performed by using `exec`. Upon workflow start, and every time that the workflow is replayed,
the file that the workflow is defined in is re-imported into a new sandbox created for that workflow run. In order to
keep the sandbox performant, not all modules are re-imported in this way: instead, a known set of "passthrough modules"
are obtained as references to the already-imported module _outside_ the sandbox. These modules should be side-effect
free on import and, if they make any non-deterministic calls, then these should be restricted by sandbox restriction
rules. By default the entire Python standard library, `temporalio`, and a couple of other modules are "passed through"
in this way from outside of the sandbox. To update this list, see "Customizing the Sandbox".

Restrictions preventing known non-deterministic library calls are achieved using proxy objects on modules wrapped around
the custom importer set in the sandbox. Many restrictions apply at workflow import time and workflow run time, while
some restrictions only apply at workflow run time. A default set of restrictions is included that prevents most
dangerous standard library calls. However it is known in Python that some otherwise-non-deterministic invocations, like
reading a file from disk via `open` or using `os.environ`, are done as part of importing modules. To customize what is
and isn't restricted, see "Customizing the Sandbox".

##### Avoiding the Sandbox

There are three increasingly-scoped ways to avoid the sandbox. Users are discouraged from avoiding the sandbox if
possible, except for passing through safe modules, which is recommended.

To remove restrictions around a particular block of code, use `with temporalio.workflow.unsafe.sandbox_unrestricted():`.
The workflow will still be running in the sandbox, but no restrictions for invalid library calls will be applied.

To run an entire workflow outside of a sandbox, set `sandboxed=False` on the `@workflow.defn` decorator when defining
it. This will run the entire workflow outside of the sandbox which means it can share global state and other bad
things.

To disable the sandbox entirely for a worker, set the `Worker` init's `workflow_runner` keyword argument to
`temporalio.worker.UnsandboxedWorkflowRunner()`. This value is defaulted to
`temporalio.worker.workflow_sandbox.SandboxedWorkflowRunner()` so by changing it to the unsandboxed runner, the sandbox
will not be used at all.

##### Customizing the Sandbox

⚠️ WARNING: APIs in the `temporalio.worker.workflow_sandbox` module are not yet considered stable and may change in
future releases.

When creating the `Worker`, the `workflow_runner` is defaulted to
`temporalio.worker.workflow_sandbox.SandboxedWorkflowRunner()`. The `SandboxedWorkflowRunner`'s init accepts a
`restrictions` keyword argument that is defaulted to `SandboxRestrictions.default`. The `SandboxRestrictions` dataclass
is immutable and contains three fields that can be customized, but only two have notable value. See below.

###### Passthrough Modules

By default the sandbox completely reloads non-standard-library and non-Temporal modules for every workflow run. To make
the sandbox quicker and use less memory when importing known-side-effect-free modules, they can be marked
as passthrough modules.

**For performance and behavior reasons, users are encouraged to pass through all third party modules whose calls will be
deterministic.** In particular, this advice extends to modules containing the activities to be referenced in workflows,
and modules containing dataclasses and Pydantic models, which can be particularly expensive to import.

One way to pass through a module is at import time in the workflow file using the `imports_passed_through` context
manager like so:

```python
# my_workflow_file.py

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    import pydantic

@workflow.defn
class MyWorkflow:
    ...
```

Alternatively, this can be done at worker creation time by customizing the runner's restrictions. For example:

```python
my_worker = Worker(
  ...,
  workflow_runner=SandboxedWorkflowRunner(
    restrictions=SandboxRestrictions.default.with_passthrough_modules("pydantic")
  )
)
```

In both of these cases, now the `pydantic` module will be passed through from outside of the sandbox instead of
being reloaded for every workflow run.

If users are sure that no imports they use in workflow files will ever need to be sandboxed (meaning all calls within
are deterministic and never mutate shared, global state), the `passthrough_all_modules` option can be set on the
restrictions or the `with_passthrough_all_modules` helper can by used, for example:

```python
my_worker = Worker(
  ...,
  workflow_runner=SandboxedWorkflowRunner(
    restrictions=SandboxRestrictions.default.with_passthrough_all_modules()
  )
)
```

Note, some calls from the module may still be checked for invalid calls at runtime for certain builtins.

###### Invalid Module Members

`SandboxRestrictions.invalid_module_members` contains a root matcher that applies to all module members. This already
has a default set which includes things like `datetime.date.today()` which should never be called from a workflow. To
remove this restriction:

```python
my_restrictions = dataclasses.replace(
    SandboxRestrictions.default,
    invalid_module_members=SandboxRestrictions.invalid_module_members_default.with_child_unrestricted(
      "datetime", "date", "today",
    ),
)
my_worker = Worker(..., workflow_runner=SandboxedWorkflowRunner(restrictions=my_restrictions))
```

Restrictions can also be added by `|`'ing together matchers, for example to restrict the `datetime.date` class from
being used altogether:

```python
my_restrictions = dataclasses.replace(
    SandboxRestrictions.default,
    invalid_module_members=SandboxRestrictions.invalid_module_members_default | SandboxMatcher(
      children={"datetime": SandboxMatcher(use={"date"})},
    ),
)
my_worker = Worker(..., workflow_runner=SandboxedWorkflowRunner(restrictions=my_restrictions))
```

See the API for more details on exact fields and their meaning.

##### Known Sandbox Issues

Below are known sandbox issues. As the sandbox is developed and matures, some may be resolved.

###### Global Import/Builtins

Currently the sandbox references/alters the global `sys.modules` and `builtins` fields while running workflow code. In
order to prevent affecting other sandboxed code, thread locals are leveraged to only intercept these values during the
workflow thread running. Therefore, technically if top-level import code starts a thread, it may lose sandbox
protection.

###### Sandbox is not Secure

The sandbox is built to catch many non-deterministic and state sharing issues, but it is not secure. Some known bad
calls are intercepted, but for performance reasons, every single attribute get/set cannot be checked. Therefore a simple
call like `setattr(temporalio.common, "__my_key", "my value")` will leak across sandbox runs.

The sandbox is only a helper, it does not provide full protection.

###### Sandbox Performance

The sandbox does not add significant CPU or memory overhead for workflows that are in files which only import standard
library modules. This is because they are passed through from outside of the sandbox. However, every
non-standard-library import that is performed at the top of the same file the workflow is in will add CPU overhead (the
module is re-imported every workflow run) and memory overhead (each module independently cached as part of the workflow
run for isolation reasons). This becomes more apparent for large numbers of workflow runs.

To mitigate this, users should:

* Define workflows in files that have as few non-standard-library imports as possible
* Alter the max workflow cache and/or max concurrent workflows settings if memory grows too large
* Set third-party libraries as passthrough modules if they are known to be side-effect free

###### Extending Restricted Classes

Extending a restricted class causes Python to instantiate the restricted metaclass which is unsupported. Therefore if
you attempt to use a class in the sandbox that extends a restricted class, it will fail. For example, if you have a
`class MyZipFile(zipfile.ZipFile)` and try to use that class inside a workflow, it will fail.

Classes used inside the workflow should not extend restricted classes. For situations where third-party modules need to
at import time, they should be marked as pass through modules.

###### Certain Standard Library Calls on Restricted Objects

If an object is restricted, internal C Python validation may fail in some cases. For example, running
`dict.items(os.__dict__)` will fail with:

> descriptor 'items' for 'dict' objects doesn't apply to a '_RestrictedProxy' object

This is a low-level check that cannot be subverted. The solution is to not use restricted objects inside the sandbox.
For situations where third-party modules need to at import time, they should be marked as pass through modules.

###### is_subclass of ABC-based Restricted Classes

Due to [https://bugs.python.org/issue44847](https://bugs.python.org/issue44847), classes that are wrapped and then
checked to see if they are subclasses of another via `is_subclass` may fail (see also
[this wrapt issue](https://github.com/GrahamDumpleton/wrapt/issues/130)).


### Activities

#### Definition

Activities are decorated with `@activity.defn` like so:

```python
from temporalio import activity

@activity.defn
def say_hello_activity(name: str) -> str:
    return f"Hello, {name}!"
```

Some things to note about activity definitions:

* The `say_hello_activity` is synchronous which is the recommended activity type (see "Types of Activities" below), but
  it can be `async`
* A custom name for the activity can be set with a decorator argument, e.g. `@activity.defn(name="my activity")`
* Long running activities should regularly heartbeat and handle cancellation
* Activities can only have positional arguments. Best practice is to only take a single argument that is an
  object/dataclass of fields that can be added to as needed.
* Activities can be defined on methods instead of top-level functions. This allows the instance to carry state that an
  activity may need (e.g. a DB connection). The instance method should be what is registered with the worker.
* Activities can also be defined on callable classes (i.e. classes with `__call__`). An instance of the class should be
  what is registered with the worker.
* The `@activity.defn` can have `dynamic=True` set which means all otherwise unhandled activities fall through to this.
  If present, cannot have `name` argument, and the activity function must accept a single parameter of
  `Sequence[temporalio.common.RawValue]`. The payload of the raw value can be converted via
  `activity.payload_converter().from_payload`.

#### Types of Activities

There are 3 types of activity callables accepted and described below: synchronous multithreaded, synchronous
multiprocess/other, and asynchronous. Only positional parameters are allowed in activity callables.

##### Synchronous Activities

Synchronous activities, i.e. functions that do not have `async def`, can be used with workers, but the
`activity_executor` worker parameter must be set with a `concurrent.futures.Executor` instance to use for executing the
activities.

All long running, non-local activities should heartbeat so they can be cancelled. Cancellation in threaded activities
throws but multiprocess/other activities does not. The sections below on each synchronous type explain further. There
are also calls on the context that can check for cancellation. For more information, see "Activity Context" and
"Heartbeating and Cancellation" sections later.

Note, all calls from an activity to functions in the `temporalio.activity` package are powered by
[contextvars](https://docs.python.org/3/library/contextvars.html). Therefore, new threads starting _inside_ of
activities must `copy_context()` and then `.run()` manually to ensure `temporalio.activity` calls like `heartbeat` still
function in the new threads.

If any activity ever throws a `concurrent.futures.BrokenExecutor`, the failure is consisted unrecoverable and the worker
will fail and shutdown.

###### Synchronous Multithreaded Activities

If `activity_executor` is set to an instance of `concurrent.futures.ThreadPoolExecutor` then the synchronous activities
are considered multithreaded activities. If `max_workers` is not set to at least the worker's
`max_concurrent_activities` setting a warning will be issued. Besides `activity_executor`, no other worker parameters
are required for synchronous multithreaded activities.

By default, cancellation of a synchronous multithreaded activity is done via a `temporalio.exceptions.CancelledError`
thrown into the activity thread. Activities that do not wish to have cancellation thrown can set
`no_thread_cancel_exception=True` in the `@activity.defn` decorator.

Code that wishes to be temporarily shielded from the cancellation exception can run inside
`with activity.shield_thread_cancel_exception():`. But once the last nested form of that block is finished, even if
there is a return statement within, it will throw the cancellation if there was one. A `try` +
`except temporalio.exceptions.CancelledError` would have to surround the `with` to handle the cancellation explicitly.

###### Synchronous Multiprocess/Other Activities

If `activity_executor` is set to an instance of `concurrent.futures.Executor` that is _not_
`concurrent.futures.ThreadPoolExecutor`, then the synchronous activities are considered multiprocess/other activities.
Users should prefer threaded activities over multiprocess ones since, among other reasons, threaded activities can raise
on cancellation.

These require special primitives for heartbeating and cancellation. The `shared_state_manager` worker parameter must be
set to an instance of `temporalio.worker.SharedStateManager`. The most common implementation can be created by passing a
`multiprocessing.managers.SyncManager` (i.e. result of `multiprocessing.managers.Manager()`) to
`temporalio.worker.SharedStateManager.create_from_multiprocessing()`.

Also, all of these activity functions must be
["picklable"](https://docs.python.org/3/library/pickle.html#what-can-be-pickled-and-unpickled).

##### Asynchronous Activities

Asynchronous activities are functions defined with `async def`. Asynchronous activities are often much more performant
than synchronous ones. When using asynchronous activities no special worker parameters are needed.

**⚠️ WARNING: Do not block the thread in `async def` Python functions. This can stop the processing of the rest of the
Temporal.**

Cancellation for asynchronous activities is done via
[`asyncio.Task.cancel`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel). This means that
`asyncio.CancelledError` will be raised (and can be caught, but it is not recommended). A non-local activity must
heartbeat to receive cancellation and there are other ways to be notified about cancellation (see "Activity Context" and
"Heartbeating and Cancellation" later).

#### Activity Context

During activity execution, an implicit activity context is set as a
[context variable](https://docs.python.org/3/library/contextvars.html). The context variable itself is not visible, but
calls in the `temporalio.activity` package make use of it. Specifically:

* `in_activity()` - Whether an activity context is present
* `info()` - Returns the immutable info of the currently running activity
* `client()` - Returns the Temporal client used by this worker. Only available in `async def` activities.
* `heartbeat(*details)` - Record a heartbeat
* `is_cancelled()` - Whether a cancellation has been requested on this activity
* `wait_for_cancelled()` - `async` call to wait for cancellation request
* `wait_for_cancelled_sync(timeout)` - Synchronous blocking call to wait for cancellation request
* `shield_thread_cancel_exception()` - Context manager for use in `with` clauses by synchronous multithreaded activities
  to prevent cancel exception from being thrown during the block of code
* `is_worker_shutdown()` - Whether the worker has started graceful shutdown
* `wait_for_worker_shutdown()` - `async` call to wait for start of graceful worker shutdown
* `wait_for_worker_shutdown_sync(timeout)` - Synchronous blocking call to wait for start of graceful worker shutdown
* `raise_complete_async()` - Raise an error that this activity will be completed asynchronously (i.e. after return of
  the activity function in a separate client call)

With the exception of `in_activity()`, if any of the functions are called outside of an activity context, an error
occurs. Synchronous activities cannot call any of the `async` functions.

##### Heartbeating and Cancellation

In order for a non-local activity to be notified of cancellation requests, it must be given a `heartbeat_timeout` at
invocation time and invoke `temporalio.activity.heartbeat()` inside the activity. It is strongly recommended that all
but the fastest executing activities call this function regularly. "Types of Activities" has specifics on cancellation
for synchronous and asynchronous activities.

In addition to obtaining cancellation information, heartbeats also support detail data that is persisted on the server
for retrieval during activity retry. If an activity calls `temporalio.activity.heartbeat(123, 456)` and then fails and
is retried, `temporalio.activity.info().heartbeat_details` will return an iterable containing `123` and `456` on the
next run.

Heartbeating has no effect on local activities.

##### Worker Shutdown

An activity can react to a worker shutdown. Using `is_worker_shutdown` or one of the `wait_for_worker_shutdown`
functions an activity can react to a shutdown.

When the `graceful_shutdown_timeout` worker parameter is given a `datetime.timedelta`, on shutdown the worker will
notify activities of the graceful shutdown. Once that timeout has passed (or if wasn't set), the worker will perform
cancellation of all outstanding activities.

The `shutdown()` invocation will wait on all activities to complete, so if a long-running activity does not at least
respect cancellation, the shutdown may never complete.

#### Testing

Unit testing an activity or any code that could run in an activity is done via the
`temporalio.testing.ActivityEnvironment` class. Simply instantiate this and any callable + params passed to `run` will
be invoked inside the activity context. The following are attributes/methods on the environment that can be used to
affect calls activity code might make to functions on the `temporalio.activity` package.

* `info` property can be set to customize what is returned from `activity.info()`
* `on_heartbeat` property can be set to handle `activity.heartbeat()` calls
* `cancel()` can be invoked to simulate a cancellation of the activity
* `worker_shutdown()` can be invoked to simulate a worker shutdown during execution of the activity


### Interceptors

The behavior of the SDK can be customized in many useful ways by modifying inbound and outbound calls using
interceptors. This is similar to the use of middleware in other frameworks.

There are five categories of inbound and outbound calls that you can modify in this way:

1. Outbound client calls, such as `start_workflow()`, `signal_workflow()`, `list_workflows()`, `update_schedule()`, etc.

2. Inbound workflow calls: `execute_workflow()`, `handle_signal()`, `handle_update_handler()`, etc

3. Outbound workflow calls: `start_activity()`, `start_child_workflow()`, `start_nexus_operation()`, etc

4. Inbound call to execute an activity: `execute_activity()`

5. Outbound activity calls: `info()` and `heartbeat()`


To modify outbound client calls, define a class inheriting from
[`client.Interceptor`](https://python.temporal.io/temporalio.client.Interceptor.html), and implement the method
`intercept_client()` to return an instance of
[`OutboundInterceptor`](https://python.temporal.io/temporalio.client.OutboundInterceptor.html) that implements the
subset of outbound client calls that you wish to modify.

Then, pass a list containing an instance of your `client.Interceptor` class as the
`interceptors` argument of [`Client.connect()`](https://python.temporal.io/temporalio.client.Client.html#connect).

The purpose of the interceptor framework is that the methods you implement on your interceptor classes can perform
arbitrary side effects and/or arbitrary modifications to the data, before it is received by the SDK's "real"
implementation. The `interceptors` list can contain multiple interceptors. In this case they form a chain: a method
implemented on an interceptor instance in the list can perform side effects, and modify the data, before passing it on
to the corresponding method on the next interceptor in the list. Your interceptor classes need not implement every
method; the default implementation is always to pass the data on to the next method in the interceptor chain.

The remaining four categories are worker calls. To modify these, define a class inheriting from
[`worker.Interceptor`](https://python.temporal.io/temporalio.worker.Interceptor.html) and implement methods on that
class to define the
[`ActivityInboundInterceptor`](https://python.temporal.io/temporalio.worker.ActivityInboundInterceptor.html),
[`ActivityOutboundInterceptor`](https://python.temporal.io/temporalio.worker.ActivityOutboundInterceptor.html),
[`WorkflowInboundInterceptor`](https://python.temporal.io/temporalio.worker.WorkflowInboundInterceptor.html), and
[`WorkflowOutboundInterceptor`](https://python.temporal.io/temporalio.worker.WorkflowOutboundInterceptor.html) classes
that you wish to use to effect your modifications. Then, pass a list containing an instance of your `worker.Interceptor`
class as the `interceptors` argument of the [`Worker()`](https://python.temporal.io/temporalio.worker.Worker.html)
constructor.

It often happens that your worker and client interceptors will share code because they implement closely related logic.
For convenience, you can create an interceptor class that inherits from _both_ `client.Interceptor` and
`worker.Interceptor` (their method sets do not overlap). You can then pass this in the `interceptors` argument of
`Client.connect()` when starting your worker _as well as_ in your client/starter code. If you do this, your worker will
automatically pick up the interceptors from its underlying client (and you should not pass them directly to the
`Worker()` constructor).

This is best explained by example. The [Context Propagation Interceptor
Sample](https://github.com/temporalio/samples-python/tree/main/context_propagation) is a good starting point. In
[context_propagation/interceptor.py](https://github.com/temporalio/samples-python/blob/main/context_propagation/interceptor.py)
a class is defined that inherits from both `client.Interceptor` and `worker.Interceptor`. It implements the various
methods such that the outbound client and workflow calls set a certain key in the outbound `headers` field, and the
inbound workflow and activity calls retrieve the header value from the inbound workflow/activity input data. An instance
of this interceptor class is passed to `Client.connect()` when [starting the
worker](https://github.com/temporalio/samples-python/blob/main/context_propagation/worker.py) and when connecting the
client in the [workflow starter
code](https://github.com/temporalio/samples-python/blob/main/context_propagation/starter.py).


### Nexus

⚠️  **Nexus support is currently at an experimental release stage. Backwards-incompatible changes are anticipated until a stable release is announced.** ⚠️

[Nexus](https://github.com/nexus-rpc/) is a synchronous RPC protocol. Arbitrary duration operations that can respond
asynchronously are modeled on top of a set of pre-defined synchronous RPCs.

Temporal supports calling Nexus operations **from a workflow**. See https://docs.temporal.io/nexus. There is no support
currently for calling a Nexus operation from non-workflow code.

To get started quickly using Nexus with Temporal, see the Python Nexus sample:
https://github.com/temporalio/samples-python/tree/nexus/hello_nexus.


Two types of Nexus operation are supported, each using a decorator:

- `@temporalio.nexus.workflow_run_operation`: a Nexus operation that is backed by a Temporal workflow. The operation
  handler you write will start the handler workflow and then respond with a token indicating that the handler workflow
  is in progress. When the handler workflow completes, Temporal server will automatically deliver the result (success or
  failure) to the caller workflow.
- `@nexusrpc.handler.sync_operation`: an operation that responds synchronously. It may be `def` or `async def` and it
may do network I/O, but it must respond within 10 seconds.

The following steps are an overview of the [Python Nexus sample](
https://github.com/temporalio/samples-python/tree/nexus/hello_nexus).

1. Create the caller and handler namespaces, and the Nexus endpoint. For example,
    ```
    temporal operator namespace create --namespace my-handler-namespace
    temporal operator namespace create --namespace my-caller-namespace

    temporal operator nexus endpoint create \
      --name my-nexus-endpoint \
      --target-namespace my-handler-namespace \
      --target-task-queue my-handler-task-queue
    ```

2. Define your service contract. This specifies the names and input/output types of your operations. You will use this
   to refer to the operations when calling them from a workflow.
    ```python
    @nexusrpc.service
    class MyNexusService:
        my_sync_operation: nexusrpc.Operation[MyInput, MyOutput]
        my_workflow_run_operation: nexusrpc.Operation[MyInput, MyOutput]
    ```

3. Implement your operation handlers in a service handler:
    ```python
    @service_handler(service=MyNexusService)
    class MyNexusServiceHandler:
        @sync_operation
        async def my_sync_operation(
            self, ctx: StartOperationContext, input: MyInput
        ) -> MyOutput:
            return MyOutput(message=f"Hello {input.name} from sync operation!")

        @workflow_run_operation
        async def my_workflow_run_operation(
            self, ctx: WorkflowRunOperationContext, input: MyInput
        ) -> nexus.WorkflowHandle[MyOutput]:
            return await ctx.start_workflow(
                WorkflowStartedByNexusOperation.run,
                input,
                id=str(uuid.uuid4()),
            )
    ```

4. Register your service handler with a Temporal worker.
    ```python
    client = await Client.connect("localhost:7233", namespace="my-handler-namespace")
    worker = Worker(
        client,
        task_queue="my-handler-task-queue",
        workflows=[WorkflowStartedByNexusOperation],
        nexus_service_handlers=[MyNexusServiceHandler()],
    )
    await worker.run()
    ```

5. Call your Nexus operations from your caller workflow.
    ```python
    @workflow.defn
    class CallerWorkflow:
        def __init__(self):
            self.nexus_client = workflow.create_nexus_client(
                service=MyNexusService, endpoint="my-nexus-endpoint"
            )

        @workflow.run
        async def run(self, name: str) -> tuple[MyOutput, MyOutput]:
            # Start the Nexus operation and wait for the result in one go, using execute_operation.
            wf_result = await self.nexus_client.execute_operation(
                MyNexusService.my_workflow_run_operation,
                MyInput(name),
            )
            # Or alternatively, obtain the operation handle using start_operation,
            # and then use it to get the result:
            sync_operation_handle = await self.nexus_client.start_operation(
                MyNexusService.my_sync_operation,
                MyInput(name),
            )
            sync_result = await sync_operation_handle
            return sync_result, wf_result
    ```


### Plugins

Plugins provide a way to extend and customize the behavior of Temporal clients and workers through a chain of 
responsibility pattern. They allow you to intercept and modify client creation, service connections, worker 
configuration, and worker execution. Common customizations may include but are not limited to:

1. DataConverter
2. Activities
3. Workflows
4. Interceptors

A single plugin class can implement both client and worker plugin interfaces to share common logic between both 
contexts. When used with a client, it will automatically be propagated to any workers created with that client.

#### Client Plugins

Client plugins can intercept and modify client configuration and service connections. They are useful for adding 
authentication, modifying connection parameters, or adding custom behavior during client creation.

Here's an example of a client plugin that adds custom authentication:

```python
from temporalio.client import Plugin, ClientConfig
import temporalio.service

class AuthenticationPlugin(Plugin):
    def __init__(self, api_key: str):
        self.api_key = api_key
        
    def init_client_plugin(self, next: Plugin) -> None:
      self.next_client_plugin = next

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        # Modify client configuration
        config["namespace"] = "my-secure-namespace"
        return self.next_client_plugin.configure_client(config)

    async def connect_service_client(
        self, config: temporalio.service.ConnectConfig
    ) -> temporalio.service.ServiceClient:
        # Add authentication to the connection
        config.api_key = self.api_key
        return await self.next_client_plugin.connect_service_client(config)

# Use the plugin when connecting
client = await Client.connect(
    "my-server.com:7233",
    plugins=[AuthenticationPlugin("my-api-key")]
)
```

#### Worker Plugins

Worker plugins can modify worker configuration and intercept worker execution. They are useful for adding monitoring, 
custom lifecycle management, or modifying worker settings. Worker plugins can also configure replay. 
They should do this in the case that they modified the worker in a way which would also need to be present 
for replay to function. For instance, changing the data converter or adding workflows. 

Here's an example of a worker plugin that adds custom monitoring:

```python
import temporalio
from contextlib import asynccontextmanager
from typing import AsyncIterator
from temporalio.worker import Plugin, WorkerConfig, ReplayerConfig, Worker, Replayer, WorkflowReplayResult
import logging

class MonitoringPlugin(Plugin):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def init_worker_plugin(self, next: Plugin) -> None:
        self.next_worker_plugin = next
        
    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        # Modify worker configuration
        original_task_queue = config["task_queue"]
        config["task_queue"] = f"monitored-{original_task_queue}"
        self.logger.info(f"Worker created for task queue: {config['task_queue']}")
        return self.next_worker_plugin.configure_worker(config)

    async def run_worker(self, worker: Worker) -> None:
        self.logger.info("Starting worker execution")
        try:
            await self.next_worker_plugin.run_worker(worker)
        finally:
            self.logger.info("Worker execution completed")
      
  def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
      return self.next_worker_plugin.configure_replayer(config)
  
  @asynccontextmanager
  async def run_replayer(
      self,
      replayer: Replayer,
      histories: AsyncIterator[temporalio.client.WorkflowHistory],
  ) -> AsyncIterator[AsyncIterator[WorkflowReplayResult]]:
        self.logger.info("Starting replay execution")
        try:
          async with self.next_worker_plugin.run_replayer(replayer, histories) as results:
              yield results
        finally:
            self.logger.info("Replay execution completed")

# Use the plugin when creating a worker
worker = Worker(
    client,
    task_queue="my-task-queue",
    workflows=[MyWorkflow],
    activities=[my_activity],
    plugins=[MonitoringPlugin()]
)
```

For plugins that need to work with both clients and workers, you can implement both interfaces in a single class:

```python
import temporalio
from contextlib import AbstractAsyncContextManager
from typing import AsyncIterator
from temporalio.client import Plugin as ClientPlugin, ClientConfig
from temporalio.worker import Plugin as WorkerPlugin, WorkerConfig, ReplayerConfig, Worker, Replayer, WorkflowReplayResult


class UnifiedPlugin(ClientPlugin, WorkerPlugin):
    def init_client_plugin(self, next: ClientPlugin) -> None:
        self.next_client_plugin = next

    def init_worker_plugin(self, next: WorkerPlugin) -> None:
        self.next_worker_plugin = next
  
    def configure_client(self, config: ClientConfig) -> ClientConfig:
        # Client-side customization
        config["data_converter"] = pydantic_data_converter
        return self.next_client_plugin.configure_client(config)
  
    async def connect_service_client(
        self, config: temporalio.service.ConnectConfig
    ) -> temporalio.service.ServiceClient:
        # Add authentication to the connection
        config.api_key = self.api_key
        return await self.next_client_plugin.connect_service_client(config)
        
    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        # Worker-side customization
        return self.next_worker_plugin.configure_worker(config)
  
    async def run_worker(self, worker: Worker) -> None:
        print("Starting unified worker")
        await self.next_worker_plugin.run_worker(worker)
        
    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        config["data_converter"] = pydantic_data_converter
        return config
    
    async def run_replayer(
        self,
        replayer: Replayer,
        histories: AsyncIterator[temporalio.client.WorkflowHistory],
    ) -> AbstractAsyncContextManager[AsyncIterator[WorkflowReplayResult]]:
        return self.next_worker_plugin.run_replayer(replayer, histories)
              
# Create client with the unified plugin
client = await Client.connect(
    "localhost:7233",
    plugins=[UnifiedPlugin()]
)

# Worker will automatically inherit the plugin from the client
worker = Worker(
    client,
    task_queue="my-task-queue",
    workflows=[MyWorkflow],
    activities=[my_activity]
)
```

**Important Notes:**

- Plugins are executed in reverse order (last plugin wraps the first), forming a chain of responsibility
- Client plugins that also implement worker plugin interfaces are automatically propagated to workers
- Avoid providing the same plugin to both client and worker to prevent double execution
- Plugin methods should call the plugin provided during initialization to maintain the plugin chain
- Each plugin's `name()` method returns a unique identifier for debugging purposes


### Workflow Replay

Given a workflow's history, it can be replayed locally to check for things like non-determinism errors. For example,
assuming `history_str` is populated with a JSON string history either exported from the web UI or from `tctl`, the
following function will replay it:

```python
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

async def run_replayer(history_str: str):
  replayer = Replayer(workflows=[SayHello])
  await replayer.replay_workflow(WorkflowHistory.from_json(history_str))
```

This will throw an error if any non-determinism is detected.

Replaying from workflow history is a powerful concept that many use to test that workflow alterations won't cause
non-determinisms with past-complete workflows. The following code will make sure that all workflow histories for a
certain workflow type (i.e. workflow class) are safe with the current code.

```python
from temporalio.client import Client, WorkflowHistory
from temporalio.worker import Replayer

async def check_past_histories(my_client: Client):
  replayer = Replayer(workflows=[SayHello])
  await replayer.replay_workflows(
    await my_client.list_workflows("WorkflowType = 'SayHello'").map_histories(),
  )
```

### Observability

See https://github.com/temporalio/samples-python/tree/main/open_telemetry for a sample demonstrating collection of
metrics and tracing data emitted by the SDK.

#### Metrics

The SDK emits various metrics by default: see https://docs.temporal.io/references/sdk-metrics. To configure additional
attributes to be emitted with all metrics, pass
[global_tags](https://python.temporal.io/temporalio.runtime.TelemetryConfig.html#global_tags) when creating the
[TelemetryConfig](https://python.temporal.io/temporalio.runtime.TelemetryConfig.html).

For emitting custom metrics, the SDK makes a metric meter available:
- In Workflow code, use https://python.temporal.io/temporalio.workflow.html#metric_meter
- In Activity code, use https://python.temporal.io/temporalio.activity.html#metric_meter
- In normal application code, use https://python.temporal.io/temporalio.runtime.Runtime.html#metric_meter

The attributes emitted by these default to `namespace`, `task_queue`, and `workflow_type`/`activity_type`; use
`with_additional_attributes` to create a meter emitting additional attributes.

#### OpenTelemetry Tracing

Tracing support requires the optional `opentelemetry` dependencies which are part of the `opentelemetry` extra. When
using `pip`, running

    pip install 'temporalio[opentelemetry]'

will install needed dependencies. Then the `temporalio.contrib.opentelemetry.TracingInterceptor` can be created and set
as an interceptor on the `interceptors` argument of `Client.connect`. When set, spans will be created for all client
calls and for all activity and workflow invocations on the worker, spans will be created and properly serialized through
the server to give one proper trace for a workflow execution.

### Protobuf 3.x vs 4.x

Python currently has two somewhat-incompatible protobuf library versions - the 3.x series and the 4.x series. Python
currently recommends 4.x and that is the primary supported version. Some libraries like
[Pulumi](https://github.com/pulumi/pulumi) require 4.x. Other libraries such as [ONNX](https://github.com/onnx/onnx) and
[Streamlit](https://github.com/streamlit/streamlit), for one reason or another, have/will not leave 3.x.

To support these, Temporal Python SDK allows any protobuf library >= 3.19. However, the C extension in older Python
versions can cause issues with the sandbox due to global state sharing. Temporal strongly recommends using the latest
protobuf 4.x library unless you absolutely cannot at which point some proto libraries may have to be marked as
[Passthrough Modules](#passthrough-modules).

### Known Compatibility Issues

Below are known compatibility issues with the Python SDK.

#### gevent Patching

When using `gevent.monkey.patch_all()`, asyncio event loops can get messed up, especially those using custom event loops
like Temporal. See [this gevent issue](https://github.com/gevent/gevent/issues/982). This is a known incompatibility and
users are encouraged to not use gevent in asyncio applications (including Temporal). But if you must, there is
[a sample](https://github.com/temporalio/samples-python/tree/main/gevent_async) showing how it is possible.

# Development

The Python SDK is built to work with Python 3.9 and newer. It is built using
[SDK Core](https://github.com/temporalio/sdk-core/) which is written in Rust.

### Building

#### Prepare

To build the SDK from source for use as a dependency, the following prerequisites are required:

* [uv](https://docs.astral.sh/uv/)
* [Rust](https://www.rust-lang.org/)
* [Protobuf Compiler](https://protobuf.dev/)

Use `uv` to install `poe`:

```bash
uv tool install poethepoet
```

Now clone the SDK repository recursively:

```bash
git clone --recursive https://github.com/temporalio/sdk-python.git
cd sdk-python
```

Install the dependencies:

```bash
uv sync --all-extras
```

#### Build

Now perform the release build:

> This will take a while because Rust will compile the core project in release mode (see [Local SDK development
environment](#local-sdk-development-environment) for the quicker approach to local development).

```bash
uv build
```


The `.whl` wheel file in `dist/` is now ready to use.

#### Use

The wheel can now be installed into any virtual environment.

For example,
[create a virtual environment](https://packaging.python.org/en/latest/tutorials/installing-packages/#creating-virtual-environments)
somewhere and then run the following inside the virtual environment:

```bash
pip install wheel
```

```bash
pip install /path/to/cloned/sdk-python/dist/*.whl
```

Create this Python file at `example.py`:

```python
import asyncio
from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker

@workflow.defn
class SayHello:
    @workflow.run
    async def run(self, name: str) -> str:
        return f"Hello, {name}!"

async def main():
    client = await Client.connect("localhost:7233")
    async with Worker(client, task_queue="my-task-queue", workflows=[SayHello]):
        result = await client.execute_workflow(SayHello.run, "Temporal",
            id="my-workflow-id", task_queue="my-task-queue")
        print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

Assuming there is a [local Temporal server](https://docs.temporal.io/docs/server/quick-install/) running, execute the
file with `python` (or `python3` if necessary):

```bash
python example.py
```

It should output:

    Result: Hello, Temporal!

### Local SDK development environment

For local development, it is quicker to use a debug build.

Perform the same steps as the "Prepare" section above by installing the prerequisites, cloning the project, and
installing dependencies:

```bash
git clone --recursive https://github.com/temporalio/sdk-python.git
cd sdk-python
uv sync --all-extras
```

Now compile the Rust extension in develop mode which is quicker than release mode:

```bash
poe build-develop
```

That step can be repeated for any Rust changes made.

The environment is now ready to develop in.

#### Testing

To execute tests:

```bash
poe test
```

This runs against [Temporalite](https://github.com/temporalio/temporalite). To run against the time-skipping test
server, pass `--workflow-environment time-skipping`. To run against the `default` namespace of an already-running
server, pass the `host:port` to `--workflow-environment`. Can also use regular pytest arguments. For example, here's how
to run a single test with debug logs on the console:

```bash
poe test -s --log-cli-level=DEBUG -k test_sync_activity_thread_cancel_caught
```

#### Proto Generation and Testing

To allow for backwards compatibility, protobuf code is generated on the 3.x series of the protobuf library. To generate
protobuf code, you must be on Python <= 3.10, and then run `uv add "protobuf<4"` + `uv sync --all-extras`. Then the
protobuf files can be generated via `poe gen-protos`. Tests can be run for protobuf version 3 by setting the
`TEMPORAL_TEST_PROTO3` env var to `1` prior to running tests.

Do not commit `uv.lock` or `pyproject.toml` changes. To go back from this downgrade, restore both of those files and run
`uv sync --all-extras`. Make sure you `poe format` the results.

For a less system-intrusive approach, you can:
```shell
docker build -f scripts/_proto/Dockerfile .
docker run --rm -v "${PWD}/temporalio/api:/api_new" -v "${PWD}/temporalio/bridge/proto:/bridge_new" <just built image sha>
poe format
```

### Style

* Mostly [Google Style Guide](https://google.github.io/styleguide/pyguide.html). Notable exceptions:
  * We use [ruff](https://docs.astral.sh/ruff/) for formatting, so that takes precedence
  * In tests and example code, can import individual classes/functions to make it more readable. Can also do this for
    rarely in library code for some Python common items (e.g. `dataclass` or `partial`), but not allowed to do this for
    any `temporalio` packages (except `temporalio.types`) or any classes/functions that aren't clear when unqualified.
  * We allow relative imports for private packages
  * We allow `@staticmethod`
