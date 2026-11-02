# OpenAI Agents SDK Integration for Temporal

We welcome questions and feedback in the [#python-sdk](https://temporalio.slack.com/archives/CTT84RS0P) Slack channel at [temporalio.slack.com](https://temporalio.slack.com/).

## Introduction

This integration combines [OpenAI Agents SDK](https://github.com/openai/openai-agents-python) with [Temporal's durable execution](https://docs.temporal.io/evaluate/understanding-temporal#durable-execution).
It allows you to build durable agents that never lose their progress and handle long-running, asynchronous, and human-in-the-loop workflows with production-grade reliability.

Temporal and OpenAI Agents SDK are complementary technologies, both of which contribute to simplifying what it takes to build highly capable, high-quality AI systems.
Temporal provides a crash-proof system foundation, taking care of the distributed systems challenges inherent to production agentic systems.
OpenAI Agents SDK offers a lightweight yet powerful framework for defining those agents.

This document is organized as follows:

- **[Hello World Durable Agent](#hello-world-durable-agent).** Your first durable agent example.
- **[Background Concepts](#core-concepts).** Background on durable execution and AI agents.
- **[Full Example](#full-example)** Running the Hello World Durable Agent example.
- **[Tool Calling](#tool-calling).** Calling agent Tools in Temporal.
- **[Sandbox Support](#sandbox-support).** Running sandbox agents in Temporal.
- **[Feature Support](#feature-support).** Compatibility matrix.

The [samples repository](https://github.com/temporalio/samples-python/tree/main/openai_agents) contains examples including basic usage, common agent patterns, and more complete samples.

## Hello World Durable Agent

The code below shows how to wrap an agent for durable execution.

### File 1: Durable Agent (`hello_world.py`)

```python
from temporalio import workflow
from agents import Agent, Runner

@workflow.defn
class HelloWorldAgent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        agent = Agent(
            name="Assistant",
            instructions="You only respond in haikus.",
        )

        result = await Runner.run(agent, input=prompt)
        return result.final_output
```

In this example, Temporal provides the durable execution wrapper: the `HelloWorldAgent.run` method.
The content of that method, is regular OpenAI Agents SDK code.

If you are familiar with Temporal and with Open AI Agents SDK, this code will look very familiar.
The `@workflow.defn` annotation on the `HelloWorldAgent` indicates that this class will contain durable execution logic. The `@workflow.run` annotation defines the entry point.
We use the `Agent` class from OpenAI Agents SDK to define a simple agent, instructing it to always respond with haikus.
We then run that agent, using the `Runner` class from OpenAI Agents SDK, passing through `prompt` as an argument.

We will [complete this example below](#full-example).
Before digging further into the code, we will review some background that will make it easier to understand.

## Background Concepts

We encourage you to review this section thoroughly to gain a solid understanding of AI agents and durable execution with Temporal.
This knowledge will make it easier to design and build durable agents.
If you are already well versed in these topics, feel free to skim this section or skip ahead.

### AI Agents

In the OpenAI Agents SDK, an agent is an AI model configured with instructions, tools, MCP servers, guardrails, handoffs, context, and more.

We describe each of these briefly:

- _AI model_. An LLM such as OpenAI's GPT, Google's Gemini, or one of many others.
- _Instructions_. Also known as a system prompt, the instructions contain the initial input to the model, which configures it for the job it will do.
- _Tools_. Typically, Python functions that the model may choose to invoke. Tools are functions with text-descriptions that explain their functionality to the model.
- _MCP servers_. Best known for providing tools, MCP offers a pluggable standard for interoperability, including file-like resources, prompt templates, and human approvals. MCP servers may be accessed over the network or run in a local process.
- _Guardrails_. Checks on the input or the output of an agent to ensure compliance or safety. Guardrails may be implemented as regular code or as AI agents.
- _Handoffs_. A handoff occurs when an agent delegates a task to another agent. During a handoff the conversation history remains the same, and passes to a new agent with its own model, instructions, tools.
- _Context_. This is an overloaded term. Here, context refers to a framework object that is shared across tools and other code, but is not passed to the model.

Now, let's see how these components work together.
In a common pattern, the model first receives user input and then reasons about which tool to invoke.
The tool's response is passed back to the model, which may call additional tools, repeating this loop until the task is complete.

The diagram below illustrates this flow.

```text
           +-------------------+
           |     User Input    |
           +-------------------+
                     |
                     v
          +---------------------+
          |  Reasoning (Model)  |  <--+
          +---------------------+     |
                     |                |
           (decides which action)     |
                     v                |
          +---------------------+     |
          |       Action        |     |
          | (e.g., use a Tool)  |     |
          +---------------------+     |
                     |                |
                     v                |
          +---------------------+     |
          |     Observation     |     |
          |    (Tool Output)    |     |
          +---------------------+     |
                     |                |
                     +----------------+
          (loop: uses new info to reason
           again, until task is complete)
```

Even in a simple example like this, there are many places where things can go wrong.
Tools call APIs that sometimes fail, while models can encounter rate limits, requiring retries.
The longer the agent runs, the more costly it is to start the job over.
We next describe durable execution, which handles such failures seamlessly.

### Durable Execution

In Temporal's durable execution implementation, a program that crashes or encounters an exception while interacting with a model or API will retry until it can successfully complete.

Temporal relies primarily on a replay mechanism to recover from failures.
As the program makes progress, Temporal saves key inputs and decisions, allowing a re-started program to pick up right where it left off.

The key to making this work is to separate the applications repeatable (deterministic) and non-repeatable (non-deterministic) parts:

1. Deterministic pieces, termed _workflows_, execute the same way when re-run with the same inputs.
2. Non-deterministic pieces, termed _activities_, can run arbitrary code, performing I/O and any other operations.

Workflow code can run for extended periods and, if interrupted, resume exactly where it left off.
Activity code faces no restrictions on I/O or external interactions, but if it fails part-way through it restarts from the beginning.

In this integration, model invocations are automatically routed through Temporal activities, while the logic that coordinates them lives in the workflow.
Tools that perform I/O or other non-deterministic work should run as Temporal activities, while deterministic, workflow-safe tools can run directly in the workflow.
This pattern generalizes to more sophisticated agents.
We refer to the coordinating logic as _agent orchestration_.

The diagram below shows the overall architecture of an agentic application in Temporal.
The Temporal Server is responsible to tracking program execution and making sure associated state is preserved reliably (i.e., stored to a database, possibly replicated across cloud regions).
Temporal Server manages data in encrypted form, so all data processing occurs on the Worker, which runs the workflow and activities.

```text
            +---------------------+
            |   Temporal Server   |      (Stores workflow state,
            +---------------------+       schedules activities,
                     ^                    persists progress)
                     |
        Save state,  |   Schedule Tasks,
        progress,    |   load state on resume
        timeouts     |
                     |
+------------------------------------------------------+
|                      Worker                          |
|   +----------------------------------------------+   |
|   |              Workflow Code                   |   |
|   |  (Agent orchestration + deterministic tools) |   |
|   +----------------------------------------------+   |
|          |          |                |               |
|          v          v                v               |
|   +-----------+ +-----------+ +-------------+        |
|   | Activity  | | Activity  | |  Activity   |        |
|   | (I/O Tool | | (I/O Tool | | (Model API) |        |
|   |     1)    | |     2)    | |             |        |
|   +-----------+ +-----------+ +-------------+        |
|         |           |                |               |
+------------------------------------------------------+
          |           |                |
          v           v                v
      [External APIs, services, databases, etc.]
```

See the [Temporal documentation](https://docs.temporal.io/evaluate/understanding-temporal#temporal-application-the-building-blocks) for more information.

## Complete Example

To make the [Hello World durable agent](#hello-world-durable-agent) shown earlier available in Temporal, we need to create a worker program.
To see it run, we also need a client to launch it.
We show these files below.

### File 2: Launch Worker (`run_worker.py`)

```python
# File: run_worker.py

import asyncio
from datetime import timedelta

from temporalio.client import Client
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin, ModelActivityParameters
from temporalio.worker import Worker

from hello_world_workflow import HelloWorldAgent


async def worker_main():
    # Use the plugin to configure Temporal for use with OpenAI Agents SDK
    client = await Client.connect(
        "localhost:7233",
        plugins=[
            OpenAIAgentsPlugin(
                model_params=ModelActivityParameters(
                    start_to_close_timeout=timedelta(seconds=30)
                )
            ),
        ],
    )

    worker = Worker(
        client,
        task_queue="my-task-queue",
        workflows=[HelloWorldAgent],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(worker_main())
```

We use the `OpenAIAgentsPlugin` to configure Temporal for use with OpenAI Agents SDK.
The plugin automatically handles several important setup tasks:

- Ensures proper serialization of Pydantic types
- Propagates context for [OpenAI Agents tracing](https://openai.github.io/openai-agents-python/tracing/).
- Registers an activity for invoking model calls with the Temporal worker.
- Configures OpenAI Agents SDK to run model calls as Temporal activities.

### File 3: Client Execution (`run_hello_world_workflow.py`)

```python
# File: run_hello_world_workflow.py

import asyncio

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin

from hello_world_workflow import HelloWorldAgent

async def main():
    # Create client connected to server at the given address
    client = await Client.connect(
        "localhost:7233",
        plugins=[OpenAIAgentsPlugin()],
    )

    # Execute a workflow
    result = await client.execute_workflow(
        HelloWorldAgent.run,
        "Tell me about recursion in programming.",
        id="my-workflow-id",
        task_queue="my-task-queue",
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy=WorkflowIDConflictPolicy.TERMINATE_EXISTING,
    )
    print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

This file is a standard Temporal launch script.
We also configure the client with the `OpenAIAgentsPlugin` to ensure serialization is compatible with the worker.

To run this example, see the detailed instructions in the [Temporal Python Samples Repository](https://github.com/temporalio/samples-python/tree/main/openai_agents).

## Tool Calling

Model invocations are automatically routed through Temporal activities.
OpenAI-hosted tools are passed through the model invocation and executed by the model provider.
User-defined `FunctionTool`s, including tools created with `@function_tool`, are not automatically converted into Temporal activities; they execute in the workflow unless explicitly backed by a Temporal activity.
Where a tool executes depends on how it is defined:

| Tool | Execution | Use for |
| --- | --- | --- |
| `activity_as_tool()` | Temporal activity | External I/O and non-deterministic operations |
| `FunctionTool` / `@function_tool` | Workflow | Deterministic, workflow-safe computation |
| OpenAI-hosted tool | Model provider | Provider-hosted features executed as part of the model invocation |

### Temporal Activities as OpenAI Agents Tools

One of the powerful features of this integration is the ability to convert Temporal activities into agent tools using `activity_as_tool`.
This allows your agent to leverage Temporal's durable execution for tool calls.
`activity_as_tool()` creates an OpenAI Agents `FunctionTool` whose invocation schedules the underlying Temporal activity.

In the example below, we apply the `@activity.defn` decorator to the `get_weather` function to create a Temporal activity.
We then pass this through the `activity_as_tool` helper function to create an OpenAI Agents tool that is passed to the `Agent`.

```python
from dataclasses import dataclass
from datetime import timedelta
from temporalio import activity, workflow
from temporalio.contrib import openai_agents
from agents import Agent, Runner

@dataclass
class Weather:
    city: str
    temperature_range: str
    conditions: str

@activity.defn
async def get_weather(city: str) -> Weather:
    """Get the weather for a given city."""
    return Weather(city=city, temperature_range="14-20C", conditions="Sunny with wind.")

@workflow.defn
class WeatherAgent:
    @workflow.run
    async def run(self, question: str) -> str:
        agent = Agent(
            name="Weather Assistant",
            instructions="You are a helpful weather agent.",
            tools=[
                openai_agents.workflow.activity_as_tool(
                    get_weather,
                    start_to_close_timeout=timedelta(seconds=10)
                )
            ],
        )
        result = await Runner.run(starting_agent=agent, input=question)
        return result.final_output
```

The activity must also be registered with a Worker.
`activity_as_tool()` controls how the Agent invokes the activity; it does not register the activity with the Worker.

```python
from temporalio.worker import Worker

worker = Worker(
    client,
    task_queue="my-task-queue",
    workflows=[WeatherAgent],
    activities=[get_weather],
)
```

### Calling OpenAI Agents Tools inside Temporal Workflows

For simple computations that don't involve external calls, you can call the tool directly from the workflow by using the standard OpenAI Agents SDK `@function_tool` decorator.

```python
from temporalio import workflow
from agents import Agent, Runner
from agents import function_tool

@function_tool
def calculate_circle_area(radius: float) -> float:
    """Calculate the area of a circle given its radius."""
    import math
    return math.pi * radius ** 2

@workflow.defn
class MathAssistantAgent:
    @workflow.run
    async def run(self, message: str) -> str:
        agent = Agent(
            name="Math Assistant",
            instructions="You are a helpful math assistant. Use the available tools to help with calculations.",
            tools=[calculate_circle_area],
        )
        result = await Runner.run(agent, input=message)
        return result.final_output
```

Use regular `@function_tool` tools only for deterministic, workflow-safe logic.
Do not perform network, database, filesystem, or other external I/O directly from these tools.
Use a Temporal activity with `activity_as_tool()` instead.
Code running in the workflow can also invoke a Temporal activity directly when needed.

Tools that run in the workflow can also update OpenAI Agents context, which is read-only for tools run as Temporal activities.

## MCP Support

This integration provides support for Model Context Protocol (MCP) servers through two wrapper approaches designed to handle different implications of failures.

While Temporal provides durable execution for your workflows, this durability does not extend to MCP servers, which operate independently of the workflow and must provide their own durability. The integration handles this by offering stateless and stateful wrappers that you can choose based on your MCP server's design.

### Stateless vs Stateful MCP Servers

You need to understand your MCP server's behavior to choose the correct wrapper:

**Stateless MCP servers** treat each operation independently. For example, a weather server with a `get_weather(location)` tool is stateless because each call is self-contained and includes all necessary information. These servers can be safely restarted or reconnected to without changing their behavior.

**Stateful MCP servers** maintain session state between calls. For example, a weather server that requires calling `set_location(location)` followed by `get_weather()` is stateful because it remembers the configured location and uses it for subsequent calls. If the session or the server is restarted, state crucial for operation is lost. Temporal identifies such failures and raises an `ApplicationError` to signal the need for application-level failure handling.

### Usage Example (Stateless MCP)

The code below gives an example of using a stateless MCP server.

#### Worker Configuration

```python
import asyncio
from datetime import timedelta
from agents.mcp import MCPServerStdio
from temporalio.client import Client
from temporalio.contrib.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
    StatelessMCPServerProvider,
)
from temporalio.worker import Worker


async def main():
    # Create the MCP server provider
    filesystem_server = StatelessMCPServerProvider(
        lambda: MCPServerStdio(
            name="FileSystemServer",
            params={
                "command": "npx",
                "args": ["-y", "@modelcontextprotocol/server-filesystem", "/path/to/files"],
            },
        )
    )

    # Register the MCP server with the OpenAI Agents plugin
    client = await Client.connect(
        "localhost:7233",
        plugins=[
            OpenAIAgentsPlugin(
                model_params=ModelActivityParameters(
                    start_to_close_timeout=timedelta(seconds=60)
                ),
                mcp_server_providers=[filesystem_server],
            ),
        ],
    )

    worker = Worker(
        client,
        task_queue="my-task-queue",
        workflows=[FileSystemWorkflow],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

#### Workflow Implementation

```python
from temporalio import workflow
from temporalio.contrib import openai_agents
from agents import Agent, Runner

@workflow.defn
class FileSystemWorkflow:
    @workflow.run
    async def run(self, query: str) -> str:
        # Reference the MCP server by name (matches name in worker configuration)
        server = openai_agents.workflow.stateless_mcp_server("FileSystemServer")

        agent = Agent(
            name="File Assistant",
            instructions="Use the filesystem tools to read files and answer questions.",
            mcp_servers=[server],
        )

        result = await Runner.run(agent, input=query)
        return result.final_output
```

The `StatelessMCPServerProvider` takes a factory function that creates new MCP server instances. The server name used in `stateless_mcp_server()` must match the name configured in the MCP server instance. In this example, the name is `FileSystemServer`.

### Stateful MCP Servers

For implementation details and examples, see the [samples repository](https://github.com/temporalio/samples-python/tree/main/openai_agents/mcp).

When using stateful servers, the dedicated worker maintaining the connection may fail due to network issues or server problems. When this happens, Temporal raises an `ApplicationError` and cannot automatically recover because it cannot restore the lost server state.
To recover from such failures, you need to implement your own application-level retry logic.

### Factory Arguments

Both `stateless_mcp_server()` and `stateful_mcp_server()` accept an optional `factory_argument`, which is passed to the registered server factory when the MCP server is created.

A stateless factory that declares no parameters — like the `lambda: MCPServerStdio(...)` example above — ignores the value, but it is still recorded in history.

**Do not pass secrets, credentials, or API keys through `factory_argument`.** It is an activity argument, so it is recorded in workflow history and, without a payload codec, visible in the web UI. Resolve credentials worker-side inside the server factory instead.

### Hosted MCP Tool

For network-accessible MCP servers, you can also use `HostedMCPTool` from the OpenAI Agents SDK, which uses an MCP client hosted by OpenAI.

## Secrets from the Worker's Environment

⚠️ **Experimental** - This functionality is subject to change prior to General Availability.

A credential an agent needs can stay in the worker process's environment instead of being written into your workflow. Where the value would otherwise go, you name the environment variable that holds it, and the worker reads that variable when the value is actually needed.

There are two forms, and which one you use follows from where the value goes:

- For a hosted tool credential, use `temporal_worker_env_ref()`. It is substituted only in the fields listed under [Hosted Tool Credentials](#hosted-tool-credentials).
- For a sandbox environment variable, use `TemporalWorkerEnvValue`.

Both are gated by `resolvable_worker_env_vars`, an allowlist of the variable names a worker is willing to read. On every worker that runs model or sandbox activities, set the variable and add its name to that list:

```python
plugin = OpenAIAgentsPlugin(resolvable_worker_env_vars=["MY_MCP_TOKEN"])
```

Names are matched exactly, with no globbing. Passing `AllowAllWorkerEnvVars()` in place of the list makes every environment variable on the worker resolvable, so a workflow-authored sandbox manifest can name any variable on the worker and have its value land inside the container.

```python
from temporalio.contrib.openai_agents import AllowAllWorkerEnvVars

plugin = OpenAIAgentsPlugin(resolvable_worker_env_vars=AllowAllWorkerEnvVars())
```

The reference form never raises. A name the worker does not allow is sent on as the reference string, and a name it allows resolves to whatever the variable holds — an empty string when that variable is unset or empty.

### Hosted Tool Credentials

Pass `temporal_worker_env_ref()` the *name of an environment variable*, in place of the credential itself:

```python
from agents import HostedMCPTool
from temporalio.contrib.openai_agents import temporal_worker_env_ref

tool = HostedMCPTool(
    tool_config={
        "type": "mcp",
        "server_label": "my_server",
        "server_url": "https://example.com/mcp",
        "authorization": temporal_worker_env_ref("MY_MCP_TOKEN"),
    }
)
```

A reference can sit inside a larger value: in `"Bearer " + temporal_worker_env_ref("MY_MCP_TOKEN")`, the reference is replaced in place and the rest of the string is sent unchanged.

The environment variable's value is substituted in these fields and no others:

- `authorization`, and the value of each entry in `headers`, in a `HostedMCPTool`'s `tool_config`
- `value` in each entry of `network_policy.domain_secrets` under a hosted `ShellTool`'s `environment`
- `value` in each entry of `network_policy.domain_secrets` under the `container` in a `CodeInterpreterTool`'s `tool_config`

### Sandbox Environment Variables

Put a `TemporalWorkerEnvValue` in the environment of a [sandbox](#sandbox-support) manifest, in place of the value itself:

```python
from agents.sandbox import Manifest
from agents.sandbox.manifest import Environment

from temporalio.contrib.openai_agents import TemporalWorkerEnvValue

manifest = Manifest(
    environment=Environment(
        value={
            "OPENAI_API_KEY": TemporalWorkerEnvValue(name="PROD_OPENAI_KEY"),
            "REGION": "us-west-2",
        }
    )
)
```

Pass that manifest to `SandboxRunConfig(manifest=...)`. This reads `PROD_OPENAI_KEY` on the worker and sets `OPENAI_API_KEY` inside the sandbox, so the two names need not match.

## Sandbox Support

⚠️ **Pre-release** - This functionality is subject to change prior to General Availability.

The sandbox integration lets `SandboxAgent` from the OpenAI Agents SDK execute inside a remote or local sandbox (Daytona, Docker, E2B, local Unix, etc.) while keeping all coordination durable in Temporal.

Every sandbox operation — creating a session, running commands, reading/writing files, PTY interactions — is dispatched as a Temporal activity. This means sandbox work is fully observable, retryable, and recoverable like any other activity, and sandbox session state is serialized with the workflow so it survives worker restarts.

### Architecture

```text
Workflow Code
  ↓
temporal_sandbox_client("daytona")   [returns TemporalSandboxClient]
  ↓
SandboxAgent.run(run_config=RunConfig(sandbox=SandboxRunConfig(client=...)))
  ↓
sandbox agent calls session.exec / session.read / session.write / …
  ↓
TemporalSandboxSession routes each call as a Temporal activity
("daytona-sandbox_session_exec", "daytona-sandbox_session_read", …)
  ↓
SandboxClientProvider activities on the worker call the real sandbox client
  ↓
Actual sandbox backend (Daytona, Docker, local, …)
```

### Worker Configuration

Register one or more `SandboxClientProvider` instances with the plugin. Each provider pairs a unique name with a real `BaseSandboxClient` implementation. The plugin automatically registers all required activities on the worker.

```python
import asyncio
from datetime import timedelta
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin, SandboxClientProvider, ModelActivityParameters
from agents.extensions.sandbox.daytona import DaytonaSandboxClient
from agents.sandbox.sandboxes.unix_local import UnixLocalSandboxClient

async def main():
    client = await Client.connect(
        "localhost:7233",
        plugins=[
            OpenAIAgentsPlugin(
                model_params=ModelActivityParameters(
                    start_to_close_timeout=timedelta(seconds=30)
                ),
                sandbox_clients=[
                    SandboxClientProvider("daytona", DaytonaSandboxClient()),
                    SandboxClientProvider("local", UnixLocalSandboxClient()),
                ],
            ),
        ],
    )

    worker = Worker(
        client,
        task_queue="my-task-queue",
        workflows=[MyWorkflow],
    )
    await worker.run()
```

Provider names must be unique. Each name becomes the prefix for that backend's activities, allowing multiple backends to coexist on a single worker.

### Workflow Usage

In the workflow, use `temporal_sandbox_client()` to create a reference to a registered backend by name. Pass it to `SandboxRunConfig` inside `RunConfig`:

```python
from temporalio import workflow
from temporalio.contrib.openai_agents.workflow import temporal_sandbox_client
from agents import Runner
from agents.sandbox import SandboxAgent, SandboxRunConfig
from agents.run import RunConfig
from agents.extensions.sandbox.daytona import DaytonaSandboxClientOptions

@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self, prompt: str) -> str:
        agent = SandboxAgent(
            name="Coding Assistant",
            instructions="You are a helpful coding assistant with access to a sandbox.",
        )

        result = await Runner.run(
            agent,
            prompt,
            run_config=RunConfig(
                sandbox=SandboxRunConfig(
                    client=temporal_sandbox_client("daytona"),
                    options=DaytonaSandboxClientOptions(pause_on_exit=False),
                ),
            ),
        )
        return result.final_output
```

The name passed to `temporal_sandbox_client()` must exactly match the name used in `SandboxClientProvider` on the worker.

### Multiple Backends

A single workflow can target different backends by name. Register all backends on the worker and reference each by name in the workflow:

```python
# Run a task on the "daytona" backend
result = await Runner.run(
    agent, prompt,
    run_config=RunConfig(sandbox=SandboxRunConfig(
        client=temporal_sandbox_client("daytona"),
        options=DaytonaSandboxClientOptions(pause_on_exit=False),
    )),
)

# Run a different task on the "local" backend
result = await Runner.run(
    agent, prompt,
    run_config=RunConfig(sandbox=SandboxRunConfig(
        client=temporal_sandbox_client("local"),
        options=UnixLocalSandboxClientOptions(),
    )),
)
```

## Streaming

⚠️ **Experimental** - This functionality is subject to change prior to General Availability.

The integration supports streaming model responses via the SDK-native
`Runner.run_streamed` API. Inside a workflow, model calls execute as a
streaming activity (`invoke_model_activity_streaming`) that consumes
`Model.stream_response` and returns the collected list of native OpenAI
response events. The workflow surfaces those events to the caller
through `RunResultStreaming.stream_events()`, which wraps them in the
agents-SDK `StreamEvent` union (so raw model events arrive as
`RawResponsesStreamEvent.data`).

External consumers (UIs, tracing pipelines, etc.) observe events as
they arrive by hosting a [`WorkflowStream`](../workflow_streams/README.md)
in the workflow and subscribing with `WorkflowStreamClient`. The
streaming activity publishes each event to the topic configured on
`ModelActivityParameters.streaming_topic`. The topic is required
when using `Runner.run_streamed`; calling it without a configured topic
raises before any activity is scheduled.

Example workflow consuming events via `stream_events()` while the
streaming activity publishes to the `"events"` topic:

```python
from agents import Agent, Runner
from agents.stream_events import RawResponsesStreamEvent

from temporalio import workflow

@workflow.defn
class MyAgent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        agent = Agent(name="Assistant", instructions="...")
        result = Runner.run_streamed(agent, prompt)
        async for event in result.stream_events():
            if isinstance(event, RawResponsesStreamEvent):
                raw_event = event.data  # native OpenAI ResponseStreamEvent
                ...
        return result.final_output
```

To publish raw model events to external subscribers, host a
`WorkflowStream` in the workflow and configure
`OpenAIAgentsPlugin(model_params=ModelActivityParameters(streaming_topic="events"))`. See [`temporalio.contrib.workflow_streams`](../workflow_streams/README.md) for the
publisher and subscriber API.

`RunResultStreaming.stream_events()` yields the agents-SDK
`StreamEvent` union (`RawResponsesStreamEvent`, `RunItemStreamEvent`,
`AgentUpdatedStreamEvent`); native OpenAI response events arrive
wrapped as `RawResponsesStreamEvent.data`. Workflow-stream subscribers,
by contrast, receive the unwrapped native events directly because the
streaming activity publishes them straight from `Model.stream_response`.

Streaming is incompatible with `use_local_activity` because local
activities support neither activity heartbeats nor the workflow stream
signal channel.

Activity retries surface to workflow-stream subscribers but not to
`RunResultStreaming.stream_events()`. Events are published to the
stream as `Model.stream_response` produces them, so a partial attempt
that fails mid-response leaves its emitted events on the stream and the
retry attempt publishes a second sequence. `stream_events()` only sees
the final successful attempt's collected events because it consumes the
activity's return value. Workflow-stream subscribers should treat
retries the same way as any other workflow_streams publisher — see
[Delivery semantics](../workflow_streams/README.md) for the trade and
the conventional `RETRY` event pattern for surfacing the transition to
consumers.

## Feature Support

This integration is presently subject to certain limitations.
Realtime agents are not supported. Streaming is supported via
`Runner.run_streamed` — see [Streaming](#streaming) above.
Certain tools are not suitable for a distributed computing environment, so these have been disabled as well.

### Model Providers

| Model Provider | Supported |
| :------------- | :-------: |
| OpenAI         |    Yes    |
| LiteLLM        |    Yes    |

### Model Response format

| Model Response | Supported            |
| :------------- | :------------------: |
| Get Response   |         Yes          |
| Streaming      | Yes (experimental)   |

### Tools

#### Tool Type

`LocalShellTool` and `ComputerTool` are not suited to a distributed computing setting.

| Tool Type           | Supported |
| :------------------ | :-------: |
| FunctionTool        |    Yes    |
| LocalShellTool      |    No     |
| WebSearchTool       |    Yes    |
| FileSearchTool      |    Yes    |
| HostedMCPTool       |    Yes    |
| ImageGenerationTool |    Yes    |
| CodeInterpreterTool |    Yes    |
| ShellTool           |    Yes    |
| ComputerTool        |    No     |

#### Tool Context

As described in [Tool Calling](#tool-calling), context propagation is read-only when Temporal activities are used as tools.

| Context Propagation                    | Supported |
| :------------------------------------- | :-------: |
| Activity Tool receives copy of context |    Yes    |
| Activity Tool can update context       |    No     |
| Function Tool received context         |    Yes    |
| Function Tool can update context       |    Yes    |

### MCP

The MCP protocol is stateful, but many MCP servers are stateless.
We let you choose between two MCP wrappers, one designed for stateless MCP servers and one for stateful MCP servers.
These wrappers work with all transport varieties.

Note that when using network-accessible MCP servers, you also can also use the tool `HostedMCPTool`, which is part of the OpenAI Responses API and uses an MCP client hosted by OpenAI.

| MCP Class               | Supported |
| :---------------------- | :-------: |
| MCPServerStdio          |    Yes    |
| MCPServerSse            |    Yes    |
| MCPServerStreamableHttp |    Yes    |

### Guardrails

| Guardrail Type | Supported |
| :------------- | :-------: |
| Code           |    Yes    |
| Agent          |    Yes    |

### Sessions

SQLite storage is not suited to a distributed environment.

| Feature       | Supported |
| :------------ | :-------: |
| SQLiteSession |    No     |

### Tracing

| Tracing Provider | Supported |
| :--------------- | :-------: |
| OpenAI platform  |    Yes    |

## OpenTelemetry Integration

⚠️ **Public Preview** - This functionality is subject to change prior to General Availability.

This integration provides seamless export of OpenAI agent telemetry to OpenTelemetry (OTEL) endpoints for observability and monitoring. The integration automatically handles workflow replay semantics, ensuring spans are only exported when workflows actually complete.

### Quick Start

To enable OTEL telemetry export, you need to set up a global `ReplaySafeTracerProvider` and enable the integration in the `OpenAIAgentsPlugin`:

```python
from datetime import timedelta
from temporalio.client import Client
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin, ModelActivityParameters
from temporalio.contrib.opentelemetry import create_tracer_provider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry import trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

# Configure your OTEL exporters

# Set up the global tracer provider
tracer_provider = create_tracer_provider()
tracer_provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317")))
trace.set_tracer_provider(tracer_provider)

client = await Client.connect(
    "localhost:7233",
    plugins=[
        OpenAIAgentsPlugin(
            use_otel_instrumentation=True,  # Enable OTEL integration
            model_params=ModelActivityParameters(
                start_to_close_timeout=timedelta(seconds=30)
            )
        ),
    ],
)
```

### Features

- **Multiple Exporters**: Send telemetry to multiple OTEL endpoints simultaneously via the global tracer provider
- **Replay-Safe**: Spans are only exported when workflows actually complete, not during replays
- **Deterministic IDs**: Consistent span IDs across workflow replays for reliable correlation
- **Automatic Setup**: No manual instrumentation required - just enable the flag and set up the global tracer provider
- **Graceful Degradation**: Works seamlessly whether OTEL dependencies are installed or not

### Dependencies

OTEL integration requires additional dependencies:

```bash
pip install openinference-instrumentation-openai-agents opentelemetry-sdk
```

Choose the appropriate OTEL exporter for your monitoring system:

```bash
# For OTLP (works with most OTEL collectors and monitoring systems)
pip install opentelemetry-exporter-otlp

# For Console output (development/debugging)
pip install opentelemetry-exporter-console

# Other exporters available for specific systems
pip install opentelemetry-exporter-<your-system>
```

### Example: Multiple Exporters

```python
from temporalio.contrib.opentelemetry import create_tracer_provider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.console import ConsoleSpanExporter
from opentelemetry import trace

exporters = [
    # Production monitoring system
    OTLPSpanExporter(
        endpoint="https://your-monitoring-system:4317",
        headers={"api-key": "your-api-key"}
    ),
    
    # Secondary monitoring endpoint
    OTLPSpanExporter(endpoint="https://backup-collector:4317"),
    
    # Development debugging
    ConsoleSpanExporter(),
]

# Set up global tracer provider with multiple exporters
tracer_provider = create_tracer_provider(exporters=exporters)
trace.set_tracer_provider(tracer_provider)

plugin = OpenAIAgentsPlugin(use_otel_instrumentation=True)
```

### Error Handling

If you enable OTEL instrumentation but the required dependencies are not installed, you'll receive a clear error message:

```
ImportError: OTEL dependencies not available. Install with: pip install openinference-instrumentation-openai-agents opentelemetry-sdk
```

If you enable OTEL instrumentation but don't have a proper global tracer provider set up, you'll get:

```
ValueError: Global tracer provider must a ReplaySafeTracerProvider. Use temporalio.contrib.opentelemtry.create_trace_provider to create one.
```

### Direct OpenTelemetry API Calls in Workflows

When using direct OpenTelemetry API calls within workflows (e.g., `opentelemetry.trace.get_tracer(__name__).start_as_current_span()`), you need to ensure proper context bridging and sandbox configuration.

#### Sandbox Configuration

Workflows run in a sandbox that restricts module access. To use direct OTEL API calls, you must explicitly allow OpenTelemetry passthrough:

```python
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

# Configure worker with OpenTelemetry passthrough
worker = Worker(
    client,
    task_queue="my-task-queue",
    workflows=[MyWorkflow],
    workflow_runner=SandboxedWorkflowRunner(
        SandboxRestrictions.default.with_passthrough_modules("opentelemetry")
    )
)
```

#### Context Bridging Pattern

Direct OTEL spans must be created within an active OpenAI Agents SDK span to ensure proper parenting:

```python
import opentelemetry.trace
from agents import custom_span
from temporalio import workflow

@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Start an SDK span first to establish OTEL context bridge
        with custom_span("Workflow coordination"):
            # Now direct OTEL spans will be properly parented
            tracer = opentelemetry.trace.get_tracer(__name__)
            with tracer.start_as_current_span("Custom workflow span"):
                # Your workflow logic here
                result = await self.do_work()
                return result
```

#### Why This Pattern is Required

- **OpenInference instrumentation** bridges OpenAI Agents SDK spans to OpenTelemetry context
- **Direct OTEL API calls** without an active SDK span become root spans with no parent
- **SDK spans** (`custom_span()`) establish the context bridge that allows subsequent direct OTEL spans to inherit proper trace parenting

#### Complete Example

```python
import opentelemetry.trace
from agents import custom_span
from temporalio import workflow
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

@workflow.defn
class TracedWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Establish OTEL context with SDK span
        with custom_span("Main workflow"):
            # Create direct OTEL spans for fine-grained tracing
            tracer = opentelemetry.trace.get_tracer(__name__)
            
            with tracer.start_as_current_span("Data processing"):
                data = await self.process_data()
                
            with tracer.start_as_current_span("Business logic"):
                result = await self.execute_business_logic(data)
                
            return result

# Worker configuration
worker = Worker(
    client,
    task_queue="traced-workflows",
    workflows=[TracedWorkflow],
    workflow_runner=SandboxedWorkflowRunner(
        SandboxRestrictions.default.with_passthrough_modules("opentelemetry")
    )
)
```

This ensures your direct OTEL spans are properly parented within the trace hierarchy initiated by your client SDK traces.

### Client-Side Trace Initialization

You can also start an Agents SDK trace on the client side before executing a workflow. This is useful when you want the entire workflow execution to be part of a larger trace context:

```python
from agents import trace, custom_span
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin

# Set up the plugin with OTEL integration
plugin = OpenAIAgentsPlugin(use_otel_instrumentation=True)

# Client setup
client = await Client.connect(
    "localhost:7233",
    plugins=[plugin]
)

# Start a trace on the client side
with plugin.tracing_context():
    with trace("Customer support workflow"):
        with custom_span("Workflow execution"):
            # Execute workflow within the trace context
            result = await client.execute_workflow(
                CustomerSupportAgent.run,
                "Help me with my order",
                id="customer-support-123",
                task_queue="my-task-queue",
            )
            print(f"Result: {result}")
```

The `plugin.tracing_context()` is required when starting traces outside of a worker context. This ensures proper instrumentation setup and trace propagation into the workflow execution.

If OTEL instrumentation is not enabled, the integration works normally without any OTEL setup.

### Voice

| Mode                     | Supported     |
| :----------------------- | :-----------: |
| Voice agents (pipelines) | Yes [^voice]  |
| Realtime agents          |      No       |

[^voice]: `VoicePipeline` runs in your process and delegates the agent
    step (`VoiceWorkflowBase.run`) to a Temporal workflow that uses
    `Runner.run` or `Runner.run_streamed`. STT and TTS run outside
    Temporal; the agent loop is durable.

### Utilities

The REPL utility is not suitable for a distributed setting.

| Utility | Supported |
| :------ | :-------: |
| REPL    |    No     |

## Additional Examples

You can find additional examples in the [Temporal Python Samples Repository](https://github.com/temporalio/samples-python/tree/main/openai_agents).
