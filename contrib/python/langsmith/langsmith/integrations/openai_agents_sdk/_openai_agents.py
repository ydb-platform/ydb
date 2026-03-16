import logging
from datetime import datetime
from typing import Optional

from langsmith import run_trees as rt
from langsmith._internal import _context
from langsmith.run_helpers import get_current_run_tree

try:
    from agents import tracing  # type: ignore[import]

    required = (
        "TracingProcessor",
        "Trace",
        "Span",
        "ResponseSpanData",
    )
    if not all(hasattr(tracing, name) for name in required):
        raise ImportError("The `agents` package is not installed.")

    from langsmith.integrations.openai_agents_sdk import (
        _openai_agent_utils as agent_utils,
    )

    HAVE_AGENTS = True
except ImportError:
    HAVE_AGENTS = False

    class OpenAIAgentsTracingProcessor:
        """Tracing processor for the `OpenAI Agents SDK <https://openai.github.io/openai-agents-python/>`_.

        Traces all intermediate steps of your OpenAI Agent to LangSmith.

        Requirements: Make sure to install ``pip install -U langsmith[openai-agents]``.

        Args:
            client: An instance of langsmith.client.Client. If not provided,
                a default client is created.

        Example:
            .. code-block:: python

                from agents import (
                    Agent,
                    FileSearchTool,
                    Runner,
                    WebSearchTool,
                    function_tool,
                    set_trace_processors,
                )

                from langsmith.wrappers import OpenAIAgentsTracingProcessor

                set_trace_processors([OpenAIAgentsTracingProcessor()])


                @function_tool
                def get_weather(city: str) -> str:
                    return f"The weather in {city} is sunny"


                haiku_agent = Agent(
                    name="Haiku agent",
                    instructions="Always respond in haiku form",
                    model="o3-mini",
                    tools=[get_weather],
                )
                agent = Agent(
                    name="Assistant",
                    tools=[WebSearchTool()],
                    instructions="speak in spanish. use Haiku agent if they ask for a haiku or for the weather",
                    handoffs=[haiku_agent],
                )

                result = await Runner.run(
                    agent,
                    "write a haiku about the weather today and tell me a recent news story about new york",
                )
                print(result.final_output)
        """  # noqa: E501

        def __init__(self, *args, **kwargs):
            raise ImportError(
                "The `agents` package is not installed. "
                "Please install it with `pip install langsmith[openai-agents]`."
            )


from langsmith import client as ls_client

logger = logging.getLogger(__name__)

if HAVE_AGENTS:

    class OpenAIAgentsTracingProcessor(tracing.TracingProcessor):  # type: ignore[no-redef]
        """Tracing processor for the `OpenAI Agents SDK <https://openai.github.io/openai-agents-python/>`_.

        Traces all intermediate steps of your OpenAI Agent to LangSmith.

        Requirements: Make sure to install ``pip install -U langsmith[openai-agents]``.

        Args:
            client: An instance of langsmith.client.Client. If not provided,
                a default client is created.
            metadata: Metadata to associate with all traces.
            tags: Tags to associate with all traces.
            project_name: LangSmith project to trace to.
            name: Name of the root trace.

        Example:
            .. code-block:: python

                from agents import (
                    Agent,
                    FileSearchTool,
                    Runner,
                    WebSearchTool,
                    function_tool,
                    set_trace_processors,
                )

                from langsmith.wrappers import OpenAIAgentsTracingProcessor

                set_trace_processors([OpenAIAgentsTracingProcessor()])


                @function_tool
                def get_weather(city: str) -> str:
                    return f"The weather in {city} is sunny"


                haiku_agent = Agent(
                    name="Haiku agent",
                    instructions="Always respond in haiku form",
                    model="o3-mini",
                    tools=[get_weather],
                )
                agent = Agent(
                    name="Assistant",
                    tools=[WebSearchTool()],
                    instructions="speak in spanish. use Haiku agent if they ask for a haiku or for the weather",
                    handoffs=[haiku_agent],
                )

                result = await Runner.run(
                    agent,
                    "write a haiku about the weather today and tell me a recent news story about new york",
                )
                print(result.final_output)
        """  # noqa: E501

        def __init__(
            self,
            client: Optional[ls_client.Client] = None,
            *,
            metadata: Optional[dict] = None,
            tags: Optional[list[str]] = None,
            project_name: Optional[str] = None,
            name: Optional[str] = None,
        ):
            self.client = client or rt.get_cached_client()
            self._metadata = metadata
            self._tags = tags
            self._project_name = project_name
            self._name = name
            self._first_response_inputs: dict = {}
            self._last_response_outputs: dict = {}

            self._runs: dict[str, rt.RunTree] = {}

        def on_trace_start(self, trace: tracing.Trace) -> None:
            current_run_tree = get_current_run_tree()

            # Determine run name
            if self._name:
                run_name = self._name
            elif trace.name:
                run_name = trace.name
            else:
                run_name = "Agent workflow"

            # Build metadata
            run_extra = {"metadata": self._metadata or {}}
            trace_dict = trace.export() or {}
            if trace_dict.get("group_id") is not None:
                run_extra["metadata"]["thread_id"] = trace_dict["group_id"]

            try:
                if current_run_tree is not None:
                    # Nest under existing trace
                    new_run = current_run_tree.create_child(
                        name=run_name,
                        run_type="chain",
                        inputs={},
                        extra=run_extra,
                        tags=self._tags,
                    )
                else:
                    # Create new root trace
                    run_kwargs = {
                        "name": run_name,
                        "run_type": "chain",
                        "inputs": {},
                        "extra": run_extra,
                        "tags": self._tags,
                        "client": self.client,
                    }
                    if self._project_name is not None:
                        run_kwargs["project_name"] = self._project_name
                    new_run = rt.RunTree(**run_kwargs)  # type: ignore[arg-type]

                new_run.post()
                _context._PARENT_RUN_TREE.set(new_run)
                self._runs[trace.trace_id] = new_run
            except Exception as e:
                logger.exception(f"Error creating trace run: {e}")

        def on_trace_end(self, trace: tracing.Trace) -> None:
            run = self._runs.pop(trace.trace_id, None)
            if not run:
                return

            trace_dict = trace.export() or {}
            metadata = {**(trace_dict.get("metadata") or {}), **(self._metadata or {})}

            try:
                # Update run with final inputs/outputs
                run.inputs = self._first_response_inputs.pop(trace.trace_id, {})
                run.outputs = self._last_response_outputs.pop(trace.trace_id, {})

                # Update metadata
                if "metadata" not in run.extra:
                    run.extra["metadata"] = {}
                run.extra["metadata"].update(metadata)

                # End and patch
                run.end()
                run.patch()
            except Exception as e:
                logger.exception(f"Error updating trace run: {e}")

        def on_span_start(self, span: tracing.Span) -> None:
            # Find parent run
            parent_run = (
                self._runs.get(span.parent_id)
                if span.parent_id
                else self._runs.get(span.trace_id)
            )

            if parent_run is None:
                logger.warning(
                    f"No trace info found for span, skipping: {span.span_id}"
                )
                return

            # Extract span data
            run_name = agent_utils.get_run_name(span)
            if isinstance(span.span_data, tracing.ResponseSpanData):
                parent_name = parent_run.name
                raw_span_name = getattr(span, "name", None) or getattr(
                    span.span_data, "name", None
                )
                span_name = str(raw_span_name) if raw_span_name else run_name
                if parent_name:
                    run_name = f"{parent_name} {span_name}".strip()
                else:
                    run_name = span_name

            run_type = agent_utils.get_run_type(span)
            extracted = agent_utils.extract_span_data(span)

            try:
                # Create child run
                child_run = parent_run.create_child(
                    name=run_name,
                    run_type=run_type,
                    inputs=extracted.get("inputs", {}),
                    extra=extracted,
                    start_time=datetime.fromisoformat(span.started_at)
                    if span.started_at
                    else None,
                )

                child_run.post()
                _context._PARENT_RUN_TREE.set(child_run)
                self._runs[span.span_id] = child_run
            except Exception as e:
                logger.exception(f"Error creating span run: {e}")

        def on_span_end(self, span: tracing.Span) -> None:
            run = self._runs.pop(span.span_id, None)
            if not run:
                return

            try:
                # Extract outputs and metadata
                extracted = agent_utils.extract_span_data(span)
                outputs = extracted.pop("outputs", {})
                inputs = extracted.pop("inputs", {})

                # Update run
                run.outputs = outputs
                if inputs:
                    run.inputs = inputs
                if error := span.error:
                    run.error = str(error)

                # Add OpenAI metadata
                if "metadata" not in run.extra:
                    run.extra["metadata"] = {}
                run.extra["metadata"].update(
                    {
                        "openai_parent_id": span.parent_id,
                        "openai_trace_id": span.trace_id,
                        "openai_span_id": span.span_id,
                    }
                )
                # Merge any additional metadata from extracted
                if metadata := extracted.get("metadata"):
                    run.extra["metadata"].update(metadata)

                # Track first/last response inputs/outputs for trace
                if isinstance(span.span_data, tracing.ResponseSpanData):
                    self._first_response_inputs[span.trace_id] = (
                        self._first_response_inputs.get(span.trace_id) or inputs
                    )
                    self._last_response_outputs[span.trace_id] = outputs
                elif isinstance(span.span_data, tracing.GenerationSpanData):
                    # Use generation spans as fallback if no response spans exist
                    self._first_response_inputs[span.trace_id] = (
                        self._first_response_inputs.get(span.trace_id) or inputs
                    )
                    self._last_response_outputs[span.trace_id] = outputs

                # End and patch
                if span.ended_at:
                    run.end_time = datetime.fromisoformat(span.ended_at)
                else:
                    run.end()

                run.patch()
            except Exception as e:
                logger.exception(f"Error updating span run: {e}")

        def shutdown(self) -> None:
            self.client.flush()

        def force_flush(self) -> None:
            self.client.flush()
