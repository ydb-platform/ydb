"""Schemas for the LangSmith API."""

from __future__ import annotations

import functools
import json
import logging
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Optional, Union, cast
from uuid import UUID

from typing_extensions import TypedDict

from langsmith._internal._uuid import uuid7, warn_if_not_uuid_v7
from langsmith.uuid import uuid7_from_datetime

try:
    from pydantic.v1 import Field, root_validator  # type: ignore[import]
except ImportError:
    from pydantic import (  # type: ignore[assignment, no-redef]
        Field,
        root_validator,
    )

import contextvars
import threading
import urllib.parse

import langsmith._internal._context as _context
from langsmith import schemas as ls_schemas
from langsmith import utils
from langsmith.client import ID_TYPE, RUN_TYPE_T, Client, _dumps_json, _ensure_uuid

logger = logging.getLogger(__name__)


class WriteReplica(TypedDict, total=False):
    api_url: Optional[str]
    api_key: Optional[str]
    project_name: Optional[str]
    updates: Optional[dict]


LANGSMITH_PREFIX = "langsmith-"
LANGSMITH_DOTTED_ORDER = sys.intern(f"{LANGSMITH_PREFIX}trace")
LANGSMITH_DOTTED_ORDER_BYTES = LANGSMITH_DOTTED_ORDER.encode("utf-8")
LANGSMITH_METADATA = sys.intern(f"{LANGSMITH_PREFIX}metadata")
LANGSMITH_TAGS = sys.intern(f"{LANGSMITH_PREFIX}tags")
LANGSMITH_PROJECT = sys.intern(f"{LANGSMITH_PREFIX}project")
LANGSMITH_REPLICAS = sys.intern(f"{LANGSMITH_PREFIX}replicas")
OVERRIDE_OUTPUTS = sys.intern("__omit_auto_outputs")
NOT_PROVIDED = cast(None, object())
_LOCK = threading.Lock()

# Context variables
_REPLICAS = contextvars.ContextVar[Optional[Sequence[WriteReplica]]](
    "_REPLICAS", default=None
)

_DISTRIBUTED_PARENT_ID = contextvars.ContextVar[Optional[str]](
    "_DISTRIBUTED_PARENT_ID", default=None
)

_SENTINEL = cast(None, object())

TIMESTAMP_LENGTH = 36


# Note, this is called directly by langchain. Do not remove.
def get_cached_client(**init_kwargs: Any) -> Client:
    global _CLIENT
    if _CLIENT is None:
        with _LOCK:
            if _CLIENT is None:
                _CLIENT = Client(**init_kwargs)
    return _CLIENT


def configure(
    client: Optional[Client] = _SENTINEL,
    enabled: Optional[bool] = _SENTINEL,
    project_name: Optional[str] = _SENTINEL,
    tags: Optional[list[str]] = _SENTINEL,
    metadata: Optional[dict[str, Any]] = _SENTINEL,
):
    """Configure global LangSmith tracing context.

    This function allows you to set global configuration options for LangSmith
    tracing that will be applied to all subsequent traced operations. It modifies
    context variables that control tracing behavior across your application.

    Do this once at startup to configure the global settings in code.

    If, instead, you wish to only configure tracing for a single invocation,
    use the `tracing_context` context manager instead.

    Args:
        client: A LangSmith Client instance to use for all tracing operations.
            If provided, this client will be used instead of creating new clients.
            Pass `None` to explicitly clear the global client.
        enabled: Whether tracing is enabled. Can be:
            - `True`: Enable tracing and send data to LangSmith
            - `False`: Disable tracing completely
            - `"local"`: Enable tracing but only store data locally
            - `None`: Clear the setting (falls back to environment variables)
        project_name: The LangSmith project name where traces will be sent.
            This determines which project dashboard will display your traces.
            Pass `None` to explicitly clear the project name.
        tags: A list of tags to be applied to all traced runs. Tags are useful
            for filtering and organizing runs in the LangSmith UI.
            Pass `None` to explicitly clear all global tags.
        metadata: A dictionary of metadata to attach to all traced runs.
            Metadata can store any additional context about your runs.
            Pass `None` to explicitly clear all global metadata.

    Returns:
        None

    Examples:
        Basic configuration:
        >>> import langsmith as ls
        >>> # Enable tracing with a specific project
        >>> ls.configure(enabled=True, project_name="my-project")

        Set global trace masking:
        >>> def hide_keys(data):
        ...     if not data:
        ...         return {}
        ...     return {k: v for k, v in data.items() if k not in ["key1", "key2"]}
        >>> ls.configure(
        ...     client=ls.Client(
        ...         hide_inputs=hide_keys,
        ...         hide_outputs=hide_keys,
        ...     )
        ... )

        Adding global tags and metadata:
        >>> ls.configure(
        ...     tags=["production", "v1.0"],
        ...     metadata={"environment": "prod", "version": "1.0.0"},
        ... )

        Disabling tracing:
        >>> ls.configure(enabled=False)
    """
    global _CLIENT
    with _LOCK:
        if client is not _SENTINEL:
            _CLIENT = client
        if enabled is not _SENTINEL:
            _context._TRACING_ENABLED.set(enabled)
            _context._GLOBAL_TRACING_ENABLED = enabled
        if project_name is not _SENTINEL:
            _context._PROJECT_NAME.set(project_name)
            _context._GLOBAL_PROJECT_NAME = project_name
        if tags is not _SENTINEL:
            _context._TAGS.set(tags)
            _context._GLOBAL_TAGS = tags
        if metadata is not _SENTINEL:
            _context._METADATA.set(metadata)
            _context._GLOBAL_METADATA = metadata


def validate_extracted_usage_metadata(
    data: ls_schemas.ExtractedUsageMetadata,
) -> ls_schemas.ExtractedUsageMetadata:
    """Validate that the dict only contains allowed keys."""
    allowed_keys = {
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "input_token_details",
        "output_token_details",
        "input_cost",
        "output_cost",
        "total_cost",
        "input_cost_details",
        "output_cost_details",
    }

    extra_keys = set(data.keys()) - allowed_keys
    if extra_keys:
        raise ValueError(f"Unexpected keys in usage metadata: {extra_keys}")
    return data  # type: ignore


class RunTree(ls_schemas.RunBase):
    """Run Schema with back-references for posting runs."""

    name: str
    id: UUID = Field(default_factory=uuid7)
    run_type: str = Field(default="chain")
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    # Note: no longer set.
    parent_run: Optional[RunTree] = Field(default=None, exclude=True)
    parent_dotted_order: Optional[str] = Field(default=None, exclude=True)
    child_runs: list[RunTree] = Field(
        default_factory=list,
        exclude={"__all__": {"parent_run_id"}},
    )
    session_name: str = Field(
        default_factory=lambda: utils.get_tracer_project() or "default",
        alias="project_name",
    )
    session_id: Optional[UUID] = Field(default=None, alias="project_id")
    extra: dict = Field(default_factory=dict)
    tags: Optional[list[str]] = Field(default_factory=list)
    events: list[dict] = Field(default_factory=list)
    """List of events associated with the run, like
    start and end events."""
    ls_client: Optional[Any] = Field(default=None, exclude=True)
    dotted_order: str = Field(
        default="", description="The order of the run in the tree."
    )
    trace_id: UUID = Field(default="", description="The trace id of the run.")  # type: ignore
    dangerously_allow_filesystem: Optional[bool] = Field(
        default=False, description="Whether to allow filesystem access for attachments."
    )
    replicas: Optional[Sequence[WriteReplica]] = Field(
        default=None,
        description="Projects to replicate this run to with optional updates.",
    )

    class Config:
        """Pydantic model configuration."""

        arbitrary_types_allowed = True
        allow_population_by_field_name = True
        extra = "ignore"

    @root_validator(pre=True)
    def infer_defaults(cls, values: dict) -> dict:
        """Assign name to the run."""
        if values.get("name") is None and values.get("serialized") is not None:
            if "name" in values["serialized"]:
                values["name"] = values["serialized"]["name"]
            elif "id" in values["serialized"]:
                values["name"] = values["serialized"]["id"][-1]
        if values.get("name") is None:
            values["name"] = "Unnamed"
        if "client" in values:  # Handle user-constructed clients
            values["ls_client"] = values.pop("client")
        elif "_client" in values:
            values["ls_client"] = values.pop("_client")
        if not values.get("ls_client"):
            values["ls_client"] = None
        parent_run = values.pop("parent_run", None)
        if parent_run is not None:
            values["parent_run_id"] = parent_run.id
            values["parent_dotted_order"] = parent_run.dotted_order
        if "id" not in values:
            # Generate UUID from start_time if available
            if "start_time" in values and values["start_time"] is not None:
                values["id"] = uuid7_from_datetime(values["start_time"])
            else:
                values["id"] = uuid7()
        else:
            warn_if_not_uuid_v7(values["id"], "run_id")
        if "trace_id" not in values:
            if parent_run is not None:
                values["trace_id"] = parent_run.trace_id
            else:
                values["trace_id"] = values["id"]
        else:
            warn_if_not_uuid_v7(values["trace_id"], "trace_id")
        if values.get("parent_run_id"):
            warn_if_not_uuid_v7(values["parent_run_id"], "parent_run_id")
        cast(dict, values.setdefault("extra", {}))
        if values.get("events") is None:
            values["events"] = []
        if values.get("tags") is None:
            values["tags"] = []
        if values.get("outputs") is None:
            values["outputs"] = {}
        if values.get("attachments") is None:
            values["attachments"] = {}
        if values.get("replicas") is None:
            values["replicas"] = _REPLICAS.get()
        values["replicas"] = _ensure_write_replicas(values["replicas"])
        return values

    @root_validator(pre=False)
    def ensure_dotted_order(cls, values: dict) -> dict:
        """Ensure the dotted order of the run."""
        current_dotted_order = values.get("dotted_order")
        if current_dotted_order and current_dotted_order.strip():
            return values
        current_dotted_order = _create_current_dotted_order(
            values["start_time"], values["id"]
        )
        parent_dotted_order = values.get("parent_dotted_order")
        if parent_dotted_order is not None:
            values["dotted_order"] = parent_dotted_order + "." + current_dotted_order
        else:
            values["dotted_order"] = current_dotted_order
        return values

    @property
    def client(self) -> Client:
        """Return the client."""
        # Lazily load the client
        # If you never use this for API calls, it will never be loaded
        if self.ls_client is None:
            self.ls_client = get_cached_client()
        return self.ls_client

    @property
    def _client(self) -> Optional[Client]:
        # For backwards compat
        return self.ls_client

    def __setattr__(self, name, value):
        """Set the _client specially."""
        # For backwards compat
        if name == "_client":
            self.ls_client = value
        else:
            return super().__setattr__(name, value)

    def set(
        self,
        *,
        inputs: Optional[Mapping[str, Any]] = NOT_PROVIDED,
        outputs: Optional[Mapping[str, Any]] = NOT_PROVIDED,
        tags: Optional[Sequence[str]] = NOT_PROVIDED,
        metadata: Optional[Mapping[str, Any]] = NOT_PROVIDED,
        usage_metadata: Optional[ls_schemas.ExtractedUsageMetadata] = NOT_PROVIDED,
    ) -> None:
        """Set the inputs, outputs, tags, and metadata of the run.

        If performed, this will override the default behavior of the
        end() method to ignore new outputs (that would otherwise be added)
        by the @traceable decorator.

        If your LangChain or LangGraph versions are sufficiently up-to-date,
        this will also override the default behavior of LangChainTracer.

        Args:
            inputs: The inputs to set.
            outputs: The outputs to set.
            tags: The tags to set.
            metadata: The metadata to set.
            usage_metadata: Usage information to set.

        Returns:
            None
        """
        if tags is not NOT_PROVIDED:
            self.tags = list(tags)
        if metadata is not NOT_PROVIDED:
            self.extra.setdefault("metadata", {}).update(metadata or {})
        if inputs is not NOT_PROVIDED:
            # Used by LangChain core to determine whether to
            # re-upload the inputs upon run completion
            self.extra["inputs_is_truthy"] = False
            if inputs is None:
                self.inputs = {}
            else:
                self.inputs = dict(inputs)
        if outputs is not NOT_PROVIDED:
            self.extra[OVERRIDE_OUTPUTS] = True
            if outputs is None:
                self.outputs = {}
            else:
                self.outputs = dict(outputs)
        if usage_metadata is not NOT_PROVIDED:
            self.extra.setdefault("metadata", {})["usage_metadata"] = (
                validate_extracted_usage_metadata(usage_metadata)
            )

    def add_tags(self, tags: Union[Sequence[str], str]) -> None:
        """Add tags to the run."""
        if isinstance(tags, str):
            tags = [tags]
        if self.tags is None:
            self.tags = []
        self.tags.extend(tags)

    def add_metadata(self, metadata: dict[str, Any]) -> None:
        """Add metadata to the run."""
        if self.extra is None:
            self.extra = {}
        metadata_: dict = cast(dict, self.extra).setdefault("metadata", {})
        metadata_.update(metadata)

    def add_outputs(self, outputs: dict[str, Any]) -> None:
        """Upsert the given outputs into the run.

        Args:
            outputs (Dict[str, Any]): A dictionary containing the outputs to be added.

        Returns:
            None
        """
        if self.outputs is None:
            self.outputs = {}
        self.outputs.update(outputs)

    def add_inputs(self, inputs: dict[str, Any]) -> None:
        """Upsert the given outputs into the run.

        Args:
            outputs (Dict[str, Any]): A dictionary containing the outputs to be added.

        Returns:
            None
        """
        if self.inputs is None:
            self.inputs = {}
        self.inputs.update(inputs)
        # Set to False so LangChain things it needs to
        # re-upload inputs
        self.extra["inputs_is_truthy"] = False

    def add_event(
        self,
        events: Union[
            ls_schemas.RunEvent,
            Sequence[ls_schemas.RunEvent],
            Sequence[dict],
            dict,
            str,
        ],
    ) -> None:
        """Add an event to the list of events.

        Args:
            events (Union[ls_schemas.RunEvent, Sequence[ls_schemas.RunEvent],
                    Sequence[dict], dict, str]):
                The event(s) to be added. It can be a single event, a sequence
                of events, a sequence of dictionaries, a dictionary, or a string.

        Returns:
            None
        """
        if self.events is None:
            self.events = []
        if isinstance(events, dict):
            self.events.append(events)  # type: ignore[arg-type]
        elif isinstance(events, str):
            self.events.append(
                {
                    "name": "event",
                    "time": datetime.now(timezone.utc).isoformat(),
                    "message": events,
                }
            )
        else:
            self.events.extend(events)  # type: ignore[arg-type]

    def end(
        self,
        *,
        outputs: Optional[dict] = None,
        error: Optional[str] = None,
        end_time: Optional[datetime] = None,
        events: Optional[Sequence[ls_schemas.RunEvent]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Set the end time of the run and all child runs."""
        self.end_time = end_time or datetime.now(timezone.utc)
        # We've already 'set' the outputs, so ignore
        # the ones that are automatically included
        if not self.extra.get(OVERRIDE_OUTPUTS):
            if outputs is not None:
                if not self.outputs:
                    self.outputs = outputs
                else:
                    self.outputs.update(outputs)
        if error is not None:
            self.error = error
        if events is not None:
            self.add_event(events)
        if metadata is not None:
            self.add_metadata(metadata)

    def create_child(
        self,
        name: str,
        run_type: RUN_TYPE_T = "chain",
        *,
        run_id: Optional[ID_TYPE] = None,
        serialized: Optional[dict] = None,
        inputs: Optional[dict] = None,
        outputs: Optional[dict] = None,
        error: Optional[str] = None,
        reference_example_id: Optional[UUID] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        tags: Optional[list[str]] = None,
        extra: Optional[dict] = None,
        attachments: Optional[ls_schemas.Attachments] = None,
    ) -> RunTree:
        """Add a child run to the run tree."""
        serialized_ = serialized or {"name": name}
        run = RunTree(
            name=name,
            id=_ensure_uuid(run_id),
            serialized=serialized_,
            inputs=inputs or {},
            outputs=outputs or {},
            error=error,
            run_type=run_type,
            reference_example_id=reference_example_id,
            start_time=start_time or datetime.now(timezone.utc),
            end_time=end_time,
            extra=extra or {},
            parent_run=self,
            project_name=self.session_name,
            replicas=self.replicas,
            ls_client=self.ls_client,
            tags=tags,
            attachments=attachments or {},  # type: ignore
            dangerously_allow_filesystem=self.dangerously_allow_filesystem,
        )

        return run

    def _get_dicts_safe(self):
        # Things like generators cannot be copied
        self_dict = self.dict(
            exclude={"child_runs", "inputs", "outputs"}, exclude_none=True
        )
        if self.inputs is not None:
            # shallow copy. deep copying will occur in the client
            self_dict["inputs"] = self.inputs.copy()
        if self.outputs is not None:
            # shallow copy; deep copying will occur in the client
            self_dict["outputs"] = self.outputs.copy()
        return self_dict

    def _slice_parent_id(self, parent_id: str, run_dict: dict) -> None:
        """Slice the parent id from dotted order.

        Additionally check if the current run is a child of the parent. If so, update
        the parent_run_id to None, and set the trace id to the new root id after
        parent_id.
        """
        if dotted_order := run_dict.get("dotted_order"):
            segs = dotted_order.split(".")
            start_idx = None
            parent_id = str(parent_id)
            # TODO(angus): potentially use binary search to find the index
            for idx, part in enumerate(segs):
                seg_id = part[-TIMESTAMP_LENGTH:]
                if str(seg_id) == parent_id:
                    start_idx = idx
                    break
            if start_idx is not None:
                # Trim segments to start after parent_id (exclusive)
                trimmed_segs = segs[start_idx + 1 :]
                # Rebuild dotted_order
                run_dict["dotted_order"] = ".".join(trimmed_segs)
                if trimmed_segs:
                    run_dict["trace_id"] = UUID(trimmed_segs[0][-TIMESTAMP_LENGTH:])
                else:
                    run_dict["trace_id"] = run_dict["id"]
        if str(run_dict.get("parent_run_id")) == parent_id:
            # We've found the new root node.
            run_dict.pop("parent_run_id", None)

    def _remap_for_project(
        self, project_name: str, updates: Optional[dict] = None
    ) -> dict:
        """Get run dict for a given project with optional updates."""
        run_dict = self._get_dicts_safe()

        if updates and updates.get("reroot", False):
            distributed_parent_id = _DISTRIBUTED_PARENT_ID.get()
            if distributed_parent_id:
                self._slice_parent_id(distributed_parent_id, run_dict)

        dup = utils.deepish_copy(run_dict)
        dup["session_name"] = project_name

        if updates:
            dup.update(updates)
        return dup

    def post(self, exclude_child_runs: bool = True) -> None:
        """Post the run tree to the API asynchronously."""
        if self.replicas:
            for replica in self.replicas:
                project_name = replica.get("project_name") or self.session_name
                updates = replica.get("updates")
                run_dict = self._remap_for_project(project_name, updates)
                self.client.create_run(
                    **run_dict,
                    api_key=replica.get("api_key"),
                    api_url=replica.get("api_url"),
                )
        else:
            kwargs = self._get_dicts_safe()
            self.client.create_run(**kwargs)
        if self.attachments:
            keys = [str(name) for name in self.attachments]
            self.events.append(
                {
                    "name": "uploaded_attachment",
                    "time": datetime.now(timezone.utc).isoformat(),
                    "message": set(keys),
                }
            )
        if not exclude_child_runs:
            for child_run in self.child_runs:
                child_run.post(exclude_child_runs=False)

    def patch(self, *, exclude_inputs: bool = False) -> None:
        """Patch the run tree to the API in a background thread.

        Args:
            exclude_inputs: whether to exclude inputs from the patch request.
        """
        if not self.end_time:
            self.end()
        attachments = {
            a: v for a, v in self.attachments.items() if isinstance(v, tuple)
        }
        try:
            # Avoid loading the same attachment twice
            if attachments:
                uploaded = next(
                    (
                        ev
                        for ev in self.events
                        if ev.get("name") == "uploaded_attachment"
                    ),
                    None,
                )
                if uploaded:
                    attachments = {
                        a: v
                        for a, v in attachments.items()
                        if a not in uploaded["message"]
                    }
        except Exception as e:
            logger.warning(f"Error filtering attachments to upload: {e}")
        if self.replicas:
            for replica in self.replicas:
                project_name = replica.get("project_name") or self.session_name
                updates = replica.get("updates")
                run_dict = self._remap_for_project(project_name, updates)
                self.client.update_run(
                    name=run_dict["name"],
                    run_id=run_dict["id"],
                    run_type=run_dict.get("run_type"),
                    start_time=run_dict.get("start_time"),
                    inputs=None if exclude_inputs else run_dict["inputs"],
                    outputs=run_dict["outputs"],
                    error=run_dict.get("error"),
                    parent_run_id=run_dict.get("parent_run_id"),
                    session_name=run_dict.get("session_name"),
                    reference_example_id=run_dict.get("reference_example_id"),
                    end_time=run_dict.get("end_time"),
                    dotted_order=run_dict.get("dotted_order"),
                    trace_id=run_dict.get("trace_id"),
                    events=run_dict.get("events"),
                    tags=run_dict.get("tags"),
                    extra=run_dict.get("extra"),
                    attachments=attachments,
                    api_key=replica.get("api_key"),
                    api_url=replica.get("api_url"),
                )
        else:
            self.client.update_run(
                name=self.name,
                run_id=self.id,
                run_type=cast(RUN_TYPE_T, self.run_type),
                start_time=self.start_time,
                inputs=(
                    None
                    if exclude_inputs
                    else (self.inputs.copy() if self.inputs else None)
                ),
                outputs=self.outputs.copy() if self.outputs else None,
                error=self.error,
                parent_run_id=self.parent_run_id,
                session_name=self.session_name,
                reference_example_id=self.reference_example_id,
                end_time=self.end_time,
                dotted_order=self.dotted_order,
                trace_id=self.trace_id,
                events=self.events,
                tags=self.tags,
                extra=self.extra,
                attachments=attachments,
            )

    def wait(self) -> None:
        """Wait for all _futures to complete."""
        pass

    def get_url(self) -> str:
        """Return the URL of the run."""
        return self.client.get_run_url(run=self)

    @classmethod
    def from_dotted_order(
        cls,
        dotted_order: str,
        **kwargs: Any,
    ) -> RunTree:
        """Create a new 'child' span from the provided dotted order.

        Returns:
            RunTree: The new span.
        """
        headers = {
            LANGSMITH_DOTTED_ORDER: dotted_order,
        }
        return cast(RunTree, cls.from_headers(headers, **kwargs))  # type: ignore[arg-type]

    @classmethod
    def from_runnable_config(
        cls,
        config: Optional[dict],
        **kwargs: Any,
    ) -> Optional[RunTree]:
        """Create a new 'child' span from the provided runnable config.

        Requires langchain to be installed.

        Returns:
            Optional[RunTree]: The new span or None if
                no parent span information is found.
        """
        try:
            from langchain_core.callbacks.manager import (
                AsyncCallbackManager,
                CallbackManager,
            )
            from langchain_core.runnables import RunnableConfig, ensure_config
            from langchain_core.tracers.langchain import LangChainTracer
        except ImportError as e:
            raise ImportError(
                "RunTree.from_runnable_config requires langchain-core to be installed. "
                "You can install it with `pip install langchain-core`."
            ) from e
        if config is None:
            config_ = ensure_config(
                cast(RunnableConfig, config) if isinstance(config, dict) else None
            )
        else:
            config_ = cast(RunnableConfig, config)

        if (
            (cb := config_.get("callbacks"))
            and isinstance(cb, (CallbackManager, AsyncCallbackManager))
            and cb.parent_run_id
            and (
                tracer := next(
                    (t for t in cb.handlers if isinstance(t, LangChainTracer)),
                    None,
                )
            )
        ):
            if (run := tracer.run_map.get(str(cb.parent_run_id))) and run.dotted_order:
                dotted_order = run.dotted_order
                kwargs["run_type"] = run.run_type
                kwargs["inputs"] = run.inputs
                kwargs["outputs"] = run.outputs
                kwargs["start_time"] = run.start_time
                kwargs["end_time"] = run.end_time
                kwargs["tags"] = sorted(set(run.tags or [] + kwargs.get("tags", [])))
                kwargs["name"] = run.name
                extra_ = kwargs.setdefault("extra", {})
                metadata_ = extra_.setdefault("metadata", {})
                metadata_.update(run.metadata)
            elif hasattr(tracer, "order_map") and cb.parent_run_id in tracer.order_map:
                dotted_order = tracer.order_map[cb.parent_run_id][1]
            else:
                return None
            kwargs["client"] = tracer.client
            kwargs["project_name"] = tracer.project_name
            return RunTree.from_dotted_order(dotted_order, **kwargs)
        return None

    @classmethod
    def from_headers(
        cls, headers: Mapping[Union[str, bytes], Union[str, bytes]], **kwargs: Any
    ) -> Optional[RunTree]:
        """Create a new 'parent' span from the provided headers.

        Extracts parent span information from the headers and creates a new span.
        Metadata and tags are extracted from the baggage header.
        The dotted order and trace id are extracted from the trace header.

        Returns:
            Optional[RunTree]: The new span or None if
                no parent span information is found.
        """
        init_args = kwargs.copy()

        langsmith_trace = cast(Optional[str], headers.get(LANGSMITH_DOTTED_ORDER))
        if not langsmith_trace:
            langsmith_trace_bytes = cast(
                Optional[bytes], headers.get(LANGSMITH_DOTTED_ORDER_BYTES)
            )
            if not langsmith_trace_bytes:
                return  # type: ignore[return-value]
            langsmith_trace = langsmith_trace_bytes.decode("utf-8")

        parent_dotted_order = langsmith_trace.strip()
        parsed_dotted_order = _parse_dotted_order(parent_dotted_order)
        trace_id = parsed_dotted_order[0][1]
        init_args["trace_id"] = trace_id
        init_args["id"] = parsed_dotted_order[-1][1]
        init_args["dotted_order"] = parent_dotted_order
        if len(parsed_dotted_order) >= 2:
            # Has a parent
            init_args["parent_run_id"] = parsed_dotted_order[-2][1]
        # All placeholders. We assume the source process
        # handles the life-cycle of the run.
        init_args["start_time"] = init_args.get("start_time") or datetime.now(
            timezone.utc
        )
        init_args["run_type"] = init_args.get("run_type") or "chain"
        init_args["name"] = init_args.get("name") or "parent"

        baggage = _Baggage.from_headers(headers)
        if baggage.metadata or baggage.tags:
            init_args["extra"] = init_args.setdefault("extra", {})
            init_args["extra"]["metadata"] = init_args["extra"].setdefault(
                "metadata", {}
            )
            metadata = {**baggage.metadata, **init_args["extra"]["metadata"]}
            init_args["extra"]["metadata"] = metadata
            tags = sorted(set(baggage.tags + init_args.get("tags", [])))
            init_args["tags"] = tags
        if baggage.project_name:
            init_args["project_name"] = baggage.project_name
        if baggage.replicas:
            init_args["replicas"] = baggage.replicas

        run_tree = RunTree(**init_args)

        # Set the distributed parent ID to this run's ID for rerooting
        _DISTRIBUTED_PARENT_ID.set(str(run_tree.id))

        return run_tree

    def to_headers(self) -> dict[str, str]:
        """Return the RunTree as a dictionary of headers."""
        headers = {}
        if self.trace_id:
            headers[f"{LANGSMITH_DOTTED_ORDER}"] = self.dotted_order
        baggage = _Baggage(
            metadata=self.extra.get("metadata", {}),
            tags=self.tags,
            project_name=self.session_name,
            replicas=self.replicas,
        )
        headers["baggage"] = baggage.to_header()
        return headers

    def __repr__(self):
        """Return a string representation of the RunTree object."""
        return (
            f"RunTree(id={self.id}, name='{self.name}', "
            f"run_type='{self.run_type}', dotted_order='{self.dotted_order}')"
        )


class _Baggage:
    """Baggage header information."""

    def __init__(
        self,
        metadata: Optional[dict[str, str]] = None,
        tags: Optional[list[str]] = None,
        project_name: Optional[str] = None,
        replicas: Optional[Sequence[WriteReplica]] = None,
    ):
        """Initialize the Baggage object."""
        self.metadata = metadata or {}
        self.tags = tags or []
        self.project_name = project_name
        self.replicas = replicas or []

    @classmethod
    def from_header(cls, header_value: Optional[str]) -> _Baggage:
        """Create a Baggage object from the given header value."""
        if not header_value:
            return cls()
        metadata = {}
        tags = []
        project_name = None
        replicas: Optional[list[WriteReplica]] = None
        try:
            for item in header_value.split(","):
                key, value = item.split("=", 1)
                if key == LANGSMITH_METADATA:
                    metadata = json.loads(urllib.parse.unquote(value))
                elif key == LANGSMITH_TAGS:
                    tags = urllib.parse.unquote(value).split(",")
                elif key == LANGSMITH_PROJECT:
                    project_name = urllib.parse.unquote(value)
                elif key == LANGSMITH_REPLICAS:
                    replicas_data = json.loads(urllib.parse.unquote(value))
                    parsed_replicas: list[WriteReplica] = []
                    for replica_item in replicas_data:
                        if (
                            isinstance(replica_item, (tuple, list))
                            and len(replica_item) == 2
                        ):
                            # Convert legacy format to WriteReplica
                            parsed_replicas.append(
                                WriteReplica(
                                    api_url=None,
                                    api_key=None,
                                    project_name=str(replica_item[0]),
                                    updates=replica_item[1],
                                )
                            )
                        elif isinstance(replica_item, dict):
                            # New WriteReplica format: preserve as dict
                            parsed_replicas.append(cast(WriteReplica, replica_item))
                        else:
                            logger.warning(
                                f"Unknown replica format in baggage: {replica_item}"
                            )
                            continue
                    replicas = parsed_replicas
        except Exception as e:
            logger.warning(f"Error parsing baggage header: {e}")

        return cls(
            metadata=metadata, tags=tags, project_name=project_name, replicas=replicas
        )

    @classmethod
    def from_headers(cls, headers: Mapping[Union[str, bytes], Any]) -> _Baggage:
        if "baggage" in headers:
            return cls.from_header(headers["baggage"])
        elif b"baggage" in headers:
            return cls.from_header(cast(bytes, headers[b"baggage"]).decode("utf-8"))
        else:
            return cls.from_header(None)

    def to_header(self) -> str:
        """Return the Baggage object as a header value."""
        items = []
        if self.metadata:
            serialized_metadata = _dumps_json(self.metadata)
            items.append(
                f"{LANGSMITH_PREFIX}metadata={urllib.parse.quote(serialized_metadata)}"
            )
        if self.tags:
            serialized_tags = ",".join(self.tags)
            items.append(
                f"{LANGSMITH_PREFIX}tags={urllib.parse.quote(serialized_tags)}"
            )
        if self.project_name:
            items.append(
                f"{LANGSMITH_PREFIX}project={urllib.parse.quote(self.project_name)}"
            )
        if self.replicas:
            serialized_replicas = _dumps_json(self.replicas)
            items.append(
                f"{LANGSMITH_PREFIX}replicas={urllib.parse.quote(serialized_replicas)}"
            )
        return ",".join(items)


@functools.lru_cache(maxsize=1)
def _parse_write_replicas_from_env_var(env_var: Optional[str]) -> list[WriteReplica]:
    """Parse write replicas from LANGSMITH_RUNS_ENDPOINTS environment variable value.

    Supports array format [{"api_url": "x", "api_key": "y"}] and object format
    {"url": "key"}.
    """
    if not env_var:
        return []

    try:
        parsed = json.loads(env_var)

        if isinstance(parsed, list):
            replicas = []
            for item in parsed:
                if not isinstance(item, dict):
                    logger.warning(
                        f"Invalid item type in LANGSMITH_RUNS_ENDPOINTS: "
                        f"expected dict, got {type(item).__name__}"
                    )
                    continue

                api_url = item.get("api_url")
                api_key = item.get("api_key")

                if not isinstance(api_url, str):
                    logger.warning(
                        f"Invalid api_url type in LANGSMITH_RUNS_ENDPOINTS: "
                        f"expected string, got {type(api_url).__name__}"
                    )
                    continue

                if not isinstance(api_key, str):
                    logger.warning(
                        f"Invalid api_key type in LANGSMITH_RUNS_ENDPOINTS: "
                        f"expected string, got {type(api_key).__name__}"
                    )
                    continue

                replicas.append(
                    WriteReplica(
                        api_url=api_url.rstrip("/"),
                        api_key=api_key,
                        project_name=None,
                        updates=None,
                    )
                )
            return replicas
        elif isinstance(parsed, dict):
            _check_endpoint_env_unset(parsed)

            replicas = []
            for url, key in parsed.items():
                url = url.rstrip("/")

                if isinstance(key, str):
                    replicas.append(
                        WriteReplica(
                            api_url=url,
                            api_key=key,
                            project_name=None,
                            updates=None,
                        )
                    )
                else:
                    logger.warning(
                        f"Invalid value type in LANGSMITH_RUNS_ENDPOINTS for URL "
                        f"{url}: "
                        f"expected string, got {type(key).__name__}"
                    )
                    continue
            return replicas
        else:
            logger.warning(
                f"Invalid LANGSMITH_RUNS_ENDPOINTS – must be valid JSON list of "
                "objects with api_url and api_key properties, or object mapping "
                f"url->apiKey, got {type(parsed).__name__}"
            )
            return []
    except utils.LangSmithUserError:
        raise
    except Exception as e:
        logger.warning(
            "Invalid LANGSMITH_RUNS_ENDPOINTS – must be valid JSON list of "
            f"objects with api_url and api_key properties, or object mapping"
            f" url->apiKey: {e}"
        )
        return []


def _get_write_replicas_from_env() -> list[WriteReplica]:
    """Get write replicas from LANGSMITH_RUNS_ENDPOINTS environment variable."""
    env_var = utils.get_env_var("RUNS_ENDPOINTS")

    return _parse_write_replicas_from_env_var(env_var)


def _check_endpoint_env_unset(parsed: dict[str, str]) -> None:
    """Check if endpoint environment variables conflict with runs endpoints."""
    import os

    if parsed and (os.getenv("LANGSMITH_ENDPOINT") or os.getenv("LANGCHAIN_ENDPOINT")):
        raise utils.LangSmithUserError(
            "You cannot provide both LANGSMITH_ENDPOINT / LANGCHAIN_ENDPOINT "
            "and LANGSMITH_RUNS_ENDPOINTS."
        )


def _ensure_write_replicas(
    replicas: Optional[Sequence[WriteReplica]],
) -> list[WriteReplica]:
    """Convert replicas to WriteReplica format."""
    if replicas is None:
        return _get_write_replicas_from_env()

    # All replicas should now be WriteReplica dicts
    return list(replicas)


def _parse_dotted_order(dotted_order: str) -> list[tuple[datetime, UUID]]:
    """Parse the dotted order string."""
    parts = dotted_order.split(".")
    return [
        (
            datetime.strptime(part[:-TIMESTAMP_LENGTH], "%Y%m%dT%H%M%S%fZ"),
            UUID(part[-TIMESTAMP_LENGTH:]),
        )
        for part in parts
    ]


_CLIENT: Optional[Client] = _context._GLOBAL_CLIENT
__all__ = ["RunTree", "RunTree"]


def _create_current_dotted_order(
    start_time: Optional[datetime], run_id: Optional[UUID]
) -> str:
    """Create the current dotted order."""
    st = start_time or datetime.now(timezone.utc)
    id_ = run_id or uuid7_from_datetime(st)
    return st.strftime("%Y%m%dT%H%M%S%fZ") + str(id_)
