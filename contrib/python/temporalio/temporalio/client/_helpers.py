"""Client support for accessing Temporal."""

from __future__ import annotations

import copy
import json
import re
from collections.abc import (
    Iterable,
    Mapping,
)
from typing import (
    Any,
)

import google.protobuf.json_format
from google.protobuf.internal.containers import MessageMap

import temporalio.api.common.v1
import temporalio.api.history.v1
import temporalio.api.sdk.v1
import temporalio.common
import temporalio.converter
from temporalio.converter import (
    DataConverter,
)


async def _apply_headers(  # pyright: ignore[reportUnusedFunction]
    source: Mapping[str, temporalio.api.common.v1.Payload] | None,
    dest: MessageMap[str, temporalio.api.common.v1.Payload],
    encode_headers: bool,
    data_converter: DataConverter,
) -> None:
    if source is None:
        return
    if encode_headers:
        for key, payload in source.items():
            dest[key].CopyFrom(
                await data_converter._transform_outbound_payload(payload)
            )
    else:
        temporalio.common._apply_headers(source, dest)


def _history_from_json(  # pyright: ignore[reportUnusedFunction]
    history: str | dict[str, Any],
) -> temporalio.api.history.v1.History:
    if isinstance(history, str):
        history = json.loads(history)
    else:
        # Copy the dict so we can mutate it
        history = copy.deepcopy(history)
    if not isinstance(history, dict):
        raise ValueError("JSON history not a dictionary")
    events = history.get("events")
    if not isinstance(events, Iterable):
        raise ValueError("History does not have iterable 'events'")
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("Event not a dictionary")
        _fix_history_enum(
            "CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE",
            event,
            "requestCancelExternalWorkflowExecutionFailedEventAttributes",
            "cause",
        )
        _fix_history_enum("CONTINUE_AS_NEW_INITIATOR", event, "*", "initiator")
        _fix_history_enum("EVENT_TYPE", event, "eventType")
        _fix_history_enum(
            "PARENT_CLOSE_POLICY",
            event,
            "startChildWorkflowExecutionInitiatedEventAttributes",
            "parentClosePolicy",
        )
        _fix_history_enum("RETRY_STATE", event, "*", "retryState")
        _fix_history_enum(
            "SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE",
            event,
            "signalExternalWorkflowExecutionFailedEventAttributes",
            "cause",
        )
        _fix_history_enum(
            "START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE",
            event,
            "startChildWorkflowExecutionFailedEventAttributes",
            "cause",
        )
        _fix_history_enum("TASK_QUEUE_KIND", event, "*", "taskQueue", "kind")
        _fix_history_enum(
            "TIMEOUT_TYPE",
            event,
            "workflowTaskTimedOutEventAttributes",
            "timeoutType",
        )
        _fix_history_enum(
            "WORKFLOW_ID_REUSE_POLICY",
            event,
            "startChildWorkflowExecutionInitiatedEventAttributes",
            "workflowIdReusePolicy",
        )
        _fix_history_enum(
            "WORKFLOW_TASK_FAILED_CAUSE",
            event,
            "workflowTaskFailedEventAttributes",
            "cause",
        )
        _fix_history_failure(event, "*", "failure")
        _fix_history_failure(event, "activityTaskStartedEventAttributes", "lastFailure")
        _fix_history_failure(
            event, "workflowExecutionStartedEventAttributes", "continuedFailure"
        )
    return google.protobuf.json_format.ParseDict(
        history, temporalio.api.history.v1.History(), ignore_unknown_fields=True
    )


def _fix_history_failure(parent: dict[str, Any], *attrs: str) -> None:
    _fix_history_enum(
        "TIMEOUT_TYPE", parent, *attrs, "timeoutFailureInfo", "timeoutType"
    )
    _fix_history_enum("RETRY_STATE", parent, *attrs, "*", "retryState")
    # Recurse into causes. First collect all failure parents.
    parents = [parent]
    for attr in attrs:
        new_parents = []
        for parent in parents:
            if attr == "*":
                for v in parent.values():
                    if isinstance(v, dict):
                        new_parents.append(v)
            else:
                child = parent.get(attr)
                if isinstance(child, dict):
                    new_parents.append(child)
        if not new_parents:
            return
        parents = new_parents
    # Fix each
    for parent in parents:
        _fix_history_failure(parent, "cause")


_pascal_case_match = re.compile("([A-Z]+)")


def _fix_history_enum(prefix: str, parent: dict[str, Any], *attrs: str) -> None:
    # If the attr is "*", we need to handle all dict children
    if attrs[0] == "*":
        for child in parent.values():
            if isinstance(child, dict):
                _fix_history_enum(prefix, child, *attrs[1:])
    else:
        child = parent.get(attrs[0])
        if isinstance(child, str) and len(attrs) == 1:
            # We only fix it if it doesn't already have the prefix
            if not parent[attrs[0]].startswith(prefix):
                parent[attrs[0]] = (
                    prefix + _pascal_case_match.sub(r"_\1", child).upper()
                )
        elif isinstance(child, dict) and len(attrs) > 1:
            _fix_history_enum(prefix, child, *attrs[1:])
        elif isinstance(child, list) and len(attrs) > 1:
            for child_item in child:
                if isinstance(child_item, dict):
                    _fix_history_enum(prefix, child_item, *attrs[1:])


async def _encode_user_metadata(  # pyright: ignore[reportUnusedFunction]
    converter: temporalio.converter.DataConverter,
    summary: str | temporalio.api.common.v1.Payload | None,
    details: str | temporalio.api.common.v1.Payload | None,
) -> temporalio.api.sdk.v1.UserMetadata | None:
    if summary is None and details is None:
        return None
    enc_summary = None
    enc_details = None
    if summary is not None:
        if isinstance(summary, str):
            enc_summary = (await converter.encode([summary]))[0]
        else:
            enc_summary = summary
    if details is not None:
        if isinstance(details, str):
            enc_details = (await converter.encode([details]))[0]
        else:
            enc_details = details
    return temporalio.api.sdk.v1.UserMetadata(summary=enc_summary, details=enc_details)


async def _decode_user_metadata(  # pyright: ignore[reportUnusedFunction]
    converter: temporalio.converter.DataConverter,
    metadata: temporalio.api.sdk.v1.UserMetadata | None,
) -> tuple[str | None, str | None]:
    """Returns (summary, details)"""
    if metadata is None:
        return None, None
    return (
        None
        if not metadata.HasField("summary")
        else (await converter.decode([metadata.summary]))[0],
        None
        if not metadata.HasField("details")
        else (await converter.decode([metadata.details]))[0],
    )
