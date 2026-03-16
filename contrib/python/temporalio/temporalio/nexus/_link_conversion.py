from __future__ import annotations

import logging
import re
import urllib.parse
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

import nexusrpc

import temporalio.api.common.v1
import temporalio.api.enums.v1

if TYPE_CHECKING:
    import temporalio.client

logger = logging.getLogger(__name__)

_LINK_URL_PATH_REGEX = re.compile(
    r"^/namespaces/(?P<namespace>[^/]+)/workflows/(?P<workflow_id>[^/]+)/(?P<run_id>[^/]+)/history$"
)
LINK_EVENT_ID_PARAM_NAME = "eventID"
LINK_EVENT_TYPE_PARAM_NAME = "eventType"


def workflow_execution_started_event_link_from_workflow_handle(
    handle: temporalio.client.WorkflowHandle[Any, Any],
) -> temporalio.api.common.v1.Link.WorkflowEvent:
    """Create a WorkflowEvent link corresponding to a started workflow"""
    if handle.first_execution_run_id is None:
        raise ValueError(
            f"Workflow handle {handle} has no first execution run ID. "
            f"Cannot create WorkflowExecutionStarted event link."
        )
    return temporalio.api.common.v1.Link.WorkflowEvent(
        namespace=handle._client.namespace,
        workflow_id=handle.id,
        run_id=handle.first_execution_run_id,
        event_ref=temporalio.api.common.v1.Link.WorkflowEvent.EventReference(
            event_id=1,
            event_type=temporalio.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        ),
        # TODO(nexus-preview): RequestIdReference
    )


def workflow_event_to_nexus_link(
    workflow_event: temporalio.api.common.v1.Link.WorkflowEvent,
) -> nexusrpc.Link:
    """Convert a WorkflowEvent link into a nexusrpc link

    Used when propagating links from a StartWorkflow response to a Nexus start operation
    response.
    """
    scheme = "temporal"
    namespace = urllib.parse.quote(workflow_event.namespace)
    workflow_id = urllib.parse.quote(workflow_event.workflow_id)
    run_id = urllib.parse.quote(workflow_event.run_id)
    path = f"/namespaces/{namespace}/workflows/{workflow_id}/{run_id}/history"
    query_params = _event_reference_to_query_params(workflow_event.event_ref)
    return nexusrpc.Link(
        url=urllib.parse.urlunparse((scheme, "", path, "", query_params, "")),
        type=workflow_event.DESCRIPTOR.full_name,
    )


def nexus_link_to_workflow_event(
    link: nexusrpc.Link,
) -> Optional[temporalio.api.common.v1.Link.WorkflowEvent]:
    """Convert a nexus link into a WorkflowEvent link

    This is used when propagating links from a Nexus start operation request to a
    StartWorklow request.
    """
    url = urllib.parse.urlparse(link.url)
    match = _LINK_URL_PATH_REGEX.match(url.path)
    if not match:
        logger.warning(
            f"Invalid Nexus link: {link}. Expected path to match {_LINK_URL_PATH_REGEX.pattern}"
        )
        return None
    try:
        event_ref = _query_params_to_event_reference(url.query)
    except ValueError as err:
        logger.warning(
            f"Failed to parse event reference from Nexus link URL query parameters: {link} ({err})"
        )
        return None

    groups = match.groupdict()
    return temporalio.api.common.v1.Link.WorkflowEvent(
        namespace=urllib.parse.unquote(groups["namespace"]),
        workflow_id=urllib.parse.unquote(groups["workflow_id"]),
        run_id=urllib.parse.unquote(groups["run_id"]),
        event_ref=event_ref,
    )


def _event_reference_to_query_params(
    event_ref: temporalio.api.common.v1.Link.WorkflowEvent.EventReference,
) -> str:
    event_type_name = temporalio.api.enums.v1.EventType.Name(event_ref.event_type)
    if event_type_name.startswith("EVENT_TYPE_"):
        event_type_name = _event_type_constant_case_to_pascal_case(
            event_type_name.removeprefix("EVENT_TYPE_")
        )
    return urllib.parse.urlencode(
        {
            "eventID": event_ref.event_id,
            "eventType": event_type_name,
            "referenceType": "EventReference",
        }
    )


def _query_params_to_event_reference(
    raw_query_params: str,
) -> temporalio.api.common.v1.Link.WorkflowEvent.EventReference:
    """Return an EventReference from the query params or raise ValueError."""
    query_params = urllib.parse.parse_qs(raw_query_params)

    [reference_type] = query_params.get("referenceType") or [""]
    if reference_type != "EventReference":
        raise ValueError(
            f"Expected Nexus link URL query parameter referenceType to be EventReference but got: {reference_type}"
        )
    # event type
    [raw_event_type_name] = query_params.get(LINK_EVENT_TYPE_PARAM_NAME) or [""]
    if not raw_event_type_name:
        raise ValueError(f"query params do not contain event type: {query_params}")
    if raw_event_type_name.startswith("EVENT_TYPE_"):
        event_type_name = raw_event_type_name
    elif re.match("[A-Z][a-z]", raw_event_type_name):
        event_type_name = "EVENT_TYPE_" + _event_type_pascal_case_to_constant_case(
            raw_event_type_name
        )
    else:
        raise ValueError(f"Invalid event type name: {raw_event_type_name}")

    # event id
    event_id = 0
    [raw_event_id] = query_params.get(LINK_EVENT_ID_PARAM_NAME) or [""]
    if raw_event_id:
        try:
            event_id = int(raw_event_id)
        except ValueError:
            raise ValueError(f"Query params contain invalid event id: {raw_event_id}")

    return temporalio.api.common.v1.Link.WorkflowEvent.EventReference(
        event_type=temporalio.api.enums.v1.EventType.Value(event_type_name),
        event_id=event_id,
    )


def _event_type_constant_case_to_pascal_case(s: str) -> str:
    """Convert a CONSTANT_CASE string to PascalCase.

    >>> _event_type_constant_case_to_pascal_case("NEXUS_OPERATION_SCHEDULED")
    "NexusOperationScheduled"
    """
    return re.sub(r"(\b|_)([a-z])", lambda m: m.groups()[1].upper(), s.lower())


def _event_type_pascal_case_to_constant_case(s: str) -> str:
    """Convert a PascalCase string to CONSTANT_CASE.

    >>> _event_type_pascal_case_to_constant_case("NexusOperationScheduled")
    "NEXUS_OPERATION_SCHEDULED"
    """
    return re.sub(r"([A-Z])", r"_\1", s).lstrip("_").upper()
