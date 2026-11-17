from __future__ import annotations

import logging
import re
import urllib.parse
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
)

import nexusrpc

import temporalio.api.common.v1
import temporalio.api.enums.v1

if TYPE_CHECKING:
    import temporalio.client

logger = logging.getLogger(__name__)

_NEXUS_OPERATION_LINK_URL_PATH_REGEX = re.compile(
    r"^/namespaces/(?P<namespace>[^/]+)/nexus-operations/(?P<operation_id>[^/]+)/(?P<run_id>[^/]*)/details$"
)

_ACTIVITY_LINK_URL_PATH_REGEX = re.compile(
    r"^/namespaces/(?P<namespace>[^/]+)/activities/(?P<activity_id>[^/]+)/(?P<run_id>[^/]+)/details$"
)

_WORKFLOW_LINK_URL_PATH_REGEX = re.compile(
    r"^/namespaces/(?P<namespace>[^/]+)/workflows/(?P<workflow_id>[^/]+)/(?P<run_id>[^/]+)(?P<history>/history)?$"
)


class _LinkType(str, Enum):
    WORKFLOW_EVENT = temporalio.api.common.v1.Link.WorkflowEvent.DESCRIPTOR.full_name
    WORKFLOW = temporalio.api.common.v1.Link.Workflow.DESCRIPTOR.full_name
    NEXUS_OPERATION = temporalio.api.common.v1.Link.NexusOperation.DESCRIPTOR.full_name
    ACTIVITY = temporalio.api.common.v1.Link.Activity.DESCRIPTOR.full_name


LINK_EVENT_ID_PARAM_NAME = "eventID"
LINK_EVENT_TYPE_PARAM_NAME = "eventType"
LINK_REQUEST_ID_PARAM_NAME = "requestID"
LINK_REFERENCE_TYPE_PARAM_NAME = "referenceType"
LINK_REASON_PARAM_NAME = "reason"

EVENT_REFERENCE_TYPE = "EventReference"
REQUEST_ID_REFERENCE_TYPE = "RequestIdReference"


def workflow_execution_started_event_link_from_workflow_handle(
    handle: temporalio.client.WorkflowHandle[Any, Any], request_id: str
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
        request_id_ref=temporalio.api.common.v1.Link.WorkflowEvent.RequestIdReference(
            request_id=request_id,
            event_type=temporalio.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        ),
    )


def nexus_link_to_temporal_link(
    nexus_link: nexusrpc.Link,
) -> temporalio.api.common.v1.Link | None:
    """Convert a nexusrpc link into a Temporal API Link.

    Returns None when the Nexus link type is invalid or unknown.
    """
    try:
        link_type = _LinkType(nexus_link.type)
    except ValueError:
        logger.warning(f"Invalid Nexus link: unknown link type {nexus_link}")
        return None

    match link_type:
        case _LinkType.WORKFLOW_EVENT:
            return nexus_link_to_workflow_event_link(nexus_link)

        case _LinkType.WORKFLOW:
            return nexus_link_to_workflow_link(nexus_link)

        case _LinkType.NEXUS_OPERATION:
            return nexus_link_to_nexus_operation_link(nexus_link)

        case _LinkType.ACTIVITY:
            return nexus_link_to_activity_link(nexus_link)


def temporal_link_to_nexus_link(
    temporal_link: temporalio.api.common.v1.Link,
) -> nexusrpc.Link | None:
    """Convert a Temporal API Link into a nexusrpc link.

    Returns None when the Temporal link variant is missing.
    """
    match temporal_link.WhichOneof("variant"):
        case "workflow_event":
            return workflow_event_to_nexus_link(temporal_link.workflow_event)

        case "workflow":
            return workflow_to_nexus_link(temporal_link.workflow)

        case "nexus_operation":
            return nexus_operation_to_nexus_link(temporal_link.nexus_operation)

        case "activity":
            return activity_link_to_nexus_link(temporal_link.activity)

        case "batch_job":
            raise NotImplementedError("batch_job links are not supported")

        case None:
            logger.warning("Invalid Temporal link: missing variant")
            return None


def workflow_event_to_nexus_link(
    workflow_event: temporalio.api.common.v1.Link.WorkflowEvent,
) -> nexusrpc.Link:
    """Convert a WorkflowEvent link into a nexusrpc link

    Used when propagating links from a StartWorkflow response to a Nexus start operation
    response.
    """
    query_params = None
    match workflow_event.WhichOneof("reference"):
        case "event_ref":
            query_params = _event_reference_to_query_params(workflow_event.event_ref)
        case "request_id_ref":
            query_params = _request_id_reference_to_query_params(
                workflow_event.request_id_ref
            )
        case _:
            pass

    return nexusrpc.Link(
        url=_workflow_nexus_url(
            workflow_event.namespace,
            workflow_event.workflow_id,
            workflow_event.run_id,
            history=True,
            query_params=query_params,
        ),
        type=_LinkType.WORKFLOW_EVENT.value,
    )


def workflow_to_nexus_link(
    workflow: temporalio.api.common.v1.Link.Workflow,
) -> nexusrpc.Link:
    """Convert a Workflow link into a nexusrpc link."""
    query_params = ""
    if workflow.reason:
        query_params = urllib.parse.urlencode(
            {
                LINK_REASON_PARAM_NAME: workflow.reason,
            },
        )

    return nexusrpc.Link(
        url=_workflow_nexus_url(
            workflow.namespace,
            workflow.workflow_id,
            workflow.run_id,
            history=False,
            query_params=query_params,
        ),
        type=_LinkType.WORKFLOW.value,
    )


def nexus_operation_to_nexus_link(
    op_link: temporalio.api.common.v1.Link.NexusOperation,
) -> nexusrpc.Link:
    """Convert a NexusOperation link into a nexusrpc link

    Used when propagating links from a StartNexusOperation response to a Nexus start operation
    response.
    """
    namespace = urllib.parse.quote(op_link.namespace, safe="")
    operation_id = urllib.parse.quote(op_link.operation_id, safe="")
    run_id = urllib.parse.quote(op_link.run_id, safe="")
    path = f"/namespaces/{namespace}/nexus-operations/{operation_id}/{run_id}/details"

    return nexusrpc.Link(
        url=_temporal_nexus_url(path),
        type=_LinkType.NEXUS_OPERATION.value,
    )


def activity_link_to_nexus_link(
    activity: temporalio.api.common.v1.Link.Activity,
) -> nexusrpc.Link:
    """Convert an Activity link into a nexusrpc link."""
    namespace = urllib.parse.quote(activity.namespace, safe="")
    activity_id = urllib.parse.quote(activity.activity_id, safe="")
    run_id = urllib.parse.quote(activity.run_id, safe="")
    path = f"/namespaces/{namespace}/activities/{activity_id}/{run_id}/details"
    return nexusrpc.Link(url=_temporal_nexus_url(path), type=_LinkType.ACTIVITY.value)


def _workflow_nexus_url(
    namespace: str,
    workflow_id: str,
    run_id: str,
    *,
    history: bool,
    query_params: str | None = "",
) -> str:
    namespace = urllib.parse.quote(namespace, safe="")
    workflow_id = urllib.parse.quote(workflow_id, safe="")
    run_id = urllib.parse.quote(run_id, safe="")
    path = f"/namespaces/{namespace}/workflows/{workflow_id}/{run_id}"
    if history:
        path += "/history"
    return _temporal_nexus_url(path, query_params=query_params)


def _temporal_nexus_url(path: str, *, query_params: str | None = "") -> str:
    # urllib will omit '//' from the url if netloc is empty so we add the scheme manually
    return f"temporal://{urllib.parse.urlunparse(('', '', path, '', query_params or '', ''))}"


def _parse_workflow_nexus_url(
    link: nexusrpc.Link, *, history: bool
) -> tuple[dict[str, str], dict[str, list[str]]] | None:
    url = urllib.parse.urlparse(link.url)
    match = _WORKFLOW_LINK_URL_PATH_REGEX.match(url.path)
    if not match or bool(match.group("history")) != history:
        expected_suffix = "/history" if history else ""
        logger.warning(
            f"Invalid Nexus link: {link}. Expected path to match "
            f"/namespaces/{{namespace}}/workflows/{{workflow_id}}/{{run_id}}{expected_suffix}"
        )
        return None

    groups = {
        name: urllib.parse.unquote(value)
        for name, value in match.groupdict().items()
        if name != "history" and value is not None
    }
    return groups, urllib.parse.parse_qs(url.query)


def _optional_single_query_param(
    query_params: dict[str, list[str]], param_name: str
) -> str:
    match query_params.get(param_name):
        case [param]:
            return param
        case [] | None:
            return ""
        case _:
            raise ValueError(f"Expected {param_name} to have at most 1 value")


def nexus_link_to_workflow_event_link(
    link: nexusrpc.Link,
) -> temporalio.api.common.v1.Link | None:
    """Convert a nexus link into a Temporal WorkflowEvent link

    This is used when propagating links from a Nexus start operation request to a
    StartWorklow request.
    """
    parsed = _parse_workflow_nexus_url(link, history=True)
    if parsed is None:
        return None
    groups, query_params = parsed
    try:
        request_id_ref = None
        event_ref = None
        match query_params.get(LINK_REFERENCE_TYPE_PARAM_NAME):
            case ["EventReference"]:
                event_ref = _query_params_to_event_reference(query_params)
            case ["RequestIdReference"]:
                request_id_ref = _query_params_to_request_id_reference(query_params)
            case _:
                raise ValueError(
                    f"Invalid Nexus link: {link}. Expected {LINK_REFERENCE_TYPE_PARAM_NAME} to be '{EVENT_REFERENCE_TYPE}' or '{REQUEST_ID_REFERENCE_TYPE}'"
                )

    except ValueError as err:
        logger.warning(
            f"Failed to parse event reference from Nexus link URL query parameters: {link} ({err})"
        )
        return None

    workflow_event_link = temporalio.api.common.v1.Link.WorkflowEvent(
        namespace=groups["namespace"],
        workflow_id=groups["workflow_id"],
        run_id=groups["run_id"],
        event_ref=event_ref,
        request_id_ref=request_id_ref,
    )
    return temporalio.api.common.v1.Link(workflow_event=workflow_event_link)


def nexus_link_to_workflow_link(
    link: nexusrpc.Link,
) -> temporalio.api.common.v1.Link | None:
    """Convert a nexus link into a Temporal Workflow link."""
    parsed = _parse_workflow_nexus_url(link, history=False)
    if parsed is None:
        return None
    groups, query_params = parsed
    try:
        reason = _optional_single_query_param(query_params, LINK_REASON_PARAM_NAME)
    except ValueError as err:
        logger.warning(f"Invalid Nexus link: {link}. {err}")
        return None

    workflow_link = temporalio.api.common.v1.Link.Workflow(
        namespace=groups["namespace"],
        workflow_id=groups["workflow_id"],
        run_id=groups["run_id"],
        reason=reason,
    )
    return temporalio.api.common.v1.Link(workflow=workflow_link)


def nexus_link_to_nexus_operation_link(
    nexus_link: nexusrpc.Link,
) -> temporalio.api.common.v1.Link | None:
    """Convert a nexus link into a Temporal NexusOperation link

    This is used when propagating links from a Nexus start operation request to a
    StartNexusOperation request.
    """
    url = urllib.parse.urlparse(nexus_link.url)
    match = _NEXUS_OPERATION_LINK_URL_PATH_REGEX.match(url.path)
    if not match:
        logger.warning(
            f"Invalid Nexus link: {nexus_link}. Expected path to match {_NEXUS_OPERATION_LINK_URL_PATH_REGEX.pattern}"
        )
        return None

    groups = match.groupdict()
    nexus_op_link = temporalio.api.common.v1.Link.NexusOperation(
        namespace=urllib.parse.unquote(groups["namespace"]),
        operation_id=urllib.parse.unquote(groups["operation_id"]),
        run_id=urllib.parse.unquote(groups["run_id"]),
    )
    return temporalio.api.common.v1.Link(nexus_operation=nexus_op_link)


def nexus_link_to_activity_link(
    nexus_link: nexusrpc.Link,
) -> temporalio.api.common.v1.Link | None:
    """Convert a Nexus Activity link into a Temporal Activity link."""
    match = _ACTIVITY_LINK_URL_PATH_REGEX.match(
        urllib.parse.urlparse(nexus_link.url).path
    )
    if not match:
        logger.warning(
            f"Invalid Nexus link: {nexus_link}. Expected path to match {_ACTIVITY_LINK_URL_PATH_REGEX.pattern}"
        )
        return None
    groups = match.groupdict()
    return temporalio.api.common.v1.Link(
        activity=temporalio.api.common.v1.Link.Activity(
            namespace=urllib.parse.unquote(groups["namespace"]),
            activity_id=urllib.parse.unquote(groups["activity_id"]),
            run_id=urllib.parse.unquote(groups["run_id"]),
        )
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
            LINK_EVENT_ID_PARAM_NAME: event_ref.event_id,
            LINK_EVENT_TYPE_PARAM_NAME: event_type_name,
            LINK_REFERENCE_TYPE_PARAM_NAME: EVENT_REFERENCE_TYPE,
        }
    )


def _request_id_reference_to_query_params(
    request_id_ref: temporalio.api.common.v1.Link.WorkflowEvent.RequestIdReference,
) -> str:
    params = {
        LINK_REFERENCE_TYPE_PARAM_NAME: REQUEST_ID_REFERENCE_TYPE,
    }

    if request_id_ref.request_id:
        params[LINK_REQUEST_ID_PARAM_NAME] = request_id_ref.request_id

    event_type_name = temporalio.api.enums.v1.EventType.Name(request_id_ref.event_type)
    if event_type_name.startswith("EVENT_TYPE_"):
        event_type_name = _event_type_constant_case_to_pascal_case(
            event_type_name.removeprefix("EVENT_TYPE_")
        )
    params[LINK_EVENT_TYPE_PARAM_NAME] = event_type_name

    return urllib.parse.urlencode(params)


def _query_params_to_event_reference(
    query_params: dict[str, list[str]],
) -> temporalio.api.common.v1.Link.WorkflowEvent.EventReference:
    """Return an EventReference from the query params or raise ValueError."""
    [reference_type] = query_params.get(LINK_REFERENCE_TYPE_PARAM_NAME) or [""]
    if reference_type != EVENT_REFERENCE_TYPE:
        raise ValueError(
            f"Expected Nexus link URL query parameter referenceType to be EventReference but got: {reference_type}"
        )

    # event type
    match query_params.get(LINK_EVENT_TYPE_PARAM_NAME):
        case None:
            raise ValueError(f"query params do not contain event type: {query_params}")

        case [raw_event_type_name] if raw_event_type_name.startswith("EVENT_TYPE_"):
            event_type_name = raw_event_type_name

        case [raw_event_type_name] if re.match("[A-Z][a-z]", raw_event_type_name):
            event_type_name = "EVENT_TYPE_" + _event_type_pascal_case_to_constant_case(
                raw_event_type_name
            )

        case raw_event_type_name:
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


def _query_params_to_request_id_reference(
    query_params: dict[str, list[str]],
) -> temporalio.api.common.v1.Link.WorkflowEvent.RequestIdReference:
    """Return an EventReference from the query params or raise ValueError."""
    # event type
    match query_params.get(LINK_EVENT_TYPE_PARAM_NAME):
        case None:
            raise ValueError(f"query params do not contain event type: {query_params}")

        case [raw_event_type_name] if raw_event_type_name.startswith("EVENT_TYPE_"):
            event_type_name = raw_event_type_name

        case [raw_event_type_name] if re.match("[A-Z][a-z]", raw_event_type_name):
            event_type_name = "EVENT_TYPE_" + _event_type_pascal_case_to_constant_case(
                raw_event_type_name
            )

        case raw_event_type_name:
            raise ValueError(f"Invalid event type name: {raw_event_type_name}")

    [request_id] = query_params.get(LINK_REQUEST_ID_PARAM_NAME, [""])

    return temporalio.api.common.v1.Link.WorkflowEvent.RequestIdReference(
        request_id=request_id,
        event_type=temporalio.api.enums.v1.EventType.Value(event_type_name),
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
