from typing import TypedDict, Optional, Any, Dict, Union, Tuple, Type
from types import TracebackType
from typing_extensions import NotRequired  # For Python < 3.11 compatibility
from datetime import datetime
import numbers
from uuid import UUID

from posthog.types import SendFeatureFlagsOptions

ID_TYPES = Union[numbers.Number, str, UUID, int]


class OptionalCaptureArgs(TypedDict):
    """Optional arguments for the capture method.

    Args:
        distinct_id: Unique identifier for the person associated with this event. If not set, the context
            distinct_id is used, if available, otherwise a UUID is generated, and the event is marked
            as personless. Setting context-level distinct_id's is recommended.
        properties: Dictionary of properties to track with the event
        timestamp: When the event occurred (defaults to current time)
        uuid: Unique identifier for this specific event. If not provided, one is generated. The event
            UUID is returned, so you can correlate it with actions in your app (like showing users an
            error ID if you capture an exception).
        groups: Group identifiers to associate with this event (format: {group_type: group_key})
        send_feature_flags: Whether to include currently active feature flags in the event properties.
            Can be a boolean (True/False) or a SendFeatureFlagsOptions object for advanced configuration.
            Defaults to False.
        disable_geoip: Whether to disable GeoIP lookup for this event. Defaults to False.
    """

    distinct_id: NotRequired[Optional[ID_TYPES]]
    properties: NotRequired[Optional[Dict[str, Any]]]
    timestamp: NotRequired[Optional[Union[datetime, str]]]
    uuid: NotRequired[Optional[str]]
    groups: NotRequired[Optional[Dict[str, str]]]
    send_feature_flags: NotRequired[
        Optional[Union[bool, SendFeatureFlagsOptions]]
    ]  # Updated to support both boolean and options object
    disable_geoip: NotRequired[
        Optional[bool]
    ]  # As above, optional so we can tell if the user is intentionally overriding a client setting or not


class OptionalSetArgs(TypedDict):
    """Optional arguments for the set method.

    Args:
        distinct_id: Unique identifier for the user to set properties on. If not set, the context
            distinct_id is used, if available, otherwise this function does nothing. Setting
            context-level distinct_id's is recommended.
        properties: Dictionary of properties to set on the person
        timestamp: When the properties were set (defaults to current time)
        uuid: Unique identifier for this operation. If not provided, one is generated. This
            UUID is returned, so you can correlate it with actions in your app.
        disable_geoip: Whether to disable GeoIP lookup for this operation. Defaults to False.
    """

    distinct_id: NotRequired[Optional[ID_TYPES]]
    properties: NotRequired[Optional[Dict[str, Any]]]
    timestamp: NotRequired[Optional[Union[datetime, str]]]
    uuid: NotRequired[Optional[str]]
    disable_geoip: NotRequired[Optional[bool]]


ExcInfo = Union[
    Tuple[Type[BaseException], BaseException, Optional[TracebackType]],
    Tuple[None, None, None],
]

ExceptionArg = Union[BaseException, ExcInfo]
