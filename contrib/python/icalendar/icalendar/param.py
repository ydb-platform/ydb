"""Parameter access for icalendar.

Related:

- :rfc:`5545`, Section 3.2. Property Parameters
- :rfc:`7986`, Section 6. Property Parameters
- :rfc:`9253`
- https://github.com/collective/icalendar/issues/798
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, TypeVar

from icalendar import enums

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import timedelta
    from enum import Enum


if TYPE_CHECKING:
    from icalendar.prop import VPROPERTY


def _default_return_none() -> str | None:
    """Return None by default."""
    return None


def _default_return_string() -> str:
    """Return None by default."""
    return ""


T = TypeVar("T")


def string_parameter(
    name: str,
    doc: str,
    default: Callable = _default_return_none,
    convert: Callable[[str], T] | None = None,
    convert_to: Callable[[T], str] | None = None,
) -> property:
    """Return a parameter with a quoted value (case sensitive)."""

    if convert_to is None:
        convert_to = convert

    @functools.wraps(default)
    def fget(self: VPROPERTY) -> str | None:
        value = self.params.get(name)
        if value is None:
            return default()
        return convert(value) if convert else value

    def fset(self: VPROPERTY, value: str | None):
        if value is None:
            fdel(self)
        else:
            self.params[name] = convert_to(value) if convert_to else value

    def fdel(self: VPROPERTY):
        self.params.pop(name, None)

    return property(fget, fset, fdel, doc=doc)


ALTREP = string_parameter(
    "ALTREP",
    """ALTREP - Specify an alternate text representation for the property value.

Description:
    This parameter specifies a URI that points to an
    alternate representation for a textual property value.  A property
    specifying this parameter MUST also include a value that reflects
    the default representation of the text value.  The URI parameter
    value MUST be specified in a quoted-string.

.. note::

    While there is no restriction imposed on the URI schemes
    allowed for this parameter, Content Identifier (CID) :rfc:`2392`,
    HTTP :rfc:`2616`, and HTTPS :rfc:`2818` are the URI schemes most
    commonly used by current implementations.
""",
)

CN = string_parameter(
    "CN",
    """Specify the common name to be associated with the calendar user specified.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  The parameter specifies the common name
    to be associated with the calendar user specified by the property.
    The parameter value is text.  The parameter value can be used for
    display text to be associated with the calendar address specified
    by the property.
""",
    default=_default_return_string,
)


def _default_return_individual() -> enums.CUTYPE | str:
    """Default value."""
    return enums.CUTYPE.INDIVIDUAL


def _convert_enum(enum: type[Enum]) -> Callable[[str], Enum]:
    def convert(value: str) -> str:
        """Convert if possible."""
        try:
            return enum(value.upper())
        except ValueError:
            return value

    return convert


CUTYPE = string_parameter(
    "CUTYPE",
    """Identify the type of calendar user specified by the property.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  The parameter identifies the type of
    calendar user specified by the property.  If not specified on a
    property that allows this parameter, the default is INDIVIDUAL.
    Applications MUST treat x-name and iana-token values they don't
    recognize the same way as they would the UNKNOWN value.
""",
    default=_default_return_individual,
    convert=_convert_enum(enums.CUTYPE),
)


def quoted_list_parameter(name: str, doc: str) -> property:
    """Return a parameter that contains a quoted list."""

    def fget(self: VPROPERTY) -> tuple[str]:
        value = self.params.get(name)
        if value is None:
            return ()
        if isinstance(value, str):
            return tuple(value.split(","))
        return value

    def fset(self: VPROPERTY, value: str | tuple[str]):
        if value == ():
            fdel(self)
        else:
            self.params[name] = (value,) if isinstance(value, str) else value

    def fdel(self: VPROPERTY):
        self.params.pop(name, None)

    return property(fget, fset, fdel, doc=doc)


DELEGATED_FROM = quoted_list_parameter(
    "DELEGATED-FROM",
    """Specify the calendar users that have delegated their participation to the calendar user specified by the property.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  This parameter specifies those calendar
    users that have delegated their participation in a group-scheduled
    event or to-do to the calendar user specified by the property.
    The individual calendar address parameter values MUST each be
    specified in a quoted-string.
""",  # noqa: E501
)

DELEGATED_TO = quoted_list_parameter(
    "DELEGATED-TO",
    """Specify the calendar users to whom the calendar user specified by the property has delegated participation.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  This parameter specifies those calendar
    users whom have been delegated participation in a group-scheduled
    event or to-do by the calendar user specified by the property.
    The individual calendar address parameter values MUST each be
    specified in a quoted-string.
    """,  # noqa: E501
)

DIR = string_parameter(
    "DIR",
    """Specify reference to a directory entry associated with the calendar user specified by the property.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  The parameter specifies a reference to
    the directory entry associated with the calendar user specified by
    the property.  The parameter value is a URI.  The URI parameter
    value MUST be specified in a quoted-string.

.. note::

    While there is no restriction imposed on the URI schemes
    allowed for this parameter, CID :rfc:`2392`, DATA :rfc:`2397`, FILE
    :rfc:`1738`, FTP :rfc:`1738`, HTTP :rfc:`2616`, HTTPS :rfc:`2818`, LDAP
    :rfc:`4516`, and MID :rfc:`2392` are the URI schemes most commonly
    used by current implementations.
""",  # noqa: E501
)


def _default_return_busy() -> enums.FBTYPE | str:
    """Default value."""
    return enums.FBTYPE.BUSY


FBTYPE = string_parameter(
    "FBTYPE",
    """Specify the free or busy time type.

Description:
    This parameter specifies the free or busy time type.
    The value FREE indicates that the time interval is free for
    scheduling.  The value BUSY indicates that the time interval is
    busy because one or more events have been scheduled for that
    interval.  The value BUSY-UNAVAILABLE indicates that the time
    interval is busy and that the interval can not be scheduled.  The
    value BUSY-TENTATIVE indicates that the time interval is busy
    because one or more events have been tentatively scheduled for
    that interval.  If not specified on a property that allows this
    parameter, the default is BUSY.  Applications MUST treat x-name
    and iana-token values they don't recognize the same way as they
    would the BUSY value.
""",
    default=_default_return_busy,
    convert=_convert_enum(enums.FBTYPE),
)

LANGUAGE = string_parameter(
    "LANGUAGE",
    """Specify the language for text values in a property or property parameter.

Description:
    This parameter identifies the language of the text in
    the property value and of all property parameter values of the
    property.  The value of the "LANGUAGE" property parameter is that
    defined in :rfc:`5646`.

    For transport in a MIME entity, the Content-Language header field
    can be used to set the default language for the entire body part.
    Otherwise, no default language is assumed.
""",
)

MEMBER = quoted_list_parameter(
    "MEMBER",
    """Specify the group or list membership of the calendar user specified by the property.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  The parameter identifies the groups or
    list membership for the calendar user specified by the property.
    The parameter value is either a single calendar address in a
    quoted-string or a COMMA-separated list of calendar addresses,
    each in a quoted-string.  The individual calendar address
    parameter values MUST each be specified in a quoted-string.
""",  # noqa: E501
)


def _default_return_needs_action() -> enums.PARTSTAT | str:
    """Default value."""
    return enums.PARTSTAT.NEEDS_ACTION


PARTSTAT = string_parameter(
    "PARTSTAT",
    """Specify the participation status for the calendar user specified by the property.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  The parameter identifies the
    participation status for the calendar user specified by the
    property value.  The parameter values differ depending on whether
    they are associated with a group-scheduled "VEVENT", "VTODO", or
    "VJOURNAL".  The values MUST match one of the values allowed for
    the given calendar component.  If not specified on a property that
    allows this parameter, the default value is NEEDS-ACTION.
    Applications MUST treat x-name and iana-token values they don't
    recognize the same way as they would the NEEDS-ACTION value.
""",
    default=_default_return_needs_action,
    convert=_convert_enum(enums.PARTSTAT),
)


def _default_range_none() -> enums.RANGE | str | None:
    return None


RANGE = string_parameter(
    "RANGE",
    """Specify the effective range of recurrence instances from the instance specified by the recurrence identifier specified by the property.

Description:
    This parameter can be specified on a property that
    specifies a recurrence identifier.  The parameter specifies the
    effective range of recurrence instances that is specified by the
    property.  The effective range is from the recurrence identifier
    specified by the property.  If this parameter is not specified on
    an allowed property, then the default range is the single instance
    specified by the recurrence identifier value of the property.  The
    parameter value can only be "THISANDFUTURE" to indicate a range
    defined by the recurrence identifier and all subsequent instances.
    The value "THISANDPRIOR" is deprecated by this revision of
    iCalendar and MUST NOT be generated by applications.
""",  # noqa: E501
    default=_default_range_none,
    convert=_convert_enum(enums.RANGE),
)


def _default_related() -> enums.RELATED | str:
    return enums.RELATED.START


RELATED = string_parameter(
    "RELATED",
    """Specify the relationship of the alarm trigger with respect to the start or end of the calendar component.

Description:
    This parameter can be specified on properties that
    specify an alarm trigger with a "DURATION" value type.  The
    parameter specifies whether the alarm will trigger relative to the
    start or end of the calendar component.  The parameter value START
    will set the alarm to trigger off the start of the calendar
    component; the parameter value END will set the alarm to trigger
    off the end of the calendar component.  If the parameter is not
    specified on an allowable property, then the default is START.

""",  # noqa: E501
    default=_default_related,
    convert=_convert_enum(enums.RELATED),
)


def _default_req_participant() -> enums.ROLE | str:
    return enums.ROLE.REQ_PARTICIPANT


ROLE = string_parameter(
    "ROLE",
    """Specify the participation role for the calendar user specified by the property.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  The parameter specifies the participation
    role for the calendar user specified by the property in the group
    schedule calendar component.  If not specified on a property that
    allows this parameter, the default value is REQ-PARTICIPANT.
    Applications MUST treat x-name and iana-token values they don't
    recognize the same way as they would the REQ-PARTICIPANT value.
""",
    default=_default_req_participant,
    convert=_convert_enum(enums.ROLE),
)


def boolean_parameter(name: str, default: bool, doc: str) -> property:
    def _default() -> bool:
        return default

    return string_parameter(
        name,
        doc,
        default=_default,
        convert=lambda x: x.upper() == "TRUE",
        convert_to=lambda x: "TRUE" if x else "FALSE",
    )


RSVP = boolean_parameter(
    "RSVP",
    False,
    """Specify whether there is an expectation of a favor of anreply from the calendar user specified by the property value.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  The parameter identifies the expectation
    of a reply from the calendar user specified by the property value.
    This parameter is used by the "Organizer" to request a
    participation status reply from an "Attendee" of a group-scheduled
    event or to-do.  If not specified on a property that allows this
    parameter, the default value is ``False``.
""",  # noqa: E501
)

SENT_BY = string_parameter(
    "SENT-BY",
    """Specify the calendar user that is acting on behalf of the calendar user specified by the property.

Description:
    This parameter can be specified on properties with a
    CAL-ADDRESS value type.  The parameter specifies the calendar user
    that is acting on behalf of the calendar user specified by the
    property.  The parameter value MUST be a mailto URI as defined in
    :rfc:`2368`.  The individual calendar address parameter values MUST
    each be specified in a quoted-string.
""",  # noqa: E501
)

TZID = string_parameter(
    "TZID",
    """Specify the identifier for the time zone definition for a time component in the property value.

Description:
    This parameter MUST be specified on the "DTSTART",
    "DTEND", "DUE", "EXDATE", and "RDATE" properties when either a
    DATE-TIME or TIME value type is specified and when the value is
    neither a UTC or a "floating" time.  Refer to the DATE-TIME or
    TIME value type definition for a description of UTC and "floating
    time" formats.  This property parameter specifies a text value
    that uniquely identifies the "VTIMEZONE" calendar component to be
    used when evaluating the time portion of the property.  The value
    of the "TZID" property parameter will be equal to the value of the
    "TZID" property for the matching time zone definition.  An
    individual "VTIMEZONE" calendar component MUST be specified for
    each unique "TZID" parameter value specified in the iCalendar
    object.

    The parameter MUST be specified on properties with a DATE-TIME
    value if the DATE-TIME is not either a UTC or a "floating" time.
    Failure to include and follow VTIMEZONE definitions in iCalendar
    objects may lead to inconsistent understanding of the local time
    at any given location.

    The presence of the SOLIDUS character as a prefix, indicates that
    this "TZID" represents a unique ID in a globally defined time zone
    registry (when such registry is defined).

.. note::

    This document does not define a naming convention for
    time zone identifiers.  Implementers may want to use the naming
    conventions defined in existing time zone specifications such
    as the public-domain TZ database (TZDB). The specification of
    globally unique time zone identifiers is not addressed by this
    document and is left for future study.
""",  # noqa: E501
)


def _default_return_parent() -> enums.RELTYPE:
    return enums.RELTYPE.PARENT


RELTYPE = string_parameter(
    "RELTYPE",
    """Specify the type of hierarchical relationship associated with a component.

Conformance:
    :rfc:`5545` introduces the RELTYPE property parameter.
    :rfc:`9253` adds new values.

Description:
    This parameter can be specified on a property that
    references another related calendar.  The parameter specifies the
    hierarchical relationship type of the calendar component
    referenced by the property.  The parameter value can be PARENT, to
    indicate that the referenced calendar component is a superior of
    calendar component; CHILD to indicate that the referenced calendar
    component is a subordinate of the calendar component; or SIBLING
    to indicate that the referenced calendar component is a peer of
    the calendar component.  If this parameter is not specified on an
    allowable property, the default relationship type is PARENT.
    Applications MUST treat x-name and iana-token values they don't
    recognize the same way as they would the PARENT value.
""",
    default=_default_return_parent,
    convert=_convert_enum(enums.RELTYPE),
)


def _get_value(self: VPROPERTY) -> str:
    """The VALUE parameter or the default.

    Purpose:
        VALUE explicitly specify the value type format for a property value.

    Description:
        This parameter specifies the value type and format of
        the property value.  The property values MUST be of a single value
        type.  For example, a "RDATE" property cannot have a combination
        of DATE-TIME and TIME value types.

        If the property's value is the default value type, then this
        parameter need not be specified.  However, if the property's
        default value type is overridden by some other allowable value
        type, then this parameter MUST be specified.

        Applications MUST preserve the value data for ``x-name`` and
        ``iana-token`` values that they don't recognize without attempting
        to interpret or parse the value data.

    Returns:
        The VALUE parameter or the default.

    Examples:
        The VALUE defaults to the name of the property.
        Note that it is case-insensitive but always uppercase.

        .. code-block:: pycon

            >>> from icalendar import vBoolean
            >>> b = vBoolean(True)
            >>> b.VALUE
            'BOOLEAN'

        Setting the VALUE parameter of a typed property usually does not make sense.
        For convenience, using this property, the value will be converted to
        an uppercase string.
        If you have some custom property, you might use it like this:

        .. code-block:: pycon

            >>> from icalendar import vUnknown, Event
            >>> v = vUnknown("Some property text.")
            >>> v.VALUE = "x-type"  # lower case
            >>> v.VALUE
            'X-TYPE'
            >>> event = Event()
            >>> event.add("x-prop", v)
            >>> print(event.to_ical())
            BEGIN:VEVENT
            X-PROP;VALUE=X-TYPE:Some property text.
            END:VEVENT

    """
    value = self.params.value
    if value is None:
        _get_default_value = getattr(self, "_get_value", None)
        if _get_default_value is not None:
            value = _get_default_value()
    return self.default_value if value is None else value


def _set_value(self: VPROPERTY, value: str | None):
    """Set the VALUE parameter."""
    self.params.value = value


def _del_value(self: VPROPERTY):
    """Delete the VALUE parameter."""
    del self.params.value


VALUE = property(_get_value, _set_value, _del_value)

LABEL = string_parameter(
    "LABEL",
    """LABEL provides a human-readable label.

Conformance:
    This property parameter is specified in :rfc:`7986`,
    iCalendar Property Extensions.

    :rfc:`9253` makes use of this for the LINK property.
    This parameter maps to the "title"
    attribute defined in Section 3.4.1 of :rfc:`8288`.
    LABEL is used to label the destination
    of a link such that it can be used as a human-readable identifier
    (e.g., a menu entry) in the language indicated by the LANGUAGE
    (if present). The LABEL MUST NOT
    appear more than once in a given link; occurrences after the first
    MUST be ignored by parsers.

Description:
    This property parameter MAY be specified on the
    "CONFERENCE" property.  It is anticipated that other extensions to
    iCalendar will reuse this property parameter on new properties
    that they define.  As a result, clients MUST expect to find this
    property parameter present on many different properties.  It
    provides a human-readable label that can be presented to calendar
    users to allow them to discriminate between properties that might
    be similar or provide additional information for properties that
    are not self-describing.  The "LANGUAGE" property parameter can be
    used to specify the language of the text in the parameter value
    (as per Section 3.2.10 of :rfc:`5545`).

Examples:
    This is a label of a chat.

    .. code-block:: text

        CONFERENCE;VALUE=URI;FEATURE=VIDEO;
            LABEL="Web video chat, access code=76543";
            :https://video-chat.example.com/;group-id=1234

""",
)

FMTTYPE = string_parameter(
    "FMTTYPE",
    """FMTTYPE specfies the content type of a referenced object.

Conformance:
    :rfc:`5545` specifies the FMTTYPE.
    :rfc:`9253` adds FMTTYPE to LINK properties. In a LINK,
    FMTTYPE maps to the "type"
    attribute defined in Section 3.4.1 of :rfc:`8288`.
    See :rfc:`6838`.

Description:
    This parameter can be specified on properties that are
    used to reference an object.  The parameter specifies the media
    type :rfc:`4288` of the referenced object. For example, on the
    "ATTACH" property, an FTP type URI value does not, by itself,
    necessarily convey the type of content associated with the
    resource.  The parameter value MUST be the text for either an
    IANA-registered media type or a non-standard media type.

Example:
    A Microsoft Word document:

    .. code-block:: text

        ATTACH;FMTTYPE=application/msword:ftp://example.com/pub/docs/
         agenda.doc

    A website:

    .. code-block:: text

        LINK;FMTTYPE=text/html;LINKREL=SOURCE;LABEL=Venue;VALUE=URI:
         https://example.com/venue

    """,
)

LINKREL = string_parameter(
    "LINKREL",
    """LINKREL

Purpose:
    LINKREL specifies the relationship of data referenced
    by a LINK property.

Conformance:
    LINKREL is specified in :rfc:`9253`.
    This parameter maps to the link relation type defined in
    Section 2.1 of :rfc:`8288`.
    It is always quoted.

Description:
    This parameter MUST be specified on all LINK properties and define
    the type of reference.
    This allows programs consuming this data to automatically scan
    for references they support.
    There is no default relation type. Any link relation in the
    link registry established by :rfc:`8288`, or new link relations,
    may be used.
    It is expected that link relation types seeing significant usage
    in calendaring will have the calendaring usage described in an RFC.

    In the simplest case, a link relation type identifies the semantics
    of a link.  For example, a link with the relation type "copyright"
    indicates that the current link context has a copyright resource at
    the link target.

    Link relation types can also be used to indicate that the target
    resource has particular attributes, or exhibits particular
    behaviours; for example, a "service" link implies that the link
    target can be used as part of a defined protocol (in this case, a
    service description).

Registration:
    There are two kinds of relation types: registered and extension.
    These relation types are registered in :rfc:`8288`.

.. seealso::

    `Registered Link Relation Types
    <https://www.iana.org/assignments/link-relations/link-relations.xhtml>`_.


Examples:
    This identifies the latest version of the event information.

    .. code-block:: text

        LINKREL=latest-version

""",
)


def _get_GAP(prop) -> timedelta | None:  # noqa: N802
    """GAP

    Purpose:
        GAP specifies the length of the gap, positive or negative,
        between two components with a temporal relationship.

    Format Definition:
        Same as the DURATION value type defined in :rfc:`5545`, Section 3.3.6.

    Description:
        This parameter MAY be specified on the RELATED-TO property and defines
        the duration of time between the predecessor and successor in an interval.
        When positive, it defines the lag time between a task and its logical successor.
        When negative, it defines the lead time.

    Examples:
        An example of lag time might be if Task-A is "paint the room" and Task-B is
        "lay the carpets". Then, Task-A may be related to Task-B with
        RELTYPE=FINISHTOSTART with a gap of 1 day -- long enough for the paint to dry.

        .. code-block:: text

            ====================
            |  paint the room  |--+
            ====================  |
                                  |(lag of one day)
                                  |
                                  |  ===================
                                  +->| lay the carpet  |
                                     ===================

        For an example of lead time, in constructing a two-story building,
        the electrical work must be done before painting. However,
        the painter can move in to the first floor as the electricians move upstairs.

        .. code-block:: text

            =====================
            |  electrical work  |--+
            =====================  |
                     +-------------+
                     |(lead of estimated time)
                     |  ==================
                     +->|    painting    |
                        ==================
    """
    value = prop.params.get("GAP")
    if value is None:
        return None
    from icalendar.prop import vDuration

    if isinstance(value, str):
        return vDuration.from_ical(value)
    if not isinstance(value, vDuration):
        raise TypeError("Value MUST be a vDuration instance")
    return value.td


def _set_GAP(prop, value: timedelta | str | None):  # noqa: N802
    """Set the GAP parameter as a timedelta."""
    if value is None:
        prop.params.pop("GAP", None)
        return
    from icalendar.prop import vDuration

    prop.params["GAP"] = vDuration(value)


def _del_GAP(prop):  # noqa: N802
    """Delete the GAP parameter."""
    prop.params.pop("GAP", None)


GAP = property(_get_GAP, _set_GAP, _del_GAP)

__all__ = [
    "ALTREP",
    "CN",
    "CUTYPE",
    "DELEGATED_FROM",
    "DELEGATED_TO",
    "DIR",
    "FBTYPE",
    "FMTTYPE",
    "GAP",
    "LABEL",
    "LANGUAGE",
    "LINKREL",
    "MEMBER",
    "PARTSTAT",
    "RANGE",
    "RELATED",
    "ROLE",
    "RSVP",
    "SENT_BY",
    "TZID",
    "VALUE",
    "quoted_list_parameter",
    "string_parameter",
]
