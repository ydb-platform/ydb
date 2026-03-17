"""Enumerations for different types in the RFCs."""

from enum import Enum as _Enum


class Enum(_Enum):
    """Enum class that can be pickled."""

    def __reduce_ex__(self, _p):
        """For pickling."""
        return self.__class__, (self._name_,)


class StrEnum(str, Enum):
    """Enum for strings."""

    def __str__(self) -> str:
        """Convert to a string.

        This is needed when we set the value directly in components.
        """
        return self.value


class PARTSTAT(StrEnum):
    """Enum for PARTSTAT from :rfc:`5545`.

    Values:
        ``NEEDS_ACTION``,
        ``ACCEPTED``,
        ``DECLINED``,
        ``TENTATIVE``,
        ``DELEGATED``,
        ``COMPLETED``,
        ``IN_PROCESS``

    Purpose:
        To specify the participation status for the calendar user
        specified by the property.

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
    """

    NEEDS_ACTION = "NEEDS-ACTION"
    ACCEPTED = "ACCEPTED"
    DECLINED = "DECLINED"
    TENTATIVE = "TENTATIVE"
    DELEGATED = "DELEGATED"
    COMPLETED = "COMPLETED"
    IN_PROCESS = "IN-PROCESS"


class STATUS(StrEnum):
    """Enum for STATUS from :rfc:`5545`.

    Values for :class:`icalendar.cal.event.Event`:
        ``CONFIRMED``,
        ``TENTATIVE``,
        ``CANCELLED``

    Values for :class:`icalendar.cal.todo.Todo`:
        ``NEEDS_ACTION``,
        ``COMPLETED``,
        ``IN_PROCESS``,
        ``CANCELLED``

    Values for :class:`icalendar.cal.journal.Journal`:
        ``DRAFT``,
        ``FINAL``,
        ``CANCELLED``

    Description:
        In a group-scheduled calendar component, the property
        is used by the "Organizer" to provide a confirmation of the event
        to the "Attendees".  For example in a "VEVENT" calendar component,
        the "Organizer" can indicate that a meeting is tentative,
        confirmed, or cancelled.  In a "VTODO" calendar component, the
        "Organizer" can indicate that an action item needs action, is
        completed, is in process or being worked on, or has been
        cancelled.  In a "VJOURNAL" calendar component, the "Organizer"
        can indicate that a journal entry is draft, final, or has been
        cancelled or removed.
    """

    # Event
    TENTATIVE = "TENTATIVE"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"

    # VTodo
    NEEDS_ACTION = "NEEDS-ACTION"
    COMPLETED = "COMPLETED"
    IN_PROCESS = "IN-PROCESS"
    # CANCELLED

    # Journal
    DRAFT = "DRAFT"
    FINAL = "FINAL"
    # CANCELLED


class FBTYPE(StrEnum):
    """Enum for FBTYPE from :rfc:`5545`.

    Values:
        ``FREE``,
        ``BUSY``,
        ``BUSY_UNAVAILABLE``,
        ``BUSY_TENTATIVE``

    See also :class:`BUSYTYPE`.

    Purpose:
        To specify the free or busy time type.

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
    """

    FREE = "FREE"
    BUSY = "BUSY"
    BUSY_UNAVAILABLE = "BUSY-UNAVAILABLE"
    BUSY_TENTATIVE = "BUSY-TENTATIVE"


class CUTYPE(StrEnum):
    """Enum for CTYPE from :rfc:`5545`.

    Values:
        ``INDIVIDUAL``,
        ``GROUP``,
        ``RESOURCE``,
        ``ROOM``,
        ``UNKNOWN``

    Purpose:
        To identify the type of calendar user specified by the property.

    Description:
        This parameter can be specified on properties with a
        CAL-ADDRESS value type.  The parameter identifies the type of
        calendar user specified by the property.  If not specified on a
        property that allows this parameter, the default is INDIVIDUAL.
        Applications MUST treat x-name and iana-token values they don't
        recognize the same way as they would the UNKNOWN value.
    """

    INDIVIDUAL = "INDIVIDUAL"
    GROUP = "GROUP"
    RESOURCE = "RESOURCE"
    ROOM = "ROOM"
    UNKNOWN = "UNKNOWN"


class RELTYPE(StrEnum):
    """Enum for RELTYPE from :rfc:`5545`.

    Values:
        ``PARENT``,
        ``CHILD``,
        ``SIBLING``,
        ``FINISHTOSTART``,
        ``FINISHTOFINISH``,
        ``STARTTOFINISH``,
        ``STARTTOSTART``,
        ``FIRST``,
        ``NEXT``,
        ``DEPENDS-ON``,
        ``REFID``,
        ``CONCEPT``

    Purpose:
        To specify the type of hierarchical relationship associated
        with the calendar component specified by the property.

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

    New Temporal RELTYPE Parameter Values (:rfc:`9253`):
        RELTYPE=FINISHTOSTART
            Task-B cannot start until Task-A finishes.
            For example, when painting is complete, carpet laying can begin.
        RELTYPE=FINISHTOFINISH
            Task-B can only be completed after Task-A is finished.
            The related tasks may run in parallel before completion.
        RELTYPE=STARTTOFINISH
            The start of Task-A (which occurs after Task-B) controls the
            finish of Task-B. For example, ticket sales (Task-B) end
            after the game starts (Task-A).
        RELTYPE=STARTTOSTART
            The start of Task-A triggers the start of Task-B,
            that is, Task-B can start anytime after Task-A starts.

    Additional New RELTYPE Parameter Values (:rfc:`9253`):
        RELTYPE=FIRST
            This indicates that the referenced calendar component is
            the first in a series the referencing calendar component
            is part of.
        RELTYPE=NEXT
            This indicates that the referenced calendar component
            is the next in a series the referencing calendar component
            is part of.
        RELTYPE=DEPENDS-ON
            This indicates that the current calendar component
            depends on the referenced calendar component in some manner.
            For example, a task may be blocked waiting on the other,
            referenced, task.
        RELTYPE=REFID
            This establishes a reference from the current component to
            components with a REFID property that matches the value given
            in the associated RELATED-TO property.
        RELTYPE=CONCEPT
            This establishes a reference from the current component to
            components with a CONCEPT property that matches the value
            given in the associated RELATED-TO property.

        Note that the relationship types of PARENT, CHILD, and SIBLING
        establish a hierarchical relationship. The new types of FIRST
        and NEXT are an ordering relationship.

    Examples:
        The following is an example from :rfc:`5545`:

        .. code-block:: text

            RELATED-TO;RELTYPE=SIBLING:19960401-080045-4000F192713@
             example.com

        Usage inside of RELATED-TO according to :rfc:`9253`:

        .. code-block:: text

            RELATED-TO;VALUE=URI;RELTYPE=STARTTOFINISH:
             https://example.com/caldav/user/jb/cal/
             19960401-080045-4000F192713.ics
    """

    PARENT = "PARENT"
    CHILD = "CHILD"
    SIBLING = "SIBLING"
    FINISHTOSTART = "FINISHTOSTART"
    FINISHTOFINISH = "FINISHTOFINISH"
    STARTTOFINISH = "STARTTOFINISH"
    STARTTOSTART = "STARTTOSTART"
    FIRST = "FIRST"
    NEXT = "NEXT"
    DEPENDS_ON = "DEPENDS-ON"
    REFID = "REFID"
    CONCEPT = "CONCEPT"


class RANGE(StrEnum):
    """Enum for RANGE from :rfc:`5545`.

    Values:
        ``THISANDFUTURE``,
        ``THISANDPRIOR`` (deprecated)

    Purpose:
        To specify the effective range of recurrence instances from
        the instance specified by the recurrence identifier specified by
        the property.

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
    """

    THISANDFUTURE = "THISANDFUTURE"
    THISANDPRIOR = "THISANDPRIOR"  # deprecated


class RELATED(StrEnum):
    """Enum for RELATED from :rfc:`5545`.

    Values:
        ``START``,
        ``END``

    Purpose:
        To specify the relationship of the alarm trigger with
        respect to the start or end of the calendar component.

    Description:
        This parameter can be specified on properties that
        specify an alarm trigger with a "DURATION" value type.  The
        parameter specifies whether the alarm will trigger relative to the
        start or end of the calendar component.  The parameter value START
        will set the alarm to trigger off the start of the calendar
        component; the parameter value END will set the alarm to trigger
        off the end of the calendar component.  If the parameter is not
        specified on an allowable property, then the default is START.
    """

    START = "START"
    END = "END"


class ROLE(StrEnum):
    """Enum for ROLE from :rfc:`5545`.

    Values:
        ``CHAIR``,
        ``REQ_PARTICIPANT``,
        ``OPT_PARTICIPANT``,
        ``NON_PARTICIPANT``

    Purpose:
        To specify the participation role for the calendar user
        specified by the property.

    Description:
        This parameter can be specified on properties with a
        CAL-ADDRESS value type.  The parameter specifies the participation
        role for the calendar user specified by the property in the group
        schedule calendar component.  If not specified on a property that
        allows this parameter, the default value is REQ-PARTICIPANT.
        Applications MUST treat x-name and iana-token values they don't
        recognize the same way as they would the REQ-PARTICIPANT value.

    """

    CHAIR = "CHAIR"
    REQ_PARTICIPANT = "REQ-PARTICIPANT"
    OPT_PARTICIPANT = "OPT-PARTICIPANT"
    NON_PARTICIPANT = "NON-PARTICIPANT"


class VALUE(StrEnum):
    """VALUE datatypes as defined in :rfc:`5545`.

    Attributes: ``BOOLEAN``, ``CAL_ADDRESS``, ``DATE``, ``DATE_TIME``, ``DURATION``,
    ``FLOAT``, ``INTEGER``, ``PERIOD``, ``RECUR``, ``TEXT``, ``TIME``, ``URI``,
    ``UTC_OFFSET``

    Description:
        This parameter specifies the value type and format of
        the property value.  The property values MUST be of a single value
        type.  For example, a "RDATE" property cannot have a combination
        of DATE-TIME and TIME value types.

        If the property's value is the default value type, then this
        parameter need not be specified.  However, if the property's
        default value type is overridden by some other allowable value
        type, then this parameter MUST be specified.

        Applications MUST preserve the value data for x-name and iana-
        token values that they don't recognize without attempting to
        interpret or parse the value data.

    """

    BOOLEAN = "BOOLEAN"
    CAL_ADDRESS = "CAL-ADDRESS"
    DATE = "DATE"
    DATE_TIME = "DATE-TIME"
    DURATION = "DURATION"
    FLOAT = "FLOAT"
    INTEGER = "INTEGER"
    PERIOD = "PERIOD"
    RECUR = "RECUR"
    TEXT = "TEXT"
    TIME = "TIME"
    URI = "URI"
    UTC_OFFSET = "UTC-OFFSET"


class BUSYTYPE(StrEnum):
    """Enum for BUSYTYPE from :rfc:`7953`.

    Values:
        ``BUSY``,
        ``BUSY_UNAVAILABLE``,
        ``BUSY_TENTATIVE``

    Description:
        This property is used to specify the default busy time
        type.  The values correspond to those used by the :class:`FBTYPE`
        parameter used on a "FREEBUSY" property, with the exception that
        the "FREE" value is not used in this property.  If not specified
        on a component that allows this property, the default is "BUSY-
        UNAVAILABLE".

    Example:
        The following is an example of this property:

        .. code-block:: text

            BUSYTYPE:BUSY
    """

    BUSY = "BUSY"
    BUSY_UNAVAILABLE = "BUSY-UNAVAILABLE"
    BUSY_TENTATIVE = "BUSY-TENTATIVE"


class CLASS(StrEnum):
    """Enum for CLASS from :rfc:`5545`.

    Values:
        ``PUBLIC``,
        ``PRIVATE``,
        ``CONFIDENTIAL``

    Description:
        An access classification is only one component of the
        general security system within a calendar application.  It
        provides a method of capturing the scope of the access the
        calendar owner intends for information within an individual
        calendar entry.  The access classification of an individual
        iCalendar component is useful when measured along with the other
        security components of a calendar system (e.g., calendar user
        authentication, authorization, access rights, access role, etc.).
        Hence, the semantics of the individual access classifications
        cannot be completely defined by this memo alone.  Additionally,
        due to the "blind" nature of most exchange processes using this
        memo, these access classifications cannot serve as an enforcement
        statement for a system receiving an iCalendar object.  Rather,
        they provide a method for capturing the intention of the calendar
        owner for the access to the calendar component.  If not specified
        in a component that allows this property, the default value is
        PUBLIC.  Applications MUST treat x-name and iana-token values they
        don't recognize the same way as they would the PRIVATE value.
    """

    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    CONFIDENTIAL = "CONFIDENTIAL"


class TRANSP(StrEnum):
    """Enum for TRANSP from :rfc:`5545`.

    Values:
        ``OPAQUE``,
        ``TRANSPARENT``

    Description:
        Time Transparency is the characteristic of an event
        that determines whether it appears to consume time on a calendar.
        Events that consume actual time for the individual or resource
        associated with the calendar SHOULD be recorded as OPAQUE,
        allowing them to be detected by free/busy time searches.  Other
        events, which do not take up the individual's (or resource's) time
        SHOULD be recorded as TRANSPARENT, making them invisible to free/
        busy time searches.
    """

    OPAQUE = "OPAQUE"
    TRANSPARENT = "TRANSPARENT"


__all__ = [
    "BUSYTYPE",
    "CLASS",
    "CUTYPE",
    "FBTYPE",
    "PARTSTAT",
    "RANGE",
    "RELATED",
    "RELTYPE",
    "ROLE",
    "STATUS",
    "TRANSP",
    "VALUE",
]
