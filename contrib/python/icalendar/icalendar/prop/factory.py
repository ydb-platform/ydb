"""Factory class for all the property types."""

from __future__ import annotations

from typing import ClassVar

from icalendar.caselessdict import CaselessDict
from icalendar.prop.adr import vAdr
from icalendar.prop.binary import vBinary
from icalendar.prop.boolean import vBoolean
from icalendar.prop.cal_address import vCalAddress
from icalendar.prop.categories import vCategory
from icalendar.prop.dt import (
    vDate,
    vDatetime,
    vDDDLists,
    vDDDTypes,
    vDuration,
    vPeriod,
    vTime,
    vUTCOffset,
)
from icalendar.prop.float import vFloat
from icalendar.prop.geo import vGeo
from icalendar.prop.inline import vInline
from icalendar.prop.n import vN
from icalendar.prop.org import vOrg
from icalendar.prop.recur import vFrequency, vRecur, vWeekday
from icalendar.prop.text import vText
from icalendar.prop.uid import vUid
from icalendar.prop.unknown import vUnknown
from icalendar.prop.uri import vUri
from icalendar.prop.xml_reference import vXmlReference

from .integer import vInt


class TypesFactory(CaselessDict):
    """Factory for all value types defined in :rfc:`5545` and subsequent.

    The value and parameter names don't overlap. So one factory is enough for
    both kinds.
    """

    _instance: ClassVar[TypesFactory | None] = None

    def instance() -> TypesFactory:
        """Return a singleton instance of this class."""
        if TypesFactory._instance is None:
            TypesFactory._instance = TypesFactory()
        return TypesFactory._instance

    def __init__(self, *args, **kwargs):
        """Set keys to upper for initial dict"""
        super().__init__(*args, **kwargs)
        self.all_types = (
            vBinary,
            vBoolean,
            vCalAddress,
            vDDDLists,
            vDDDTypes,
            vDate,
            vDatetime,
            vDuration,
            vFloat,
            vFrequency,
            vGeo,
            vInline,
            vInt,
            vPeriod,
            vRecur,
            vText,
            vTime,
            vUTCOffset,
            vUri,
            vWeekday,
            vCategory,
            vAdr,
            vN,
            vOrg,
            vUid,
            vXmlReference,
            vUnknown,
        )
        self["binary"] = vBinary
        self["boolean"] = vBoolean
        self["cal-address"] = vCalAddress
        self["date"] = vDDDTypes
        self["date-time"] = vDDDTypes
        self["duration"] = vDDDTypes
        self["float"] = vFloat
        self["integer"] = vInt
        self["period"] = vPeriod
        self["recur"] = vRecur
        self["text"] = vText
        self["time"] = vTime
        self["uri"] = vUri
        self["utc-offset"] = vUTCOffset
        self["geo"] = vGeo
        self["inline"] = vInline
        self["date-time-list"] = vDDDLists
        self["categories"] = vCategory
        self["adr"] = vAdr  # RFC 6350 vCard
        self["n"] = vN  # RFC 6350 vCard
        self["org"] = vOrg  # RFC 6350 vCard
        self["unknown"] = vUnknown  # RFC 7265
        self["uid"] = vUid  # RFC 9253
        self["xml-reference"] = vXmlReference  # RFC 9253

    #################################################
    # Property types

    # These are the default types
    types_map = CaselessDict(
        {
            ####################################
            # Property value types
            # Calendar Properties
            "calscale": "text",
            "method": "text",
            "prodid": "text",
            "version": "text",
            # Descriptive Component Properties
            "attach": "uri",
            "categories": "categories",
            "class": "text",
            # vCard Properties (RFC 6350)
            "adr": "adr",
            "n": "n",
            "org": "org",
            "comment": "text",
            "description": "text",
            "geo": "geo",
            "location": "text",
            "percent-complete": "integer",
            "priority": "integer",
            "resources": "text",
            "status": "text",
            "summary": "text",
            # RFC 9253
            # link should be uri, xml-reference or uid
            # uri is likely most helpful if people forget to set VALUE
            "link": "uri",
            "concept": "uri",
            "refid": "text",
            # Date and Time Component Properties
            "completed": "date-time",
            "dtend": "date-time",
            "due": "date-time",
            "dtstart": "date-time",
            "duration": "duration",
            "freebusy": "period",
            "transp": "text",
            "refresh-interval": "duration",  # RFC 7986
            # Time Zone Component Properties
            "tzid": "text",
            "tzname": "text",
            "tzoffsetfrom": "utc-offset",
            "tzoffsetto": "utc-offset",
            "tzurl": "uri",
            # Relationship Component Properties
            "attendee": "cal-address",
            "contact": "text",
            "organizer": "cal-address",
            "recurrence-id": "date-time",
            "related-to": "text",
            "url": "uri",
            "conference": "uri",  # RFC 7986
            "source": "uri",
            "uid": "text",
            # Recurrence Component Properties
            "exdate": "date-time-list",
            "exrule": "recur",
            "rdate": "date-time-list",
            "rrule": "recur",
            # Alarm Component Properties
            "action": "text",
            "repeat": "integer",
            "trigger": "duration",
            "acknowledged": "date-time",
            # Change Management Component Properties
            "created": "date-time",
            "dtstamp": "date-time",
            "last-modified": "date-time",
            "sequence": "integer",
            # Miscellaneous Component Properties
            "request-status": "text",
            ####################################
            # parameter types (luckily there is no name overlap)
            "altrep": "uri",
            "cn": "text",
            "cutype": "text",
            "delegated-from": "cal-address",
            "delegated-to": "cal-address",
            "dir": "uri",
            "encoding": "text",
            "fmttype": "text",
            "fbtype": "text",
            "language": "text",
            "member": "cal-address",
            "partstat": "text",
            "range": "text",
            "related": "text",
            "reltype": "text",
            "role": "text",
            "rsvp": "boolean",
            "sent-by": "cal-address",
            "value": "text",
            # rfc 9253 parameters
            "label": "text",
            "linkrel": "text",
            "gap": "duration",
        }
    )

    def for_property(self, name, value_param: str | None = None) -> type:
        """Returns the type class for a property or parameter.

        Parameters:
            name: Property or parameter name
            value_param: Optional ``VALUE`` parameter, for example,
                "DATE", "DATE-TIME", or other string.

        Returns:
            The appropriate value type class.
        """
        # Special case: RDATE and EXDATE always use vDDDLists to support list values
        # regardless of the VALUE parameter
        if name.upper() in ("RDATE", "EXDATE"):
            return self["date-time-list"]

        # Only use VALUE parameter for known properties that support multiple value
        # types (like DTSTART, DTEND, etc. which can be DATE or DATE-TIME)
        # For unknown/custom properties, always use the default type from types_map
        if value_param and name in self.types_map and value_param in self:
            return self[value_param]
        return self[self.types_map.get(name, "unknown")]

    def to_ical(self, name, value):
        """Encodes a named value from a primitive python type to an icalendar
        encoded string.
        """
        type_class = self.for_property(name)
        return type_class(value).to_ical()

    def from_ical(self, name, value):
        """Decodes a named property or parameter value from an icalendar
        encoded string to a primitive python type.
        """
        type_class = self.for_property(name)
        return type_class.from_ical(value)


__all__ = ["TypesFactory"]
