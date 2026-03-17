"""This module contains the parser/generators (or coders/encoders if you
prefer) for the classes/datatypes that are used in iCalendar:

###########################################################################

# This module defines these property value data types and property parameters

4.2 Defined property parameters are:

.. code-block:: text

     ALTREP, CN, CUTYPE, DELEGATED-FROM, DELEGATED-TO, DIR, ENCODING, FMTTYPE,
     FBTYPE, LANGUAGE, MEMBER, PARTSTAT, RANGE, RELATED, RELTYPE, ROLE, RSVP,
     SENT-BY, TZID, VALUE

4.3 Defined value data types are:

.. code-block:: text

    BINARY, BOOLEAN, CAL-ADDRESS, DATE, DATE-TIME, DURATION, FLOAT, INTEGER,
    PERIOD, RECUR, TEXT, TIME, URI, UTC-OFFSET

###########################################################################

iCalendar properties have values. The values are strongly typed. This module
defines these types, calling val.to_ical() on them will render them as defined
in rfc5545.

If you pass any of these classes a Python primitive, you will have an object
that can render itself as iCalendar formatted date.

Property Value Data Types start with a 'v'. they all have an to_ical() and
from_ical() method. The to_ical() method generates a text string in the
iCalendar format. The from_ical() method can parse this format and return a
primitive Python datatype. So it should always be true that:

.. code-block:: python

    x == vDataType.from_ical(VDataType(x).to_ical())

These types are mainly used for parsing and file generation. But you can set
them directly.
"""

from typing import TypeAlias

from .adr import AdrFields, vAdr
from .binary import vBinary
from .boolean import vBoolean
from .broken import vBroken
from .cal_address import vCalAddress
from .categories import vCategory
from .dt import (
    DT_TYPE,
    TimeBase,
    vDate,
    vDatetime,
    vDDDLists,
    vDDDTypes,
    vDuration,
    vPeriod,
    vTime,
    vUTCOffset,
)
from .factory import TypesFactory
from .float import vFloat
from .geo import vGeo
from .inline import vInline
from .integer import vInt
from .n import NFields, vN
from .org import vOrg
from .recur import vFrequency, vMonth, vRecur, vSkip, vWeekday
from .text import vText
from .uid import vUid
from .unknown import vUnknown
from .uri import vUri
from .xml_reference import vXmlReference

VPROPERTY: TypeAlias = (
    vAdr
    | vBoolean
    | vBroken
    | vCalAddress
    | vCategory
    | vDDDLists
    | vDDDTypes
    | vDate
    | vDatetime
    | vDuration
    | vFloat
    | vFrequency
    | vInt
    | vMonth
    | vN
    | vOrg
    | vPeriod
    | vRecur
    | vSkip
    | vText
    | vTime
    | vUTCOffset
    | vUri
    | vWeekday
    | vInline
    | vBinary
    | vGeo
    | vUnknown
    | vXmlReference
    | vUid
)

__all__ = [
    "DT_TYPE",
    "VPROPERTY",
    "AdrFields",
    "NFields",
    "TimeBase",
    "TypesFactory",
    "vAdr",
    "vBinary",
    "vBoolean",
    "vBroken",
    "vCalAddress",
    "vCategory",
    "vDDDLists",
    "vDDDTypes",
    "vDate",
    "vDatetime",
    "vDuration",
    "vFloat",
    "vFrequency",
    "vGeo",
    "vInline",
    "vInt",
    "vMonth",
    "vN",
    "vOrg",
    "vPeriod",
    "vRecur",
    "vSkip",
    "vText",
    "vTime",
    "vUTCOffset",
    "vUid",
    "vUnknown",
    "vUri",
    "vWeekday",
    "vXmlReference",
]
