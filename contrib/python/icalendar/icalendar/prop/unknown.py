"""UNKNOWN values from :rfc:`7265`."""

from typing import ClassVar

from icalendar.compatibility import Self
from icalendar.prop.text import vText


class vUnknown(vText):
    """This is text but the VALUE parameter is unknown.

    Since :rfc:`7265`, it is important to record if values are unknown.
    For :rfc:`5545`, we could just assume TEXT.
    """

    default_value: ClassVar[str] = "UNKNOWN"

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vUnknown."""
        return [vUnknown("Some property text.")]

    from icalendar.param import VALUE


__all__ = ["vUnknown"]
