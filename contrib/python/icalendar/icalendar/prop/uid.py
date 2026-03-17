"""UID values from :rfc:`9253`."""

import uuid
from typing import ClassVar

from icalendar.compatibility import Self

from .text import vText


class vUid(vText):
    """A UID of a component.

    This is defined in :rfc:`9253`, Section 7.
    """

    default_value: ClassVar[str] = "UID"

    @classmethod
    def new(cls) -> Self:
        """Create a new UID for convenience.

        .. code-block:: pycon

            >>> from icalendar import vUid
            >>> vUid.new()
            vUid('d755cef5-2311-46ed-a0e1-6733c9e15c63')

        """
        return vUid(uuid.uuid4())

    @property
    def uid(self) -> str:
        """The UID of this property."""
        return str(self)

    @property
    def ical_value(self) -> str:
        """The UID of this property."""
        return self.uid

    def __repr__(self) -> str:
        """repr(self)"""
        return f"{self.__class__.__name__}({self.uid!r})"

    from icalendar.param import FMTTYPE, LABEL, LINKREL

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vUid."""
        return [cls("d755cef5-2311-46ed-a0e1-6733c9e15c63")]


__all__ = ["vUid"]
