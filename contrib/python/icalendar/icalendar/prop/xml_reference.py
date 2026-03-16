"""XML-REFERENCE values from :rfc:`9253`."""

from typing import ClassVar
from urllib.parse import unquote, urlparse

from icalendar.compatibility import Self
from icalendar.prop.uri import vUri


class vXmlReference(vUri):
    """An XML-REFERENCE.

    The associated value references an associated XML artifact and
    is a URI with an XPointer anchor value.

    This is defined in :rfc:`9253`, Section 7.
    """

    default_value: ClassVar[str] = "XML-REFERENCE"

    @property
    def xml_reference(self) -> str:
        """The XML reference URI of this property."""
        return self.uri

    @property
    def x_pointer(self) -> str | None:
        """The XPointer of the URI.

        The XPointer is defined in `W3C.WD-xptr-xpointer-20021219
        <https://www.rfc-editor.org/rfc/rfc9253.html#W3C.WD-xptr-xpointer-20021219>`_,
        and its use as an anchor is defined in `W3C.REC-xptr-framework-20030325
        <https://www.rfc-editor.org/rfc/rfc9253.html#W3C.REC-xptr-framework-20030325>`_.

        Returns:
            The decoded x-pointer or ``None`` if no valid x-pointer is found.
        """
        parsed = urlparse(self.xml_reference)
        fragment = unquote(parsed.fragment)
        if not fragment.startswith("xpointer(") or not fragment.endswith(")"):
            return None
        return fragment[9:-1]

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vXmlReference."""
        return [cls("http://example.com/doc.xml#xpointer(/doc/element)")]


__all__ = ["vXmlReference"]
