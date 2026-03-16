"""Abstract types used by `python-pptx`."""

from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import Protocol

if TYPE_CHECKING:
    from pptx.opc.package import XmlPart
    from pptx.util import Length


class ProvidesExtents(Protocol):
    """An object that has width and height."""

    @property
    def height(self) -> Length:
        """Distance between top and bottom extents of shape in EMUs."""
        ...

    @property
    def width(self) -> Length:
        """Distance between left and right extents of shape in EMUs."""
        ...


class ProvidesPart(Protocol):
    """An object that provides access to its XmlPart.

    This type is for objects that need access to their part, possibly because they need access to
    the package or related parts.
    """

    @property
    def part(self) -> XmlPart: ...
