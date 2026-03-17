"""Objects used across sub-package."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pptx.opc.package import XmlPart
    from pptx.types import ProvidesPart


class Subshape(object):
    """Provides access to the containing part for drawing elements that occur below a shape.

    Access to the part is required for example to add or drop a relationship. Provides
    `self._parent` attribute to subclasses.
    """

    def __init__(self, parent: ProvidesPart):
        super(Subshape, self).__init__()
        self._parent = parent

    @property
    def part(self) -> XmlPart:
        """The package part containing this object."""
        return self._parent.part
