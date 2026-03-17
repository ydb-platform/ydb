"""GroupShape and related objects."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pptx.dml.effect import ShadowFormat
from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.shapes.base import BaseShape
from pptx.util import lazyproperty

if TYPE_CHECKING:
    from pptx.action import ActionSetting
    from pptx.oxml.shapes.groupshape import CT_GroupShape
    from pptx.shapes.shapetree import GroupShapes
    from pptx.types import ProvidesPart


class GroupShape(BaseShape):
    """A shape that acts as a container for other shapes."""

    def __init__(self, grpSp: CT_GroupShape, parent: ProvidesPart):
        super().__init__(grpSp, parent)
        self._grpSp = grpSp

    @lazyproperty
    def click_action(self) -> ActionSetting:
        """Unconditionally raises `TypeError`.

        A group shape cannot have a click action or hover action.
        """
        raise TypeError("a group shape cannot have a click action")

    @property
    def has_text_frame(self) -> bool:
        """Unconditionally |False|.

        A group shape does not have a textframe and cannot itself contain text. This does not
        impact the ability of shapes contained by the group to each have their own text.
        """
        return False

    @lazyproperty
    def shadow(self) -> ShadowFormat:
        """|ShadowFormat| object representing shadow effect for this group.

        A |ShadowFormat| object is always returned, even when no shadow is explicitly defined on
        this group shape (i.e. when the group inherits its shadow behavior).
        """
        return ShadowFormat(self._grpSp.grpSpPr)

    @property
    def shape_type(self) -> MSO_SHAPE_TYPE:
        """Member of :ref:`MsoShapeType` identifying the type of this shape.

        Unconditionally `MSO_SHAPE_TYPE.GROUP` in this case
        """
        return MSO_SHAPE_TYPE.GROUP

    @lazyproperty
    def shapes(self) -> GroupShapes:
        """|GroupShapes| object for this group.

        The |GroupShapes| object provides access to the group's member shapes and provides methods
        for adding new ones.
        """
        from pptx.shapes.shapetree import GroupShapes

        return GroupShapes(self._element, self)
