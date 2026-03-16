# encoding: utf-8

"""GroupShape and related objects."""

from __future__ import absolute_import, division, print_function, unicode_literals

from pptx.dml.effect import ShadowFormat
from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.shapes.base import BaseShape
from pptx.util import lazyproperty


class GroupShape(BaseShape):
    """A shape that acts as a container for other shapes."""

    @property
    def click_action(self):
        """Unconditionally raises `TypeError`.

        A group shape cannot have a click action or hover action.
        """
        raise TypeError("a group shape cannot have a click action")

    @property
    def has_text_frame(self):
        """Unconditionally |False|.

        A group shape does not have a textframe and cannot itself contain
        text. This does not impact the ability of shapes contained by the
        group to each have their own text.
        """
        return False

    @lazyproperty
    def shadow(self):
        """|ShadowFormat| object representing shadow effect for this group.

        A |ShadowFormat| object is always returned, even when no shadow is
        explicitly defined on this group shape (i.e. when the group inherits
        its shadow behavior).
        """
        return ShadowFormat(self._element.grpSpPr)

    @property
    def shape_type(self):
        """Member of :ref:`MsoShapeType` identifying the type of this shape.

        Unconditionally `MSO_SHAPE_TYPE.GROUP` in this case
        """
        return MSO_SHAPE_TYPE.GROUP

    @lazyproperty
    def shapes(self):
        """|GroupShapes| object for this group.

        The |GroupShapes| object provides access to the group's member shapes
        and provides methods for adding new ones.
        """
        from pptx.shapes.shapetree import GroupShapes

        return GroupShapes(self._element, self)
