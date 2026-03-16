# encoding: utf-8

"""Base shape-related objects such as BaseShape."""

from __future__ import absolute_import, division, print_function, unicode_literals

from pptx.action import ActionSetting
from pptx.dml.effect import ShadowFormat
from pptx.shared import ElementProxy
from pptx.util import lazyproperty


class BaseShape(object):
    """Base class for shape objects.

    Subclasses include |Shape|, |Picture|, and |GraphicFrame|.
    """

    def __init__(self, shape_elm, parent):
        super(BaseShape, self).__init__()
        self._element = shape_elm
        self._parent = parent

    def __eq__(self, other):
        """|True| if this shape object proxies the same element as *other*.

        Equality for proxy objects is defined as referring to the same XML
        element, whether or not they are the same proxy object instance.
        """
        if not isinstance(other, BaseShape):
            return False
        return self._element is other._element

    def __ne__(self, other):
        if not isinstance(other, BaseShape):
            return True
        return self._element is not other._element

    @lazyproperty
    def click_action(self):
        """|ActionSetting| instance providing access to click behaviors.

        Click behaviors are hyperlink-like behaviors including jumping to
        a hyperlink (web page) or to another slide in the presentation. The
        click action is that defined on the overall shape, not a run of text
        within the shape. An |ActionSetting| object is always returned, even
        when no click behavior is defined on the shape.
        """
        cNvPr = self._element._nvXxPr.cNvPr
        return ActionSetting(cNvPr, self)

    @property
    def element(self):
        """`lxml` element for this shape, e.g. a CT_Shape instance.

        Note that manipulating this element improperly can produce an invalid
        presentation file. Make sure you know what you're doing if you use
        this to change the underlying XML.
        """
        return self._element

    @property
    def has_chart(self):
        """
        |True| if this shape is a graphic frame containing a chart object.
        |False| otherwise. When |True|, the chart object can be accessed
        using the ``.chart`` property.
        """
        # This implementation is unconditionally False, the True version is
        # on GraphicFrame subclass.
        return False

    @property
    def has_table(self):
        """
        |True| if this shape is a graphic frame containing a table object.
        |False| otherwise. When |True|, the table object can be accessed
        using the ``.table`` property.
        """
        # This implementation is unconditionally False, the True version is
        # on GraphicFrame subclass.
        return False

    @property
    def has_text_frame(self):
        """
        |True| if this shape can contain text.
        """
        # overridden on Shape to return True. Only <p:sp> has text frame
        return False

    @property
    def height(self):
        """
        Read/write. Integer distance between top and bottom extents of shape
        in EMUs
        """
        return self._element.cy

    @height.setter
    def height(self, value):
        self._element.cy = value

    @property
    def is_placeholder(self):
        """
        True if this shape is a placeholder. A shape is a placeholder if it
        has a <p:ph> element.
        """
        return self._element.has_ph_elm

    @property
    def left(self):
        """
        Read/write. Integer distance of the left edge of this shape from the
        left edge of the slide, in English Metric Units (EMU)
        """
        return self._element.x

    @left.setter
    def left(self, value):
        self._element.x = value

    @property
    def name(self):
        """
        Name of this shape, e.g. 'Picture 7'
        """
        return self._element.shape_name

    @name.setter
    def name(self, value):
        self._element._nvXxPr.cNvPr.name = value

    @property
    def part(self):
        """The package part containing this shape.

        A |BaseSlidePart| subclass in this case. Access to a slide part
        should only be required if you are extending the behavior of |pp| API
        objects.
        """
        return self._parent.part

    @property
    def placeholder_format(self):
        """
        A |_PlaceholderFormat| object providing access to
        placeholder-specific properties such as placeholder type. Raises
        |ValueError| on access if the shape is not a placeholder.
        """
        if not self.is_placeholder:
            raise ValueError("shape is not a placeholder")
        return _PlaceholderFormat(self._element.ph)

    @property
    def rotation(self):
        """
        Read/write float. Degrees of clockwise rotation. Negative values can
        be assigned to indicate counter-clockwise rotation, e.g. assigning
        -45.0 will change setting to 315.0.
        """
        return self._element.rot

    @rotation.setter
    def rotation(self, value):
        self._element.rot = value

    @lazyproperty
    def shadow(self):
        """|ShadowFormat| object providing access to shadow for this shape.

        A |ShadowFormat| object is always returned, even when no shadow is
        explicitly defined on this shape (i.e. it inherits its shadow
        behavior).
        """
        return ShadowFormat(self._element.spPr)

    @property
    def shape_id(self):
        """Read-only positive integer identifying this shape.

        The id of a shape is unique among all shapes on a slide.
        """
        return self._element.shape_id

    @property
    def shape_type(self):
        """
        Unique integer identifying the type of this shape, like
        ``MSO_SHAPE_TYPE.CHART``. Must be implemented by subclasses.
        """
        # # This one returns |None| unconditionally to account for shapes
        # # that haven't been implemented yet, like group shape and chart.
        # # Once those are done this should raise |NotImplementedError|.
        # msg = 'shape_type property must be implemented by subclasses'
        # raise NotImplementedError(msg)
        return None

    @property
    def top(self):
        """
        Read/write. Integer distance of the top edge of this shape from the
        top edge of the slide, in English Metric Units (EMU)
        """
        return self._element.y

    @top.setter
    def top(self, value):
        self._element.y = value

    @property
    def width(self):
        """
        Read/write. Integer distance between left and right extents of shape
        in EMUs
        """
        return self._element.cx

    @width.setter
    def width(self, value):
        self._element.cx = value


class _PlaceholderFormat(ElementProxy):
    """
    Accessed via the :attr:`~.BaseShape.placeholder_format` property of
    a placeholder shape, provides properties specific to placeholders, such
    as the placeholder type.
    """

    @property
    def element(self):
        """
        The `p:ph` element proxied by this object.
        """
        return super(_PlaceholderFormat, self).element

    @property
    def idx(self):
        """
        Integer placeholder 'idx' attribute.
        """
        return self._element.idx

    @property
    def type(self):
        """
        Placeholder type, a member of the :ref:`PpPlaceholderType`
        enumeration, e.g. PP_PLACEHOLDER.CHART
        """
        return self._element.type
