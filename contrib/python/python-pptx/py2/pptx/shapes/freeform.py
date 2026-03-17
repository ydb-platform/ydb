# encoding: utf-8

"""Objects related to construction of freeform shapes."""

from __future__ import absolute_import, division, print_function, unicode_literals

from pptx.compat import Sequence
from pptx.util import lazyproperty


class FreeformBuilder(Sequence):
    """Allows a freeform shape to be specified and created.

    The initial pen position is provided on construction. From there, drawing
    proceeds using successive calls to draw line segments. The freeform shape
    may be closed by calling the :meth:`close` method.

    A shape may have more than one contour, in which case overlapping areas
    are "subtracted". A contour is a sequence of line segments beginning with
    a "move-to" operation. A move-to operation is automatically inserted in
    each new freeform; additional move-to ops can be inserted with the
    `.move_to()` method.
    """

    def __init__(self, shapes, start_x, start_y, x_scale, y_scale):
        super(FreeformBuilder, self).__init__()
        self._shapes = shapes
        self._start_x = start_x
        self._start_y = start_y
        self._x_scale = x_scale
        self._y_scale = y_scale

    def __getitem__(self, idx):
        return self._drawing_operations.__getitem__(idx)

    def __iter__(self):
        return self._drawing_operations.__iter__()

    def __len__(self):
        return self._drawing_operations.__len__()

    @classmethod
    def new(cls, shapes, start_x, start_y, x_scale, y_scale):
        """Return a new |FreeformBuilder| object.

        The initial pen location is specified (in local coordinates) by
        (*start_x*, *start_y*).
        """
        return cls(shapes, int(round(start_x)), int(round(start_y)), x_scale, y_scale)

    def add_line_segments(self, vertices, close=True):
        """Add a straight line segment to each point in *vertices*.

        *vertices* must be an iterable of (x, y) pairs (2-tuples). Each x and
        y value is rounded to the nearest integer before use. The optional
        *close* parameter determines whether the resulting contour is
        *closed* or left *open*.

        Returns this |FreeformBuilder| object so it can be used in chained
        calls.
        """
        for x, y in vertices:
            self._add_line_segment(x, y)
        if close:
            self._add_close()
        return self

    def convert_to_shape(self, origin_x=0, origin_y=0):
        """Return new freeform shape positioned relative to specified offset.

        *origin_x* and *origin_y* locate the origin of the local coordinate
        system in slide coordinates (EMU), perhaps most conveniently by use
        of a |Length| object.

        Note that this method may be called more than once to add multiple
        shapes of the same geometry in different locations on the slide.
        """
        sp = self._add_freeform_sp(origin_x, origin_y)
        path = self._start_path(sp)
        for drawing_operation in self:
            drawing_operation.apply_operation_to(path)
        return self._shapes._shape_factory(sp)

    def move_to(self, x, y):
        """Move pen to (x, y) (local coordinates) without drawing line.

        Returns this |FreeformBuilder| object so it can be used in chained
        calls.
        """
        self._drawing_operations.append(_MoveTo.new(self, x, y))
        return self

    @property
    def shape_offset_x(self):
        """Return x distance of shape origin from local coordinate origin.

        The returned integer represents the leftmost extent of the freeform
        shape, in local coordinates. Note that the bounding box of the shape
        need not start at the local origin.
        """
        min_x = self._start_x
        for drawing_operation in self:
            if hasattr(drawing_operation, "x"):
                min_x = min(min_x, drawing_operation.x)
        return min_x

    @property
    def shape_offset_y(self):
        """Return y distance of shape origin from local coordinate origin.

        The returned integer represents the topmost extent of the freeform
        shape, in local coordinates. Note that the bounding box of the shape
        need not start at the local origin.
        """
        min_y = self._start_y
        for drawing_operation in self:
            if hasattr(drawing_operation, "y"):
                min_y = min(min_y, drawing_operation.y)
        return min_y

    def _add_close(self):
        """Add a close |_Close| operation to the drawing sequence."""
        self._drawing_operations.append(_Close.new())

    def _add_freeform_sp(self, origin_x, origin_y):
        """Add a freeform `p:sp` element having no drawing elements.

        *origin_x* and *origin_y* are specified in slide coordinates, and
        represent the location of the local coordinates origin on the slide.
        """
        spTree = self._shapes._spTree
        return spTree.add_freeform_sp(
            origin_x + self._left, origin_y + self._top, self._width, self._height
        )

    def _add_line_segment(self, x, y):
        """Add a |_LineSegment| operation to the drawing sequence."""
        self._drawing_operations.append(_LineSegment.new(self, x, y))

    @lazyproperty
    def _drawing_operations(self):
        """Return the sequence of drawing operation objects for freeform."""
        return []

    @property
    def _dx(self):
        """Return integer width of this shape's path in local units."""
        min_x = max_x = self._start_x
        for drawing_operation in self:
            if hasattr(drawing_operation, "x"):
                min_x = min(min_x, drawing_operation.x)
                max_x = max(max_x, drawing_operation.x)
        return max_x - min_x

    @property
    def _dy(self):
        """Return integer height of this shape's path in local units."""
        min_y = max_y = self._start_y
        for drawing_operation in self:
            if hasattr(drawing_operation, "y"):
                min_y = min(min_y, drawing_operation.y)
                max_y = max(max_y, drawing_operation.y)
        return max_y - min_y

    @property
    def _height(self):
        """Return vertical size of this shape's path in slide coordinates.

        This value is based on the actual extents of the shape and does not
        include any positioning offset.
        """
        return int(round(self._dy * self._y_scale))

    @property
    def _left(self):
        """Return leftmost extent of this shape's path in slide coordinates.

        Note that this value does not include any positioning offset; it
        assumes the drawing (local) coordinate origin is at (0, 0) on the
        slide.
        """
        return int(round(self.shape_offset_x * self._x_scale))

    def _local_to_shape(self, local_x, local_y):
        """Translate local coordinates point to shape coordinates.

        Shape coordinates have the same unit as local coordinates, but are
        offset such that the origin of the shape coordinate system (0, 0) is
        located at the top-left corner of the shape bounding box.
        """
        return (local_x - self.shape_offset_x, local_y - self.shape_offset_y)

    def _start_path(self, sp):
        """Return a newly created `a:path` element added to *sp*.

        The returned `a:path` element has an `a:moveTo` element representing
        the shape starting point as its only child.
        """
        path = sp.add_path(w=self._dx, h=self._dy)
        path.add_moveTo(*self._local_to_shape(self._start_x, self._start_y))
        return path

    @property
    def _top(self):
        """Return topmost extent of this shape's path in slide coordinates.

        Note that this value does not include any positioning offset; it
        assumes the drawing (local) coordinate origin is located at slide
        coordinates (0, 0) (top-left corner of slide).
        """
        return int(round(self.shape_offset_y * self._y_scale))

    @property
    def _width(self):
        """Return width of this shape's path in slide coordinates.

        This value is based on the actual extents of the shape path and does
        not include any positioning offset.
        """
        return int(round(self._dx * self._x_scale))


class _BaseDrawingOperation(object):
    """Base class for freeform drawing operations.

    A drawing operation has at least one location (x, y) in local
    coordinates.
    """

    def __init__(self, freeform_builder, x, y):
        super(_BaseDrawingOperation, self).__init__()
        self._freeform_builder = freeform_builder
        self._x = x
        self._y = y

    def apply_operation_to(self, path):
        """Add the XML element(s) implementing this operation to *path*.

        Must be implemented by each subclass.
        """
        raise NotImplementedError("must be implemented by each subclass")

    @property
    def x(self):
        """Return the horizontal (x) target location of this operation.

        The returned value is an integer in local coordinates.
        """
        return self._x

    @property
    def y(self):
        """Return the vertical (y) target location of this operation.

        The returned value is an integer in local coordinates.
        """
        return self._y


class _Close(object):
    """Specifies adding a `<a:close/>` element to the current contour."""

    @classmethod
    def new(cls):
        """Return a new _Close object."""
        return cls()

    def apply_operation_to(self, path):
        """Add `a:close` element to *path*."""
        return path.add_close()


class _LineSegment(_BaseDrawingOperation):
    """Specifies a straight line segment ending at the specified point."""

    @classmethod
    def new(cls, freeform_builder, x, y):
        """Return a new _LineSegment object ending at point *(x, y)*.

        Both *x* and *y* are rounded to the nearest integer before use.
        """
        return cls(freeform_builder, int(round(x)), int(round(y)))

    def apply_operation_to(self, path):
        """Add `a:lnTo` element to *path* for this line segment.

        Returns the `a:lnTo` element newly added to the path.
        """
        return path.add_lnTo(
            self._x - self._freeform_builder.shape_offset_x,
            self._y - self._freeform_builder.shape_offset_y,
        )


class _MoveTo(_BaseDrawingOperation):
    """Specifies a new pen position."""

    @classmethod
    def new(cls, freeform_builder, x, y):
        """Return a new _MoveTo object for move to point *(x, y)*.

        Both *x* and *y* are rounded to the nearest integer before use.
        """
        return cls(freeform_builder, int(round(x)), int(round(y)))

    def apply_operation_to(self, path):
        """Add `a:moveTo` element to *path* for this line segment."""
        return path.add_moveTo(
            self._x - self._freeform_builder.shape_offset_x,
            self._y - self._freeform_builder.shape_offset_y,
        )
