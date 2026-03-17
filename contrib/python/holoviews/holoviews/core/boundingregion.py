"""Bounding regions and bounding boxes.

File originally part of the Topographica project.

"""
### JABALERT: The aarect information should probably be rewritten in
### matrix notation, not list notation, so that it can be scaled,
### translated, etc. easily.
###
from param.parameterized import get_occupied_slots

from .util import datetime_types


class BoundingRegion:
    """Abstract bounding region class, for any portion of a 2D plane.

    Only subclasses can be instantiated directly.

    """

    __abstract = True

    __slots__ = ['_aarect']


    def contains(self, x, y):
        raise NotImplementedError


    def __contains__(self, point):
        (x, y) = point
        return self.contains(x, y)


    def scale(self, xs, ys):
        raise NotImplementedError


    def translate(self, xoff, yoff):
        l, b, r, t = self.aarect().lbrt()
        self._aarect = AARectangle((l + xoff, b + yoff), (r + xoff, t + yoff))


    def rotate(self, theta):
        raise NotImplementedError


    def aarect(self):
        raise NotImplementedError


    def centroid(self):
        """Return the coordinates of the center of this BoundingBox

        """
        return self.aarect().centroid()


    def set(self, points):
        self._aarect = AARectangle(*points)


    # CEBALERT: same as methods on Parameter
    def __getstate__(self):
        # BoundingRegions have slots, not a dict, so we have to
        # support pickle and deepcopy ourselves.
        state = {}
        for slot in get_occupied_slots(self):
            state[slot] = getattr(self, slot)
        return state


    def __setstate__(self, state):
        for (k, v) in state.items():
            setattr(self, k, v)


class BoundingBox(BoundingRegion):
    """A rectangular bounding box defined either by two points forming
    an axis-aligned rectangle (or simply a radius for a square).

    """

    __slots__ = []


    def __str__(self):
        """Return BoundingBox(points=((left,bottom),(right,top)))

        Reimplemented here so that 'print' for a BoundingBox
        will display the bounds.

        """
        l, b, r, t = self._aarect.lbrt()
        if (not isinstance(r, datetime_types) and r == -l and
            not isinstance(b, datetime_types) and t == -b and r == t):
            return f'BoundingBox(radius={r})'
        else:
            return f'BoundingBox(points=(({l},{b}),({r},{t})))'


    def __repr__(self):
        return self.__str__()


    def script_repr(self, imports=None, prefix="    "):
        # Generate import statement
        if imports is None:
            imports = []
        cls = self.__class__.__name__
        mod = self.__module__
        imports.append(f"from {mod} import {cls}")
        return self.__str__()


    def __init__(self, **args):
        """Create a BoundingBox.

        Either 'radius' or 'points' can be specified for the AARectangle.

        If neither radius nor points is passed in, create a default
        AARectangle defined by (-0.5,-0.5),(0.5,0.5).

        """
        # if present, 'radius', 'min_radius', and 'points' are deleted from
        # args before they're passed to the superclass (because they
        # aren't parameters to be set)
        if 'radius' in args:
            r = args['radius']
            del args['radius']

            self._aarect = AARectangle((-r, -r), (r, r))

        elif 'points' in args:
            self._aarect = AARectangle(*args['points'])
            del args['points']
        else:
            self._aarect = AARectangle((-0.5, -0.5), (0.5, 0.5))

        super().__init__(**args)


    def __contains__(self, other):
        if isinstance(other, BoundingBox):
            return self.containsbb_inclusive(other)
        (x, y) = other
        return self.contains(x, y)


    def contains(self, x, y):
        """Returns true if the given point is contained within the
        bounding box, where all boundaries of the box are
        considered to be inclusive.

        """
        left, bottom, right, top = self.aarect().lbrt()
        return (left <= x <= right) and (bottom <= y <= top)


    def contains_exclusive(self, x, y):
        """Return True if the given point is contained within the
        bounding box, where the bottom and right boundaries are
        considered exclusive.

        """
        left, bottom, right, top = self._aarect.lbrt()
        return (left <= x < right) and (bottom < y <= top)


    def containsbb_exclusive(self, x):
        """Returns true if the given BoundingBox x is contained within the
        bounding box, where at least one of the boundaries of the box has
        to be exclusive.

        """
        left, bottom, right, top = self.aarect().lbrt()
        leftx, bottomx, rightx, topx = x.aarect().lbrt()
        return (left <= leftx) and (bottom <= bottomx) and (right >= rightx) and (top >= topx) and\
               (not ((left == leftx) and (bottom == bottomx) and (right == rightx) and (top == topx)))


    def containsbb_inclusive(self, x):
        """Returns true if the given BoundingBox x is contained within the
        bounding box, including cases of exact match.

        """
        left, bottom, right, top = self.aarect().lbrt()
        leftx, bottomx, rightx, topx = x.aarect().lbrt()
        return (left <= leftx) and (bottom <= bottomx) and (
        right >= rightx) and (top >= topx)


    def upperexclusive_contains(self, x, y):
        """Returns true if the given point is contained within the
        bounding box, where the right and upper boundaries
        are exclusive, and the left and lower boundaries are
        inclusive.  Useful for tiling a plane into non-overlapping
        regions.

        """
        left, bottom, right, top = self.aarect().lbrt()
        return (left <= x < right) and (bottom <= y < top)


    def aarect(self):
        return self._aarect


    def lbrt(self):
        """Return left,bottom,right,top values for the BoundingBox.

        """
        return self._aarect.lbrt()


    def __eq__(self, other):
        if isinstance(self, BoundingBox) and isinstance(other, BoundingBox):
            return self.lbrt() == other.lbrt()
        else:
            return False

    def __hash__(self):
        return hash(self.aarect)



class BoundingEllipse(BoundingBox):
    """Similar to BoundingBox, but the region is the ellipse
    inscribed within the rectangle.

    """

    __slots__ = []


    def contains(self, x, y):
        left, bottom, right, top = self.aarect().lbrt()
        xr = (right - left) / 2.0
        yr = (top - bottom) / 2.0
        xc = left + xr
        yc = bottom + yr

        xd = x - xc
        yd = y - yc

        return (xd ** 2 / xr ** 2 + yd ** 2 / yr ** 2) <= 1


# JABALERT: Should probably remove top, bottom, etc. accessor functions,
# and use the slot itself instead.
###################################################
class AARectangle:
    """Axis-aligned rectangle class.

    Defines the smallest axis-aligned rectangle that encloses a set of
    points.

    Examples
    --------
    >>> aar = AARectangle( (x1,y1),(x2,y2), ... , (xN,yN) )

    """

    __slots__ = ['_bottom', '_left', '_right', '_top']


    def __init__(self, *points):
        self._top = max([y for x, y in points])
        self._bottom = min([y for x, y in points])
        self._left = min([x for x, y in points])
        self._right = max([x for x, y in points])


    # support for pickling because this class has __slots__ rather
    # than __dict__
    def __getstate__(self):
        state = {}
        for k in self.__slots__:
            state[k] = getattr(self, k)
        return state


    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)


    def top(self):
        """Return the y-coordinate of the top of the rectangle.

        """
        return self._top


    def bottom(self):
        """Return the y-coordinate of the bottom of the rectangle.

        """
        return self._bottom


    def left(self):
        """Return the x-coordinate of the left side of the rectangle.

        """
        return self._left


    def right(self):
        """Return the x-coordinate of the right side of the rectangle.

        """
        return self._right


    def lbrt(self):
        """Return (left,bottom,right,top) as a tuple.

        """
        return self._left, self._bottom, self._right, self._top


    def centroid(self):
        """Return the centroid of the rectangle.

        """
        left, bottom, right, top = self.lbrt()
        return (right + left) / 2.0, (top + bottom) / 2.0


    def intersect(self, other):
        l1, b1, r1, t1 = self.lbrt()
        l2, b2, r2, t2 = other.lbrt()

        l = max(l1, l2)
        b = max(b1, b2)
        r = min(r1, r2)
        t = min(t1, t2)

        return AARectangle(points=((l, b), (r, t)))


    def width(self):
        return self._right - self._left


    def height(self):
        return self._top - self._bottom


    def empty(self):
        l, b, r, t = self.lbrt()
        return (r <= l) or (t <= b)
