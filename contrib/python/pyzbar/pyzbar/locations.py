from collections import namedtuple
from itertools import chain
from operator import itemgetter


__all__ = ['bounding_box', 'convex_hull', 'Point', 'Rect']


Point = namedtuple('Point', ['x', 'y'])
Rect = namedtuple('Rect', ['left', 'top', 'width', 'height'])


def bounding_box(locations):
    """Computes the bounding box of an iterable of (x, y) coordinates.

    Args:
        locations: iterable of (x, y) tuples.

    Returns:
        `Rect`: Coordinates of the bounding box.
    """
    x_values = list(map(itemgetter(0), locations))
    x_min, x_max = min(x_values), max(x_values)
    y_values = list(map(itemgetter(1), locations))
    y_min, y_max = min(y_values), max(y_values)
    return Rect(x_min, y_min, x_max - x_min, y_max - y_min)


def convex_hull(points):
    """Computes the convex hull of an iterable of (x, y) coordinates.

    Args:
        points: iterable of (x, y) tuples.

    Returns:
        `list`: instances of `Point` - vertices of the convex hull in
        counter-clockwise order, starting from the vertex with the
        lexicographically smallest coordinates.

    Andrew's monotone chain algorithm. O(n log n) complexity.
    https://en.wikibooks.org/wiki/Algorithm_Implementation/Geometry/Convex_hull/Monotone_chain
    """

    def is_not_clockwise(p0, p1, p2):
        return 0 <= (
            (p1[0] - p0[0]) * (p2[1] - p0[1]) -
            (p1[1] - p0[1]) * (p2[0] - p0[0])
        )

    def go(points_):
        res = []
        for p in points_:
            while 1 < len(res) and is_not_clockwise(res[-2], res[-1], p):
                res.pop()
            res.append(p)

        # The last point in each list is the first point in the other list
        res.pop()

        return res

    # Discard duplicates and sort by x then y
    points = sorted(set(points))

    # Algorithm needs at least two points
    hull = (
        points if len(points) < 2 else chain(go(points), go(reversed(points)))
    )

    return list(map(Point._make, hull))
