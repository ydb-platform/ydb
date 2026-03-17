"""Bounding box tuple, and disjoint operator."""

from collections import namedtuple

BoundingBox = namedtuple('BoundingBox', ('left', 'bottom', 'right', 'top'))

# Required to properly document this namedtuple and its attributes
BoundingBox.__doc__ = \
    """Bounding box named tuple, defining extent in cartesian coordinates.

    .. code::

        BoundingBox(left, bottom, right, top)
    """
BoundingBox.left.__doc__ = "Left coordinate"
BoundingBox.bottom.__doc__ = "Bottom coordinate"
BoundingBox.right.__doc__ = "Right coordinate"
BoundingBox.top.__doc__ = "Top coordinate"

def disjoint_bounds(bounds1, bounds2):
    """Compare two bounds and determine if they are disjoint.

    Parameters
    ----------
    bounds1: 4-tuple
        rasterio bounds tuple (left, bottom, right, top)
    bounds2: 4-tuple
        rasterio bounds tuple

    Returns
    -------
    boolean
    ``True`` if bounds are disjoint,
    ``False`` if bounds overlap

    """
    bounds1_north_up = bounds1[3] > bounds1[1]
    bounds2_north_up = bounds2[3] > bounds2[1]

    if not bounds1_north_up and bounds2_north_up:
        # or both south-up (also True)
        raise ValueError("Bounds must both have the same orientation")

    if bounds1_north_up:
        return (bounds1[0] > bounds2[2] or bounds2[0] > bounds1[2] or
                bounds1[1] > bounds2[3] or bounds2[1] > bounds1[3])
    else:
        return (bounds1[0] > bounds2[2] or bounds2[0] > bounds1[2] or
                bounds1[3] > bounds2[1] or bounds2[3] > bounds1[1])
