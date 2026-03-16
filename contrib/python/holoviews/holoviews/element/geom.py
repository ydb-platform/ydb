import numpy as np
import param

from ..core import Dataset, Dimension, Element2D
from .selection import Selection2DExpr, SelectionGeomExpr


class Geometry(Dataset, Element2D):
    """Geometry elements represent a collection of objects drawn in
    a 2D coordinate system. The two key dimensions correspond to the
    x- and y-coordinates in the 2D space, while the value dimensions
    may be used to control other visual attributes of the Geometry

    """

    group = param.String(default='Geometry', constant=True)

    kdims = param.List(default=[Dimension('x'), Dimension('y')],
                       bounds=(2, 2), constant=True, doc="""
        The key dimensions of a geometry represent the x- and y-
        coordinates in a 2D space.""")

    vdims = param.List(default=[], constant=True, doc="""
        Value dimensions can be associated with a geometry.""")

    __abstract = True


class Points(Selection2DExpr, Geometry):
    """Points represents a set of coordinates in 2D space, which may
    optionally be associated with any number of value dimensions.

    """

    group = param.String(default='Points', constant=True)

    _auto_indexable_1d = True


class VectorField(Selection2DExpr, Geometry):
    """A VectorField represents a set of vectors in 2D space with an
    associated angle, as well as an optional magnitude and any number
    of other value dimensions. The angles are assumed to be defined in
    radians and by default the magnitude is assumed to be normalized
    to be between 0 and 1.

    """

    group = param.String(default='VectorField', constant=True)

    vdims = param.List(default=[Dimension('Angle', cyclic=True, range=(0,2*np.pi)),
                                Dimension('Magnitude')], bounds=(1, None))

    @classmethod
    def from_uv(cls, data, kdims=None, vdims=None, **params):
        if kdims is None:
            kdims = ['x', 'y']
        if vdims is None:
            vdims = ['u', 'v']
        dataset = Dataset(data, kdims=kdims, vdims=vdims, **params)
        us, vs = (dataset.dimension_values(i) for i in range(2, 4))

        uv_magnitudes = np.hypot(us, vs)  # unscaled
        # this follows mathematical conventions,
        # unlike WindBarbs which follows meteorological conventions
        radians = np.arctan2(vs, us)

        # calculations on this data could mutate the original data
        # here we do not do any calculations; we only store the data
        repackaged_dataset = {}
        for kdim in kdims:
            repackaged_dataset[kdim] = dataset[kdim]
        repackaged_dataset["Angle"] = radians
        repackaged_dataset["Magnitude"] = uv_magnitudes
        for vdim in vdims[2:]:
            repackaged_dataset[vdim] = dataset[vdim]
        vdims = [
            Dimension('Angle', cyclic=True, range=(0, 2 * np.pi)),
            Dimension('Magnitude'),
            *vdims[2:],
        ]
        return cls(repackaged_dataset, kdims=kdims, vdims=vdims, **params)


class Segments(SelectionGeomExpr, Geometry):
    """Segments represent a collection of lines in 2D space.

    """

    group = param.String(default='Segments', constant=True)

    kdims = param.List(default=[Dimension('x0'), Dimension('y0'),
                                Dimension('x1'), Dimension('y1')],
                       bounds=(4, 4), constant=True, doc="""
        Segments represent lines given by x- and y-
        coordinates in 2D space.""")


class Rectangles(SelectionGeomExpr, Geometry):
    """Rectangles represent a collection of axis-aligned rectangles in 2D space.

    """

    group = param.String(default='Rectangles', constant=True)

    kdims = param.List(default=[Dimension('x0'), Dimension('y0'),
                                Dimension('x1'), Dimension('y1')],
                       bounds=(4, 4), constant=True, doc="""
        The key dimensions of the Rectangles element represent the
        bottom-left (x0, y0) and top right (x1, y1) coordinates
        of each box.""")
