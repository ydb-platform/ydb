"""The path module provides a set of elements to draw paths and polygon
geometries in 2D space. In addition to three general elements are
Path, Contours and Polygons, it defines a number of elements to
quickly draw common shapes.

"""

import numpy as np
import param

from ..core import Dataset
from ..core.data import MultiInterface
from ..core.dimension import Dimension
from .geom import Geometry
from .selection import SelectionPolyExpr


class Path(SelectionPolyExpr, Geometry):
    """The Path element represents one or more of path geometries with
    associated values. Each path geometry may be split into
    sub-geometries on NaN-values and may be associated with scalar
    values or array values varying along its length. In analogy to
    GEOS geometry types a Path is a collection of LineString and
    MultiLineString geometries with associated values.

    Like all other elements a Path may be defined through an
    extensible list of interfaces. Natively, HoloViews provides the
    MultiInterface which allows representing paths as lists of regular
    columnar data objects including arrays, dataframes and
    dictionaries of column arrays and scalars.

    The canonical representation is a list of dictionaries storing the
    x- and y-coordinates along with any other values:

        [{'x': 1d-array, 'y': 1d-array, 'value': scalar, 'continuous': 1d-array}, ...]

    Alternatively Path also supports a single columnar data-structure
    to specify an individual path:

        {'x': 1d-array, 'y': 1d-array, 'value': scalar, 'continuous': 1d-array}

    Both scalar values and values continuously varying along the
    geometries coordinates a Path may be used vary visual properties
    of the paths such as the color. Since not all formats allow
    storing scalar values as actual scalars, arrays that are the same
    length as the coordinates but have only one unique value are also
    considered scalar.

    The easiest way of accessing the individual geometries is using
    the `Path.split` method, which returns each path geometry as a
    separate entity, while the other methods assume a flattened
    representation where all paths are separated by NaN values.

    """

    group = param.String(default="Path", constant=True)

    datatype = param.List(default=[
        'multitabular', 'spatialpandas', 'dask_spatialpandas']
    )

    def __init__(self, data, kdims=None, vdims=None, **params):
        if isinstance(data, tuple) and len(data) == 2:
            # Add support for (x, ys) where ys defines multiple paths
            x, y = map(np.asarray, data)
            if y.ndim > 1:
                if len(x) != y.shape[0]:
                    raise ValueError("Path x and y values must be the same length.")
                data = [np.column_stack((x, y[:, i])) for i in range(y.shape[1])]
        elif isinstance(data, list) and all(isinstance(path, Path) for path in data):
            # Allow unpacking of a list of Path elements
            kdims = kdims or self.kdims
            paths = []
            for path in data:
                if path.kdims != kdims:
                    redim = {okd.name: nkd for okd, nkd in zip(path.kdims, kdims, strict=None)}
                    path = path.redim(**redim)
                if path.interface.multi and isinstance(path.data, list):
                    paths += path.data
                else:
                    paths.append(path.data)
            data = paths

        super().__init__(data, kdims=kdims, vdims=vdims, **params)

    def __getitem__(self, key):
        if isinstance(key, np.ndarray):
            return self.select(selection_mask=np.squeeze(key))
        if key in self.dimensions(): return self.dimension_values(key)
        if not isinstance(key, tuple) or len(key) == 1:
            key = (key, slice(None))
        elif len(key) == 0: return self.clone()
        if not all(isinstance(k, slice) for k in key):
            raise KeyError(f"{self.__class__.__name__} only support slice indexing")
        xkey, ykey = key
        xstart, xstop = xkey.start, xkey.stop
        ystart, ystop = ykey.start, ykey.stop
        return self.clone(extents=(xstart, ystart, xstop, ystop))

    def select(self, selection_expr=None, selection_specs=None, **selection):
        """Applies selection by dimension name

        Applies a selection along the dimensions of the object using
        keyword arguments. The selection may be narrowed to certain
        objects using selection_specs. For container objects the
        selection will be applied to all children as well.

        Selections may select a specific value, slice or set of values:

        * value: Scalar values will select rows along with an exact
                 match, e.g.:

            ds.select(x=3)

        * slice: Slices may be declared as tuples of the upper and
                 lower bound, e.g.:

            ds.select(x=(0, 3))

        * values: A list of values may be selected using a list or
                  set, e.g.:

            ds.select(x=[0, 1, 2])

        * predicate expression: A holoviews.dim expression, e.g.:

            from holoviews import dim
            ds.select(selection_expr=dim('x') % 2 == 0)

        Parameters
        ----------
        selection_expr : holoviews.dim predicate expression
            specifying selection.
        selection_specs : List of specs to match on
            A list of types, functions, or type[.group][.label]
            strings specifying which objects to apply the
            selection on.
        **selection: Dictionary declaring selections by dimension
            Selections can be scalar values, tuple ranges, lists
            of discrete values and boolean arrays

        Returns
        -------
        Returns an Dimensioned object containing the selected data
        or a scalar if a single value was selected
        """
        xdim, ydim = self.kdims[:2]
        x_range = selection.pop(xdim.name, None)
        y_range = selection.pop(ydim.name, None)
        sel = super().select(selection_expr, selection_specs,
                             **selection)
        if x_range is None and y_range is None:
            return sel
        x_range = x_range if isinstance(x_range, slice) else slice(None)
        y_range = y_range if isinstance(y_range, slice) else slice(None)
        return sel[x_range, y_range]

    def split(self, start=None, end=None, datatype=None, **kwargs):
        """The split method allows splitting a Path type into a list of
        subpaths of the same type. A start and/or end may be supplied
        to select a subset of paths.

        """
        if not self.interface.multi:
            if not len(self):
                return []
            elif datatype == 'array':
                obj = self.array(**kwargs)
            elif datatype == 'dataframe':
                obj = self.dframe(**kwargs)
            elif datatype in ('columns', 'dictionary'):
                obj = self.columns(**kwargs)
            elif datatype is None:
                obj = self.clone([self.data])
            else:
                raise ValueError(f"{datatype} datatype not support")
            return [obj]
        return self.interface.split(self, start, end, datatype, **kwargs)


class Dendrogram(Path):

    group = param.String(default="Dendrogram", constant=True)

    datatype = param.List(default=['multitabular'])

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def __init__(self, x, y=None, kdims=None, vdims=None, **params):
        data = x if y is None else zip(x, y, strict=True)
        super().__init__(data, kdims=kdims, vdims=vdims, **params)


class Contours(Path):
    """The Contours element is a subtype of a Path which is characterized
    by the fact that each path geometry may only be associated with
    scalar values. It supports all the same data formats as a `Path`
    but does not allow continuously varying values along the path
    geometry's coordinates. Conceptually Contours therefore represent
    iso-contours or isoclines, i.e. a function of two variables which
    describes a curve along which the function has a constant value.

    The canonical representation is a list of dictionaries storing the
    x- and y-coordinates along with any other (scalar) values:

        [{'x': 1d-array, 'y': 1d-array, 'value': scalar}, ...]

    Alternatively Contours also supports a single columnar
    data-structure to specify an individual contour:

        {'x': 1d-array, 'y': 1d-array, 'value': scalar, 'continuous': 1d-array}

    Since not all formats allow storing scalar values as actual
    scalars arrays which are the same length as the coordinates but
    have only one unique value are also considered scalar. This is
    strictly enforced, ensuring that each path geometry represents
    a valid iso-contour.

    The easiest way of accessing the individual geometries is using
    the `Contours.split` method, which returns each path geometry as a
    separate entity, while the other methods assume a flattened
    representation where all paths are separated by NaN values.

    """

    vdims = param.List(default=[], constant=True, doc="""
        Contours optionally accept a value dimension, corresponding
        to the supplied values.""")

    group = param.String(default='Contours', constant=True)

    _level_vdim = Dimension('Level') # For backward compatibility

    def __init__(self, data, kdims=None, vdims=None, **params):
        data = [] if data is None else data
        super().__init__(data, kdims=kdims, vdims=vdims, **params)


class Polygons(Contours):
    """The Polygons element represents one or more polygon geometries
    with associated scalar values. Each polygon geometry may be split
    into sub-geometries on NaN-values and may be associated with
    scalar values. In analogy to GEOS geometry types a Polygons
    element is a collection of Polygon and MultiPolygon
    geometries. Polygon geometries are defined as a set of coordinates
    describing the exterior bounding ring and any number of interior
    holes.

    Like all other elements a Polygons element may be defined through
    an extensible list of interfaces. Natively HoloViews provides the
    MultiInterface which allows representing paths as lists of regular
    columnar data objects including arrays, dataframes and
    dictionaries of column arrays and scalars.

    The canonical representation is a list of dictionaries storing the
    x- and y-coordinates, a list-of-lists of arrays representing the
    holes, along with any other values:

        [{'x': 1d-array, 'y': 1d-array, 'holes': list-of-lists-of-arrays, 'value': scalar}, ...]

    Alternatively Polygons also supports a single columnar
    data-structure to specify an individual polygon:

        {'x': 1d-array, 'y': 1d-array, 'holes': list-of-lists-of-arrays, 'value': scalar}

    The list-of-lists format of the holes corresponds to the potential
    for each coordinate array to be split into a multi-geometry
    through NaN-separators. Each sub-geometry separated by the NaNs
    therefore has an unambiguous mapping to a list of holes. If a
    (multi-)polygon has no holes, the 'holes' key may be omitted.

    Any value dimensions stored on a Polygons geometry must be scalar,
    just like the Contours element. Since not all formats allow
    storing scalar values as actual scalars arrays which are the same
    length as the coordinates but have only one unique value are also
    considered scalar.

    The easiest way of accessing the individual geometries is using
    the `Polygons.split` method, which returns each path geometry as a
    separate entity, while the other methods assume a flattened
    representation where all paths are separated by NaN values.

    """

    group = param.String(default="Polygons", constant=True)

    vdims = param.List(default=[], doc="""
        Polygons optionally accept a value dimension, corresponding
        to the supplied value.""")

    _level_vdim = Dimension('Value')

    # Defines which key the DictInterface uses to look for holes
    _hole_key = 'holes'

    @property
    def has_holes(self):
        """Detects whether any polygon in the Polygons element defines
        holes. Useful to avoid expanding Polygons unless necessary.

        """
        return self.interface.has_holes(self)

    def holes(self):
        """Returns a list-of-lists-of-lists of hole arrays. The three levels
        of nesting reflects the structure of the polygons:

          1. The first level of nesting corresponds to the list of geometries
          2. The second level corresponds to each Polygon in a MultiPolygon
          3. The third level of nesting allows for multiple holes per Polygon

        """
        return self.interface.holes(self)


class BaseShape(Path):
    """A BaseShape is a Path that can be succinctly expressed by a small
    number of parameters instead of a full path specification. For
    instance, a circle may be expressed by the center position and
    radius instead of an explicit list of path coordinates.

    """

    __abstract = True

    def __new__(cls, *args, **kwargs):
        return super(Dataset, cls).__new__(cls)

    def __init__(self, **params):
        super().__init__([], **params)
        self.interface = MultiInterface

    def clone(self, *args, **overrides):
        """Returns a clone of the object with matching parameter values
        containing the specified args and kwargs.

        """
        link = overrides.pop('link', True)
        settings = dict(self.param.values(), **overrides)
        if 'id' not in settings:
            settings['id'] = self.id
        if not args and link:
            settings['plot_id'] = self._plot_id

        pos_args = getattr(self, '_' + type(self).__name__ + '__pos_params', [])
        return self.__class__(*(settings[n] for n in pos_args),
                              **{k:v for k,v in settings.items()
                                 if k not in pos_args})



class Box(BaseShape):
    """Draw a centered box of a given width at the given position with
    the specified aspect ratio (if any).

    """

    x = param.Number(default=0, doc="The x-position of the box center.")

    y = param.Number(default=0, doc="The y-position of the box center.")

    width = param.Number(default=1, doc="The width of the box.")

    height = param.Number(default=1, doc="The height of the box.")

    orientation = param.Number(default=0, doc="""
       Orientation in the Cartesian coordinate system, the
       counterclockwise angle in radians between the first axis and the
       horizontal.""")

    aspect= param.Number(default=1.0, doc="""
       Optional multiplier applied to the box size to compute the
       width in cases where only the length value is set.""")

    group = param.String(default='Box', constant=True, doc="The assigned group name.")

    __pos_params = ['x','y', 'height']

    def __init__(self, x, y, spec, **params):
        if isinstance(spec, tuple):
            if 'aspect' in params:
                raise ValueError('Aspect parameter not supported when supplying '
                                 '(width, height) specification.')
            (width, height ) = spec
        else:
            width, height = params.get('width', spec), spec

        params['width']=params.get('width',width)
        super().__init__(x=x, y=y, height=height, **params)

        half_width = (self.width * self.aspect)/ 2.0
        half_height = self.height / 2.0
        (l,b,r,t) = (-half_width, -half_height, half_width, half_height)
        box = np.array([(l, b), (l, t), (r, t), (r, b),(l, b)])
        rot = np.array([[np.cos(self.orientation), -np.sin(self.orientation)],
                        [np.sin(self.orientation), np.cos(self.orientation)]])

        xs, ys = np.tensordot(rot, box.T, axes=[1,0])
        self.data = [np.column_stack([xs+x, ys+y])]


class Ellipse(BaseShape):
    """Draw an axis-aligned ellipse at the specified x,y position with
    the given orientation.

    The simplest (default) Ellipse is a circle, specified using:

    Ellipse(x,y, diameter)

    A circle is a degenerate ellipse where the width and height are
    equal. To specify these explicitly, you can use:

    Ellipse(x,y, (width, height))

    There is also an aspect parameter allowing you to generate an ellipse
    by specifying a multiplicating factor that will be applied to the
    height only.

    Note that as a subclass of Path, internally an Ellipse is a
    sequence of (x,y) sample positions. Ellipse could also be
    implemented as an annotation that uses a dedicated ellipse artist.

    """

    x = param.Number(default=0, doc="The x-position of the ellipse center.")

    y = param.Number(default=0, doc="The y-position of the ellipse center.")

    width = param.Number(default=1, doc="The width of the ellipse.")

    height = param.Number(default=1, doc="The height of the ellipse.")

    orientation = param.Number(default=0, doc="""
       Orientation in the Cartesian coordinate system, the
       counterclockwise angle in radians between the first axis and the
       horizontal.""")

    aspect= param.Number(default=1.0, doc="""
       Optional multiplier applied to the diameter to compute the width
       in cases where only the diameter value is set.""")

    samples = param.Number(default=100, doc="The sample count used to draw the ellipse.")

    group = param.String(default='Ellipse', constant=True, doc="The assigned group name.")

    __pos_params = ['x','y', 'height']

    def __init__(self, x, y, spec, **params):

        if isinstance(spec, tuple):
            if 'aspect' in params:
                raise ValueError('Aspect parameter not supported when supplying '
                                 '(width, height) specification.')
            (width, height) = spec
        else:
            width, height = params.get('width', spec), spec

        params['width']=params.get('width',width)
        super().__init__(x=x, y=y, height=height, **params)
        angles = np.linspace(0, 2*np.pi, self.samples)
        half_width = (self.width * self.aspect)/ 2.0
        half_height = self.height / 2.0
        #create points
        ellipse = np.array(
            list(zip(half_width*np.sin(angles),
                     half_height*np.cos(angles), strict=None)))
        #rotate ellipse and add offset
        rot = np.array([[np.cos(self.orientation), -np.sin(self.orientation)],
               [np.sin(self.orientation), np.cos(self.orientation)]])
        self.data = [np.tensordot(rot, ellipse.T, axes=[1,0]).T+np.array([x,y])]


class Bounds(BaseShape):
    """An arbitrary axis-aligned bounding rectangle defined by the (left,
    bottom, right, top) coordinate positions.

    If supplied a single real number as input, this value will be
    treated as the radius of a square, zero-center box which will be
    used to compute the corresponding lbrt tuple.

    """

    lbrt = param.Tuple(default=(-0.5, -0.5, 0.5, 0.5), doc="""
          The (left, bottom, right, top) coordinates of the bounding box.""")

    group = param.String(default='Bounds', constant=True, doc="The assigned group name.")

    __pos_params = ['lbrt']

    def __init__(self, lbrt, **params):
        if not isinstance(lbrt, tuple):
            lbrt = (-lbrt, -lbrt, lbrt, lbrt)

        super().__init__(lbrt=lbrt, **params)
        (l,b,r,t) = self.lbrt
        xdim, ydim = self.kdims
        self.data = [dict([(xdim.name, np.array([l, l, r, r, l])),
                                  (ydim.name, np.array([b, t, t, b, b]))])]
