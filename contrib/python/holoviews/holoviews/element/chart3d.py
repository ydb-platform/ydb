import param

from ..core import Dimension, Element3D
from .geom import Points
from .path import Path
from .raster import Image


class Surface(Image, Element3D):
    """A Surface represents a regularly sampled 2D grid with associated
    values defining the height along the z-axis. The key dimensions of
    a Surface represent the 2D coordinates along the x- and y-axes
    while the value dimension declares the height at each grid
    location.

    The data of a Surface is usually defined as a 2D array of values
    and either a bounds tuple defining the extent in the 2D space or
    explicit x- and y-coordinate arrays.

    """

    extents = param.Tuple(default=(None, None, None, None, None, None), doc="""
        Allows overriding the extents of the Element in 3D space
        defined as (xmin, ymin, zmin, xmax, ymax, zmax).""")

    group = param.String(default='Surface', constant=True)

    kdims = param.List(default=[Dimension('x'), Dimension('y')],
                       bounds=(2,2), doc="""
        The Surface x and y dimensions of the space defined
        by the supplied extent.""")

    vdims = param.List(default=[Dimension('z')], bounds=(1,1), doc="""
        The Surface height dimension.""")

    def __init__(self, data, kdims=None, vdims=None, extents=None, **params):
        extents = extents if extents else (None, None, None, None, None, None)
        Image.__init__(self, data, kdims=kdims, vdims=vdims, extents=extents, **params)

    def _get_selection_expr_for_stream_value(self, **kwargs):
        expr, bbox, _ = super()._get_selection_expr_for_stream_value(**kwargs)
        return expr, bbox, None


class TriSurface(Element3D, Points):
    """TriSurface represents a set of coordinates in 3D space which
    define a surface via a triangulation algorithm (usually Delauney
    triangulation). They key dimensions of a TriSurface define the
    position of each point along the x-, y- and z-axes, while value
    dimensions can provide additional information about each point.

    """

    group = param.String(default='TriSurface', constant=True)

    kdims = param.List(default=[
        Dimension('x'), Dimension('y'), Dimension('z')], bounds=(3, 3), doc="""
        The key dimensions of a TriSurface represent the 3D coordinates
        of each point.""")

    vdims = param.List(default=[], doc="""
        The value dimensions of a TriSurface can provide additional
        information about each 3D coordinate.""")

    def __getitem__(self, slc):
        return Points.__getitem__(self, slc)


class Scatter3D(Element3D, Points):
    """Scatter3D is a 3D element representing the position of a collection
    of coordinates in a 3D space. The key dimensions represent the
    position of each coordinate along the x-, y- and z-axis.

    Scatter3D is not available for the default Bokeh backend.

    Examples
    --------
    Matplotlib

    .. code-block::

        import holoviews as hv
        from bokeh.sampledata.iris import flowers

        hv.extension("matplotlib")

        hv.Scatter3D(
            flowers, kdims=["sepal_length", "sepal_width", "petal_length"]
        ).opts(
            color="petal_width",
            alpha=0.7,
            size=5,
            cmap="fire",
            marker='^'
        )

    Plotly

    .. code-block::

        import holoviews as hv
        from bokeh.sampledata.iris import flowers

        hv.extension("plotly")

        hv.Scatter3D(
            flowers, kdims=["sepal_length", "sepal_width", "petal_length"]
        ).opts(
            color="petal_width",
            alpha=0.7,
            size=5,
            cmap="Portland",
            colorbar=True,
            marker="circle",
        )

    """

    kdims = param.List(default=[Dimension('x'),
                                Dimension('y'),
                                Dimension('z')], bounds=(3, 3))

    vdims = param.List(default=[], doc="""
        Scatter3D can have optional value dimensions,
        which may be mapped onto color and size.""")

    group = param.String(default='Scatter3D', constant=True)

    def __getitem__(self, slc):
        return Points.__getitem__(self, slc)


class Path3D(Element3D, Path):
    """Path3D is a 3D element representing a line through 3D space. The
    key dimensions represent the position of each coordinate along the
    x-, y- and z-axis while the value dimensions can optionally supply
    additional information.

    """

    kdims = param.List(default=[Dimension('x'),
                                Dimension('y'),
                                Dimension('z')], bounds=(3, 3))

    vdims = param.List(default=[], doc="""
        Path3D can have optional value dimensions.""")

    group = param.String(default='Path3D', constant=True)

    def __getitem__(self, slc):
        return Path.__getitem__(self, slc)
