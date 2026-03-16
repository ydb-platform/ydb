"""Defines mix-in classes to handle support for linked brushing on
elements.

"""

import sys
from importlib.util import find_spec

import numpy as np

from ..core import Dataset, NdOverlay, util
from ..core.util import dtype_kind
from ..streams import Lasso, Selection1D, SelectionXY
from ..util.transform import dim
from .annotation import HSpan, VSpan


class SelectionIndexExpr:

    _selection_dims = None

    _selection_streams = (Selection1D,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._index_skip = False

    def _empty_region(self):
        return None

    def _get_index_selection(self, index, index_cols):
        self._index_skip = True
        if not index:
            return None, None, None
        clone_vdims = [vdim.name for vdim in self.vdims if vdim.name not in index_cols]
        cols = clone_vdims + index_cols
        ds = self.clone(kdims=index_cols, vdims=clone_vdims, new_type=Dataset)
        if len(index_cols) == 1:
            index_dim = index_cols[0]
            vals = dim(index_dim).apply(ds.iloc[index, cols], expanded=False)
            if dtype_kind(vals) == 'O' and all(isinstance(v, np.ndarray) for v in vals):
                vals = [v for arr in vals for v in util.unique_iterator(arr)]
            expr = dim(index_dim).isin(list(util.unique_iterator(vals)))
        else:
            get_shape = dim(self.dataset.get_dimension(index_cols[0]), np.shape)
            index_cols = [dim(self.dataset.get_dimension(c), np.ravel) for c in index_cols]
            vals = dim(index_cols[0], util.unique_zip, *index_cols[1:]).apply(
                ds.iloc[index, cols], expanded=True, flat=True
            )
            contains = dim(index_cols[0], util.lzip, *index_cols[1:]).isin(vals, object=True)
            expr = dim(contains, np.reshape, get_shape)
        return expr, None, None

    def _get_selection_expr_for_stream_value(self, **kwargs):
        index = kwargs.get('index')
        index_cols = kwargs.get('index_cols')
        if index is None or index_cols is None:
            return None, None, None
        return self._get_index_selection(index, index_cols)

    @staticmethod
    def _merge_regions(region1, region2, operation):
        return None


def spatial_select_gridded(xvals, yvals, geometry):
    rectilinear = (np.diff(xvals, axis=0) == 0).all()
    if rectilinear:
        from .path import Polygons
        from .raster import Image
        try:
            from ..operation.datashader import rasterize
        except ImportError:
            raise ImportError("Lasso selection on gridded data requires "
                              "datashader to be available.") from None
        xs, ys = xvals[0], yvals[:, 0]
        target = Image((xs, ys, np.empty(ys.shape+xs.shape)))
        poly = Polygons([geometry])
        sel_mask = rasterize(poly, target=target, dynamic=False, aggregator='any')
        return sel_mask.dimension_values(2, flat=False)
    else:
        sel_mask = spatial_select_columnar(xvals.flatten(), yvals.flatten(), geometry)
        return sel_mask.reshape(xvals.shape)

def _cuspatial_old(xvals, yvals, geometry):
    import cudf
    import cuspatial

    result = cuspatial.point_in_polygon(
        xvals,
        yvals,
        cudf.Series([0], index=["selection"]),
        [0],
        geometry[:, 0],
        geometry[:, 1],
    )
    return result.values


def _cuspatial_new(xvals, yvals, geometry):
    import cudf
    import cuspatial
    import geopandas
    from shapely.geometry import Polygon

    df = cudf.DataFrame({'x':xvals, 'y':yvals})
    points = cuspatial.GeoSeries.from_points_xy(
       df.interleave_columns().astype('float')
    )
    polygons = cuspatial.GeoSeries(
       geopandas.GeoSeries(Polygon(geometry)), index=["selection"]
    )
    result = cuspatial.point_in_polygon(points,polygons)
    return result.values.ravel()


def spatial_select_columnar(xvals, yvals, geometry, geom_method=None):
    import pandas as pd
    if 'cudf' in sys.modules:
        import cudf
        import cupy as cp
        if isinstance(xvals, cudf.Series):
            xvals = xvals.values.astype('float')
            yvals = yvals.values.astype('float')
            try:
                try:
                    return _cuspatial_old(xvals, yvals, geometry)
                except TypeError:
                    return _cuspatial_new(xvals, yvals, geometry)
            except ImportError:
                xvals = cp.asnumpy(xvals)
                yvals = cp.asnumpy(yvals)
    if 'dask' in sys.modules:
        import dask.dataframe as dd
        if isinstance(xvals, dd.Series):
            try:
                xvals.name = "xvals"
                yvals.name = "yvals"
                df = xvals.to_frame().join(yvals)
                return df.map_partitions(
                    lambda df, geometry: spatial_select_columnar(df.xvals, df.yvals, geometry),
                    geometry,
                    meta=pd.Series(dtype=bool)
                )
            except Exception:
                xvals = np.asarray(xvals)
                yvals = np.asarray(yvals)
    x0, x1 = geometry[:, 0].min(), geometry[:, 0].max()
    y0, y1 = geometry[:, 1].min(), geometry[:, 1].max()
    sel_mask = (xvals>=x0) & (xvals<=x1) & (yvals>=y0) & (yvals<=y1)
    masked_xvals = xvals[sel_mask]
    masked_yvals = yvals[sel_mask]
    if geom_method is None:
        if find_spec("spatialpandas") is not None:
            geom_method = "spatialpandas"
        elif find_spec("shapely") is not None:
            geom_method = "shapely"
        else:
            msg = "Lasso selection on tabular data requires either spatialpandas or shapely to be available."
            raise ImportError(msg) from None
    geom_function = {"spatialpandas": _mask_spatialpandas, "shapely": _mask_shapely}[geom_method]
    geom_mask = geom_function(masked_xvals, masked_yvals, geometry)
    if isinstance(xvals, pd.Series):
        sel_mask[sel_mask.index[np.where(sel_mask)[0]]] = geom_mask
    else:
        sel_mask[np.where(sel_mask)[0]] = geom_mask
    return sel_mask


def _mask_spatialpandas(masked_xvals, masked_yvals, geometry):
    from spatialpandas.geometry import PointArray, Polygon
    points = PointArray((masked_xvals.astype('float'), masked_yvals.astype('float')))
    poly = Polygon([np.concatenate([geometry, geometry[:1]]).flatten()])
    return points.intersects(poly)


def _mask_shapely(masked_xvals, masked_yvals, geometry):
    from shapely.geometry import Point, Polygon
    points = (Point(x, y) for x, y in zip(masked_xvals, masked_yvals, strict=None))
    poly = Polygon(geometry)
    return np.array([poly.contains(p) for p in points], dtype=bool)


def spatial_select(xvals, yvals, geometry):
    if xvals.ndim > 1:
        return spatial_select_gridded(xvals, yvals, geometry)
    else:
        return spatial_select_columnar(xvals, yvals, geometry)

def spatial_geom_select(x0vals, y0vals, x1vals, y1vals, geometry):
    try:
        from shapely.geometry import Polygon, box
        boxes = (box(x0, y0, x1, y1) for x0, y0, x1, y1 in
                 zip(x0vals, y0vals, x1vals, y1vals, strict=None))
        poly = Polygon(geometry)
        return np.array([poly.contains(p) for p in boxes])
    except ImportError:
        raise ImportError("Lasso selection on geometry data requires "
                          "shapely to be available.") from None

def spatial_poly_select(xvals, yvals, geometry):
    try:
        from shapely.geometry import Polygon
        boxes = (Polygon(np.column_stack([xs, ys])) for xs, ys in zip(xvals, yvals, strict=None))
        poly = Polygon(geometry)
        return np.array([poly.contains(p) for p in boxes])
    except ImportError:
        raise ImportError("Lasso selection on geometry data requires "
                          "shapely to be available.") from None

def spatial_bounds_select(xvals, yvals, bounds):
    x0, y0, x1, y1 = bounds
    return np.array([((x0<=np.nanmin(xs)) & (y0<=np.nanmin(ys)) &
                      (x1>=np.nanmax(xs)) & (y1>=np.nanmax(ys)))
                     for xs, ys in zip(xvals, yvals, strict=None)])


class Selection2DExpr(SelectionIndexExpr):
    """Mixin class for Cartesian 2D elements to add basic support for
    SelectionExpr streams.

    """

    _selection_dims = 2

    _selection_streams = (SelectionXY, Lasso, Selection1D)

    def _empty_region(self):
        from .geom import Rectangles
        from .path import Path
        return Rectangles([]) * Path([])

    def _get_selection(self, **kwargs):
        xcats, ycats = None, None
        x0, y0, x1, y1 = kwargs['bounds']
        if 'x_selection' in kwargs:
            xsel = kwargs['x_selection']
            if isinstance(xsel, list):
                xcats = xsel
                x0, x1 = round(x0), round(x1)
            ysel = kwargs['y_selection']
            if isinstance(ysel, list):
                ycats = ysel
                y0, y1 = round(y0), round(y1)

        # Handle invert_xaxis/invert_yaxis
        if x0 > x1:
            x0, x1 = x1, x0
        if y0 > y1:
            y0, y1 = y1, y0

        return (x0, x1), xcats, (y0, y1), ycats

    def _get_index_expr(self, index_cols, sel):
        if len(index_cols) == 1:
            index_dim = index_cols[0]
            vals = dim(index_dim).apply(sel, expanded=False, flat=True)
            expr = dim(index_dim).isin(list(util.unique_iterator(vals)))
        else:
            get_shape = dim(self.dataset.get_dimension(), np.shape)
            index_cols = [dim(self.dataset.get_dimension(c), np.ravel) for c in index_cols]
            vals = dim(index_cols[0], util.unique_zip, *index_cols[1:]).apply(
                sel, expanded=True, flat=True
            )
            contains = dim(index_cols[0], util.lzip, *index_cols[1:]).isin(vals, object=True)
            expr = dim(contains, np.reshape, get_shape)
        return expr

    def _get_bounds_selection(self, xdim, ydim, **kwargs):
        from .geom import Rectangles
        (x0, x1), xcats, (y0, y1), ycats = self._get_selection(**kwargs)
        xsel = xcats or (x0, x1)
        ysel = ycats or (y0, y1)

        bbox = {xdim.name: xsel, ydim.name: ysel}
        index_cols = kwargs.get('index_cols')
        if index_cols:
            selection = self.dataset.clone(datatype=['dataframe', 'dictionary']).select(**bbox)
            selection_expr = self._get_index_expr(index_cols, selection)
            region_element = None
        else:
            if xcats:
                xexpr = dim(xdim).isin(xcats)
            else:
                xexpr = (dim(xdim) >= x0) & (dim(xdim) <= x1)
            if ycats:
                yexpr = dim(ydim).isin(ycats)
            else:
                yexpr = (dim(ydim) >= y0) & (dim(ydim) <= y1)
            selection_expr = (xexpr & yexpr)
            region_element = Rectangles([(x0, y0, x1, y1)])
        return selection_expr, bbox, region_element

    def _get_lasso_selection(self, xdim, ydim, geometry, **kwargs):
        from .path import Path
        bbox = {xdim.name: geometry[:, 0], ydim.name: geometry[:, 1]}
        expr = dim.pipe(spatial_select, xdim, dim(ydim), geometry=geometry)
        index_cols = kwargs.get('index_cols')
        if index_cols:
            selection = self[expr.apply(self)]
            selection_expr = self._get_index_expr(index_cols, selection)
            return selection_expr, bbox, None
        return expr, bbox, Path([np.concatenate([geometry, geometry[:1]])])

    def _get_selection_dims(self):
        from .graphs import Graph
        if isinstance(self, Graph):
            xdim, ydim = self.nodes.dimensions()[:2]
        else:
            xdim, ydim = self.dimensions()[:2]

        invert_axes = self.opts.get('plot').kwargs.get('invert_axes', False)
        if invert_axes:
            xdim, ydim = ydim, xdim
        return (xdim, ydim)

    def _skip(self, **kwargs):
        skip = kwargs.get('index_cols') and self._index_skip
        if skip:
            self._index_skip = False
        return skip

    def _get_selection_expr_for_stream_value(self, **kwargs):
        from .geom import Rectangles
        from .path import Path

        if (kwargs.get('bounds') is None and kwargs.get('x_selection') is None
            and kwargs.get('geometry') is None and not kwargs.get('index')):
            return None, None, Rectangles([]) * Path([])

        index_cols = kwargs.get('index_cols')

        dims = self._get_selection_dims()
        if kwargs.get('index') is not None and index_cols is not None:
            expr, _, _ = self._get_index_selection(kwargs['index'], index_cols)
            return expr, None, self._empty_region()
        elif self._skip(**kwargs):
            return None
        elif 'bounds' in kwargs:
            expr, bbox, region = self._get_bounds_selection(*dims, **kwargs)
            return expr, bbox, None if region is None else region * Path([])
        elif 'geometry' in kwargs:
            expr, bbox, region = self._get_lasso_selection(*dims, **kwargs)
            return expr, bbox, None if region is None else Rectangles([]) * region

    @staticmethod
    def _merge_regions(region1, region2, operation):
        if region1 is None or operation == "overwrite":
            return region2
        rect1 = region1.get(0)
        rect2 = region2.get(0)
        rects = rect1.clone(rect1.interface.concatenate([rect1, rect2]))
        poly1 = region1.get(1)
        poly2 = region2.get(1)
        polys = poly1.clone([poly1, poly2])
        return rects * polys


class SelectionGeomExpr(Selection2DExpr):

    def _get_selection_dims(self):
        x0dim, y0dim, x1dim, y1dim = self.kdims
        invert_axes = self.opts.get('plot').kwargs.get('invert_axes', False)
        if invert_axes:
            x0dim, x1dim, y0dim, y1dim = y0dim, y1dim, x0dim, x1dim
        return (x0dim, y0dim, x1dim, y1dim)

    def _get_bounds_selection(self, x0dim, y0dim, x1dim, y1dim, **kwargs):
        from .geom import Rectangles

        (x0, x1), xcats, (y0, y1), ycats = self._get_selection(**kwargs)
        xsel = xcats or (x0, x1)
        ysel = ycats or (y0, y1)

        bbox = {x0dim.name: xsel, y0dim.name: ysel, x1dim.name: xsel, y1dim.name: ysel}
        index_cols = kwargs.get('index_cols')
        if index_cols:
            selection = self.dataset.clone(datatype=['dataframe', 'dictionary']).select(**bbox)
            selection_expr = self._get_index_expr(index_cols, selection)
            region_element = None
        else:
            x0expr = (dim(x0dim) >= x0) & (dim(x0dim) <= x1)
            y0expr = (dim(y0dim) >= y0) & (dim(y0dim) <= y1)
            x1expr = (dim(x1dim) >= x0) & (dim(x1dim) <= x1)
            y1expr = (dim(y1dim) >= y0) & (dim(y1dim) <= y1)
            selection_expr = (x0expr & y0expr & x1expr & y1expr)
            region_element = Rectangles([(x0, y0, x1, y1)])
        return selection_expr, bbox, region_element

    def _get_lasso_selection(self, x0dim, y0dim, x1dim, y1dim, geometry, **kwargs):
        from .path import Path

        bbox = {
            x0dim.name: geometry[:, 0], y0dim.name: geometry[:, 1],
            x1dim.name: geometry[:, 0], y1dim.name: geometry[:, 1]
        }
        expr = dim.pipe(spatial_geom_select, x0dim, dim(y0dim), dim(x1dim), dim(y1dim), geometry=geometry)
        index_cols = kwargs.get('index_cols')
        if index_cols:
            selection = self[expr.apply(self)]
            selection_expr = self._get_index_expr(index_cols, selection)
            return selection_expr, bbox, None
        return expr, bbox, Path([np.concatenate([geometry, geometry[:1]])])


class SelectionPolyExpr(Selection2DExpr):

    def _skip(self, **kwargs):
        """Do not skip geometry selections until polygons support returning
        indexes on lasso based selections.

        """
        skip = kwargs.get('index_cols') and self._index_skip and 'geometry' not in kwargs
        if skip:
            self._index_skip = False
        return skip

    def _get_bounds_selection(self, xdim, ydim, **kwargs):
        from .geom import Rectangles
        (x0, x1), _, (y0, y1), _ = self._get_selection(**kwargs)

        bbox = {xdim.name: (x0, x1), ydim.name: (y0, y1)}
        index_cols = kwargs.get('index_cols')
        expr = dim.pipe(spatial_bounds_select, xdim, dim(ydim),
                                  bounds=(x0, y0, x1, y1))
        if index_cols:
            selection = self[expr.apply(self, expanded=False)]
            selection_expr = self._get_index_expr(index_cols, selection)
            return selection_expr, bbox, None
        return expr, bbox, Rectangles([(x0, y0, x1, y1)])

    def _get_lasso_selection(self, xdim, ydim, geometry, **kwargs):
        from .path import Path
        bbox = {xdim.name: geometry[:, 0], ydim.name: geometry[:, 1]}
        expr = dim.pipe(spatial_poly_select, xdim, dim(ydim), geometry=geometry)
        index_cols = kwargs.get('index_cols')
        if index_cols:
            selection = self[expr.apply(self, expanded=False)]
            selection_expr = self._get_index_expr(index_cols, selection)
            return selection_expr, bbox, None
        return expr, bbox, Path([np.concatenate([geometry, geometry[:1]])])


class Selection1DExpr(Selection2DExpr):
    """Mixin class for Cartesian 1D Chart elements to add basic support for
    SelectionExpr streams.

    """

    _selection_dims = 1

    _inverted_expr = False

    _selection_streams = (SelectionXY,)

    def _empty_region(self):
        invert_axes = self.opts.get('plot').kwargs.get('invert_axes', False)
        if ((invert_axes and not self._inverted_expr) or (not invert_axes and self._inverted_expr)):
            region_el = HSpan
        else:
            region_el = VSpan
        return NdOverlay({0: region_el()})

    def _get_selection_expr_for_stream_value(self, **kwargs):
        invert_axes = self.opts.get('plot').kwargs.get('invert_axes', False)
        if ((invert_axes and not self._inverted_expr) or (not invert_axes and self._inverted_expr)):
            region_el = HSpan
        else:
            region_el = VSpan

        if kwargs.get('bounds', None) is None:
            region = None if 'index_cols' in kwargs else NdOverlay({0: region_el()})
            return None, None, region

        x0, y0, x1, y1 = kwargs['bounds']

        # Handle invert_xaxis/invert_yaxis
        if y0 > y1:
            y0, y1 = y1, y0
        if x0 > x1:
            x0, x1 = x1, x0

        if len(self.dimensions()) == 1:
            xdim = self.dimensions()[0]
            ydim = None
        else:
            xdim, ydim = self.dimensions()[:2]

        if invert_axes:
            x0, x1, y0, y1 = y0, y1, x0, x1
            cat_kwarg = 'y_selection'
        else:
            cat_kwarg = 'x_selection'

        if self._inverted_expr:
            if ydim is not None: xdim = ydim
            x0, x1 = y0, y1
            cat_kwarg = ('y' if invert_axes else 'x') + '_selection'
        cats = kwargs.get(cat_kwarg)

        bbox = {xdim.name: (x0, x1)}
        if cats is not None and len(self.kdims) == 1:
            bbox[self.kdims[0].name] = cats
        index_cols = kwargs.get('index_cols')
        if index_cols:
            selection = self.dataset.clone(datatype=['dataframe', 'dictionary']).select(**bbox)
            selection_expr = self._get_index_expr(index_cols, selection)
            region_element = None
        else:
            if isinstance(cats, list) and xdim in self.kdims[:1]:
                selection_expr = dim(xdim).isin(cats)
            else:
                selection_expr = ((dim(xdim) >= x0) & (dim(xdim) <= x1))
                if isinstance(cats, list) and len(self.kdims) == 1:
                    selection_expr &= dim(self.kdims[0]).isin(cats)
            region_element = NdOverlay({0: region_el(x0, x1)})
        return selection_expr, bbox, region_element

    @staticmethod
    def _merge_regions(region1, region2, operation):
        if region1 is None or operation == "overwrite":
            return region2
        data = [d.data for d in region1] + [d.data for d in region2]
        prev = len(data)
        new = None
        while prev != new:
            prev = len(data)
            contiguous = []
            for l, u in data:
                if not util.isfinite(l) or not util.isfinite(u):
                    continue
                overlap = False
                for i, (pl, pu) in enumerate(contiguous):
                    if l >= pl and l <= pu:
                        pu = max(u, pu)
                        overlap = True
                    elif u <= pu and u >= pl:
                        pl = min(l, pl)
                        overlap = True
                    if overlap:
                        contiguous[i] = (pl, pu)
                if not overlap:
                    contiguous.append((l, u))
            new = len(contiguous)
            data = contiguous
        return NdOverlay([(i, region1.last.clone(l, u)) for i, (l, u) in enumerate(data)])
