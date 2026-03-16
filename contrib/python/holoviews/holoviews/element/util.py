from __future__ import annotations

import itertools
from typing import TYPE_CHECKING

import numpy as np
import param

from ..core import Dataset
from ..core.boundingregion import BoundingBox
from ..core.data import PandasInterface, default_datatype
from ..core.operation import Operation
from ..core.sheetcoords import Slice
from ..core.util import (
    PANDAS_GE_2_1_0,
    cartesian_product,
    datetime_types,
    dtype_kind,
    is_cyclic,
    is_nan,
    one_to_one,
    sort_topologically,
)

if TYPE_CHECKING:
    from typing import TypeVar

    import pandas as pd

    Array = TypeVar("Array", np.ndarray, pd.api.extensions.ExtensionArray)


def split_path(path):
    """Split a Path type containing a single NaN separated path into
    multiple subpaths.

    """
    path = path.split(0, 1)[0]
    values = path.dimension_values(0)
    splits = np.concatenate([[0], np.where(np.isnan(values))[0]+1, [None]])
    subpaths = []
    data = PandasInterface.as_dframe(path)
    for i in range(len(splits)-1):
        end = splits[i+1]
        slc = slice(splits[i], None if end is None else end-1)
        subpath = data.iloc[slc]
        if len(subpath):
            subpaths.append(subpath)
    return subpaths


def compute_slice_bounds(slices, scs, shape):
    """Given a 2D selection consisting of slices/coordinates, a
    SheetCoordinateSystem and the shape of the array returns a new
    BoundingBox representing the sliced region.

    """
    xidx, yidx = slices
    ys, xs = shape
    l, b, r, t = scs.bounds.lbrt()
    xdensity, ydensity = scs.xdensity, scs.ydensity
    xunit = (1./xdensity)
    yunit = (1./ydensity)
    if isinstance(l, datetime_types):
        xunit = np.timedelta64(round(xunit), scs._time_unit)
    if isinstance(b, datetime_types):
        yunit = np.timedelta64(round(yunit), scs._time_unit)
    if isinstance(xidx, slice):
        l = l if xidx.start is None else max(l, xidx.start)
        r = r if xidx.stop is None else min(r, xidx.stop)
    if isinstance(yidx, slice):
        b = b if yidx.start is None else max(b, yidx.start)
        t = t if yidx.stop is None else min(t, yidx.stop)
    bounds = BoundingBox(points=((l, b), (r, t)))

    # Apply new bounds
    slc = Slice(bounds, scs)

    # Apply scalar and list indices
    l, b, r, t = slc.compute_bounds(scs).lbrt()
    if not isinstance(xidx, slice):
        if not isinstance(xidx, (list, set)): xidx = [xidx]
        if len(xidx) > 1:
            xdensity = xdensity*(float(len(xidx))/xs)
        ls, rs = [], []
        for idx in xidx:
            xc, _ = scs.closest_cell_center(idx, b)
            ls.append(xc-xunit/2)
            rs.append(xc+xunit/2)
        l, r = np.min(ls), np.max(rs)
    elif not isinstance(yidx, slice):
        if not isinstance(yidx, (set, list)): yidx = [yidx]
        if len(yidx) > 1:
            ydensity = ydensity*(float(len(yidx))/ys)
        bs, ts = [], []
        for idx in yidx:
            _, yc = scs.closest_cell_center(l, idx)
            bs.append(yc-yunit/2)
            ts.append(yc+yunit/2)
        b, t = np.min(bs), np.max(ts)
    return BoundingBox(points=((l, b), (r, t)))


def reduce_fn(x):
    """Aggregation function to get the first non-zero value.

    """
    import pandas as pd
    values = x.values if isinstance(x, pd.Series) else x
    for v in values:
        if not is_nan(v):
            return v
    return np.nan


class categorical_aggregate2d(Operation):
    """Generates a gridded Dataset of 2D aggregate arrays indexed by the
    first two dimensions of the passed Element, turning all remaining
    dimensions into value dimensions. The key dimensions of the
    gridded array are treated as categorical indices. Useful for data
    indexed by two independent categorical variables such as a table
    of population values indexed by country and year. Data that is
    indexed by continuous dimensions should be binned before
    aggregation. The aggregation will retain the global sorting order
    of both dimensions.

    >>> table = Table([('USA', 2000, 282.2), ('UK', 2005, 58.89)],
                     kdims=['Country', 'Year'], vdims=['Population'])
    >>> categorical_aggregate2d(table)
    Dataset({'Country': ['USA', 'UK'], 'Year': [2000, 2005],
             'Population': [[ 282.2 , np.nan], [np.nan,   58.89]]},
            kdims=['Country', 'Year'], vdims=['Population'])

    """

    datatype = param.List(default=['xarray', 'grid'], doc="""
        The grid interface types to use when constructing the gridded Dataset.""")

    @classmethod
    def _get_coords(cls, obj: Dataset):
        """Get the coordinates of the 2D aggregate, maintaining the correct
        sorting order.

        """
        xdim, ydim = obj.dimensions(label=True)[:2]
        xcoords = obj.dimension_values(xdim, False)
        ycoords = obj.dimension_values(ydim, False)

        if dtype_kind(xcoords) not in 'SUO':
            xcoords = sort_arr(xcoords)
        if dtype_kind(ycoords) not in 'SUO':
            return xcoords, sort_arr(ycoords)

        # Determine global orderings of y-values using topological sort
        grouped = obj.groupby(xdim, container_type=dict,
                              group_type=Dataset).values()
        orderings = {}
        sort = True
        for group in grouped:
            vals = group.dimension_values(ydim, False)
            if len(vals) == 1:
                orderings[vals[0]] = [vals[0]]
            else:
                for p1, p2 in itertools.pairwise(vals):
                    orderings[p1] = [p2]
            if sort:
                if dtype_kind(vals) in ('i', 'f'):
                    sort = (np.diff(vals)>=0).all()
                else:
                    sort = np.array_equal(sort_arr(vals), vals)
        if sort or one_to_one(orderings, ycoords):
            ycoords = sort_arr(ycoords)
        elif not is_cyclic(orderings):
            coords = list(itertools.chain(*sort_topologically(orderings)))
            ycoords = coords if len(coords) == len(ycoords) else sort_arr(ycoords)
        return np.asarray(xcoords), np.asarray(ycoords)

    def _aggregate_dataset(self, obj):
        """Generates a gridded Dataset from a column-based dataset and
        lists of xcoords and ycoords

        """
        xcoords, ycoords = self._get_coords(obj)
        dim_labels = obj.dimensions(label=True)
        vdims = obj.dimensions()[2:]
        xdim, ydim = dim_labels[:2]
        shape = (len(ycoords), len(xcoords))
        nsamples = np.prod(shape)
        grid_data = {xdim: xcoords, ydim: ycoords}

        ys, xs = cartesian_product([ycoords, xcoords], copy=True)
        data = {xdim: xs, ydim: ys}
        for vdim in vdims:
            values = np.empty(nsamples)
            values[:] = np.nan
            data[vdim.name] = values
        dtype = default_datatype
        dense_data = Dataset(data, kdims=obj.kdims, vdims=obj.vdims, datatype=[dtype])
        concat_data = obj.interface.concatenate([dense_data, obj], datatype=dtype)
        reindexed = concat_data.reindex([xdim, ydim], vdims)
        if not reindexed:
            agg = reindexed
        df = PandasInterface.as_dframe(reindexed)
        df = df.groupby([xdim, ydim], sort=False).first().reset_index()
        agg = reindexed.clone(df)

        # Convert data to a gridded dataset
        for vdim in vdims:
            grid_data[vdim.name] = agg.dimension_values(vdim).reshape(shape)
        return agg.clone(grid_data, kdims=[xdim, ydim], vdims=vdims,
                         datatype=self.p.datatype)

    def _aggregate_dataset_pandas(self, obj):
        import pandas as pd
        index_cols = [d.name for d in obj.kdims]
        groupby_kwargs = {"sort": False}
        if PANDAS_GE_2_1_0:
            groupby_kwargs["observed"] = False
        df = obj.data
        if not all(c in df.index.names for c in index_cols):
            df = df.set_index(index_cols)
        df = df.groupby(index_cols, **groupby_kwargs).first()
        label = 'unique' if len(df) == len(obj) else 'non-unique'
        levels = self._get_coords(obj)
        index = pd.MultiIndex.from_product(levels, names=df.index.names)
        reindexed = df.reindex(index)
        data = tuple(levels)
        shape = tuple(d.shape[0] for d in data)
        for vdim in obj.vdims:
            data += (reindexed[vdim.name].values.reshape(shape).T,)
        return obj.clone(data, datatype=self.p.datatype, label=label)

    def _process(self, obj, key=None):
        """Generates a categorical 2D aggregate by inserting NaNs at all
        cross-product locations that do not already have a value assigned.
        Returns a 2D gridded Dataset object.

        """
        if isinstance(obj, Dataset) and obj.interface.gridded:
            return obj
        elif obj.ndims > 2:
            raise ValueError("Cannot aggregate more than two dimensions")
        elif len(obj.dimensions()) < 3:
            raise ValueError("Must have at two dimensions to aggregate over"
                             "and one value dimension to aggregate on.")

        obj = Dataset(obj, datatype=['dataframe'])
        return self._aggregate_dataset_pandas(obj)


def circular_layout(nodes):
    """Lay out nodes on a circle and add node index.

    """
    N = len(nodes)
    if not N:
        return ([], [], [])
    circ = np.pi/N*np.arange(N)*2
    x = np.cos(circ)
    y = np.sin(circ)
    return (x, y, nodes)


def quadratic_bezier(start, end, c0=(0, 0), c1=(0, 0), steps=50):
    """Compute quadratic bezier spline given start and end coordinate and
    two control points.

    """
    steps = np.linspace(0, 1, steps)
    sx, sy = start
    ex, ey = end
    cx0, cy0 = c0
    cx1, cy1 = c1
    xs = ((1-steps)**3*sx + 3*((1-steps)**2)*steps*cx0 +
          3*(1-steps)*steps**2*cx1 + steps**3*ex)
    ys = ((1-steps)**3*sy + 3*((1-steps)**2)*steps*cy0 +
          3*(1-steps)*steps**2*cy1 + steps**3*ey)
    return np.column_stack([xs, ys])


def connect_edges_pd(graph):
    """Given a Graph element containing abstract edges compute edge
    segments directly connecting the source and target nodes. This
    operation depends on pandas and is a lot faster than the pure
    NumPy equivalent.

    """
    import pandas as pd
    edges = graph.dframe()
    edges.index.name = 'graph_edge_index'
    edges = edges.reset_index()
    nodes = graph.nodes.dframe()
    src, tgt = graph.kdims
    x, y, idx = graph.nodes.kdims[:3]

    df = pd.merge(edges, nodes, left_on=[src.name], right_on=[idx.name])
    df = df.rename(columns={x.name: 'src_x', y.name: 'src_y'})

    df = pd.merge(df, nodes, left_on=[tgt.name], right_on=[idx.name])
    df = df.rename(columns={x.name: 'dst_x', y.name: 'dst_y'})
    df = df.sort_values('graph_edge_index').drop(['graph_edge_index'], axis=1)

    cols = ["src_x", "src_y", "dst_x", "dst_y"]
    edge_segments = list(df[cols].values.reshape(df.index.size, 2, 2))
    return edge_segments


def connect_tri_edges_pd(trimesh):
    """Given a TriMesh element containing abstract edges compute edge
    segments directly connecting the source and target nodes. This
    operation depends on pandas and is a lot faster than the pure
    NumPy equivalent.

    """
    import pandas as pd
    edges = trimesh.dframe().copy()
    edges.index.name = 'trimesh_edge_index'
    edges = edges.drop("color", errors="ignore", axis=1).reset_index()
    nodes = trimesh.nodes.dframe().copy()
    nodes.index.name = 'node_index'
    nodes = nodes.drop(["color", "z"], errors="ignore", axis=1)
    v1, v2, v3 = trimesh.kdims
    x, y, idx = trimesh.nodes.kdims[:3]

    df = pd.merge(edges, nodes, left_on=[v1.name], right_on=[idx.name])
    df = df.rename(columns={x.name: 'x0', y.name: 'y0'})
    df = pd.merge(df, nodes, left_on=[v2.name], right_on=[idx.name])
    df = df.rename(columns={x.name: 'x1', y.name: 'y1'})
    df = pd.merge(df, nodes, left_on=[v3.name], right_on=[idx.name])
    df = df.rename(columns={x.name: 'x2', y.name: 'y2'})
    df = df.sort_values('trimesh_edge_index').drop(['trimesh_edge_index'], axis=1)
    return df[['x0', 'y0', 'x1', 'y1', 'x2', 'y2']]


def connect_edges(graph):
    """Given a Graph element containing abstract edges compute edge
    segments directly connecting the source and target nodes.  This
    operation just uses internal HoloViews operations and will be a
    lot slower than the pandas equivalent.

    """
    paths = []
    for start, end in graph.array(graph.kdims):
        start_ds = graph.nodes[:, :, start]
        end_ds = graph.nodes[:, :, end]
        if not len(start_ds) or not len(end_ds):
            raise ValueError('Could not find node positions for all edges')
        start = start_ds.array(start_ds.kdims[:2])
        end = end_ds.array(end_ds.kdims[:2])
        paths.append(np.array([start[0], end[0]]))
    return paths


def sort_arr(arr: Array) -> Array:
    import pandas as pd

    if isinstance(arr, pd.api.extensions.ExtensionArray):
        return arr[arr.argsort()]
    return np.sort(arr)
