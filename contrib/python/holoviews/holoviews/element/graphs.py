from collections import defaultdict
from types import FunctionType

import numpy as np
import param

from ..core import Dataset, Dimension, Element2D
from ..core.accessors import Redim
from ..core.operation import Operation
from ..core.util import dtype_kind, is_dataframe, max_range, search_indices
from .chart import Points
from .path import Path
from .util import (
    circular_layout,
    connect_edges_pd,
    connect_tri_edges_pd,
    quadratic_bezier,
    split_path,
)


class RedimGraph(Redim):
    """Extension for the redim utility that allows re-dimensioning
    Graph objects including their nodes and edgepaths.

    """

    def __call__(self, specs=None, **dimensions):
        redimmed = super().__call__(specs, **dimensions)
        new_data = (redimmed.data,)
        if self._obj.nodes:
            new_data = (*new_data, self._obj.nodes.redim(specs, **dimensions))
        if self._obj._edgepaths:
            new_data = (*new_data, self._obj.edgepaths.redim(specs, **dimensions))
        return redimmed.clone(new_data)


class layout_nodes(Operation):
    """Accepts a Graph and lays out the corresponding nodes with the
    supplied networkx layout function. If no layout function is
    supplied uses a simple circular_layout function. Also supports
    LayoutAlgorithm function provided in datashader layouts.

    """

    only_nodes = param.Boolean(default=False, doc="""
        Whether to return Nodes or Graph.""")

    layout = param.Callable(default=None, doc="""
        A NetworkX layout function""")

    kwargs = param.Dict(default={}, doc="""
        Keyword arguments passed to the layout function.""")

    def _process(self, element, key=None):
        if self.p.layout and isinstance(self.p.layout, FunctionType):
            import networkx as nx
            edges = element.array([0, 1])
            graph = nx.from_edgelist(edges)
            if 'weight' in self.p.kwargs:
                weight = self.p.kwargs['weight']
                for (s, t), w in zip(edges, element[weight], strict=None):
                    graph.edges[s, t][weight] = w
            positions = self.p.layout(graph, **self.p.kwargs)
            nodes = [(*pos, idx) for idx, pos in sorted(positions.items())]
        else:
            source = element.dimension_values(0, expanded=False)
            target = element.dimension_values(1, expanded=False)
            nodes = np.unique(np.concatenate([source, target]))
            if self.p.layout:
                import pandas as pd
                df = pd.DataFrame({'index': nodes})
                nodes = self.p.layout(df, element.dframe(), **self.p.kwargs)
                nodes = nodes[['x', 'y', 'index']]
            else:
                nodes = circular_layout(nodes)
        nodes = element.node_type(nodes)
        if element._nodes:
            for d in element.nodes.vdims:
                vals = element.nodes.dimension_values(d)
                nodes = nodes.add_dimension(d, len(nodes.vdims), vals, vdim=True)
        if self.p.only_nodes:
            return nodes
        return element.clone((element.data, nodes))


class Nodes(Points):
    """Nodes is a simple Element representing Graph nodes as a set of
    Points.  Unlike regular Points, Nodes must define a third key
    dimension corresponding to the node index.

    """

    kdims = param.List(default=[Dimension('x'), Dimension('y'),
                                Dimension('index')], bounds=(3, 3))

    group = param.String(default='Nodes', constant=True)


class EdgePaths(Path):
    """EdgePaths is a simple Element representing the paths of edges
    connecting nodes in a graph.

    """

    group = param.String(default='EdgePaths', constant=True)


class Graph(Dataset, Element2D):
    """Graph is high-level Element representing both nodes and edges.
    A Graph may be defined in an abstract form representing just
    the abstract edges between nodes and optionally may be made
    concrete by supplying a Nodes Element defining the concrete
    positions of each node. If the node positions are supplied
    the EdgePaths (defining the concrete edges) can be inferred
    automatically or supplied explicitly.

    The constructor accepts regular columnar data defining the edges
    or a tuple of the abstract edges and nodes, or a tuple of the
    abstract edges, nodes, and edgepaths.

    """

    group = param.String(default='Graph', constant=True)

    kdims = param.List(default=[Dimension('start'), Dimension('end')],
                       bounds=(2, 2))

    node_type = Nodes

    edge_type = EdgePaths

    def __init__(self, data, kdims=None, vdims=None, **params):
        if isinstance(data, tuple):
            data = data + (None,)* (3-len(data))
            edges, nodes, edgepaths = data
        elif isinstance(data, type(self)):
            edges, nodes, edgepaths = data, data.nodes, data._edgepaths
        else:
            edges, nodes, edgepaths = data, None, None

        if nodes is not None:
            node_info = None
            if isinstance(nodes, self.node_type):
                pass
            elif not isinstance(nodes, Dataset) or nodes.ndims == 3:
                nodes = self.node_type(nodes)
            else:
                node_info = nodes
                nodes = None
        else:
            node_info = None
        if edgepaths is not None and not isinstance(edgepaths, self.edge_type):
            edgepaths = self.edge_type(edgepaths)

        self._nodes = nodes
        self._edgepaths = edgepaths
        super().__init__(edges, kdims=kdims, vdims=vdims, **params)
        if node_info is not None:
            self._add_node_info(node_info)
        self._validate()

    @property
    def redim(self):
        return RedimGraph(self, mode='dataset')

    def _add_node_info(self, node_info):
        import pandas as pd

        nodes = self.nodes.clone(datatype=['pandas', 'dictionary'])
        if isinstance(node_info, self.node_type):
            nodes = nodes.redim(**dict(zip(nodes.dimensions('key', label=True),
                                           node_info.kdims, strict=None)))

        if not node_info.kdims and len(node_info) != len(nodes):
            raise ValueError("The supplied node data does not match "
                             "the number of nodes defined by the edges. "
                             "Ensure that the number of nodes match"
                             "or supply an index as the sole key "
                             "dimension to allow the Graph to merge "
                             "the data.")

        left_on = nodes.kdims[-1].name
        node_info_df = node_info.dframe()
        node_df = nodes.dframe()
        if node_info.kdims:
            idx = node_info.kdims[-1]
        else:
            idx = Dimension('index')
            node_info_df = node_info_df.reset_index()
        if 'index' in node_info_df.columns and not idx.name == 'index':
            node_df = node_df.rename(columns={'index': '__index'})
            left_on = '__index'
        cols = [c for c in node_info_df.columns if c not in
                node_df.columns or c == idx.name]
        node_info_df = node_info_df[cols]
        node_df = pd.merge(node_df, node_info_df, left_on=left_on,
                            right_on=idx.name, how='left')
        nodes = nodes.clone(node_df, kdims=[*nodes.kdims[:2], idx],
                            vdims=node_info.vdims)

        self._nodes = nodes


    def _validate(self):
        if self._edgepaths is None:
            return
        mismatch = []
        for kd1, kd2 in zip(self.nodes.kdims, self.edgepaths.kdims, strict=None):
            if kd1 != kd2:
                mismatch.append(f'{kd1} != {kd2}')
        if mismatch:
            raise ValueError('Ensure that the first two key dimensions on '
                             'Nodes and EdgePaths match: {}'.format(', '.join(mismatch)))
        npaths = len(self._edgepaths.data)
        nedges = len(self)
        if nedges != npaths:
            mismatch = True
            if npaths == 1:
                edges = self.edgepaths.split()[0]
                vals = edges.dimension_values(0)
                npaths = len(np.where(np.isnan(vals))[0])
                if not np.isnan(vals[-1]):
                    npaths += 1
                mismatch = npaths != nedges
            if mismatch:
                raise ValueError('Ensure that the number of edges supplied '
                                 f'to the Graph ({nedges}) matches the number of '
                                 f'edgepaths ({npaths})')

    def clone(self, data=None, shared_data=True, new_type=None, link=True,
              *args, **overrides):
        if data is None:
            data = (self.data, self.nodes)
            if self._edgepaths is not None:
                data = (*data, self.edgepaths)
            overrides['plot_id'] = self._plot_id
        elif not isinstance(data, tuple):
            data = (data, self.nodes)
            if self._edgepaths:
                data = (*data, self.edgepaths)
        return super().clone(data, shared_data, new_type, link,
                             *args, **overrides)

    def select(self, selection_expr=None, selection_specs=None, selection_mode='edges', **selection):
        """Allows selecting data by the slices, sets and scalar values
        along a particular dimension. The indices should be supplied as
        keywords mapping between the selected dimension and
        value. Additionally selection_specs (taking the form of a list
        of type.group.label strings, types or functions) may be
        supplied, which will ensure the selection is only applied if the
        specs match the selected object.

        Selecting by a node dimensions selects all edges and nodes that are
        connected to the selected nodes. To select only edges between the
        selected nodes set the selection_mode to 'nodes'.

        """
        from ..util.transform import dim
        if selection_expr is not None and not isinstance(selection_expr, dim):
            raise ValueError("""\
            The first positional argument to the Dataset.select method is expected to be a
            holoviews.util.transform.dim expression. Use the selection_specs keyword
            argument to specify a selection specification""")

        sel_dims = (*self.dimensions('ranges'), 'selection_mask')
        selection = {dim: sel for dim, sel in selection.items() if dim in sel_dims}
        if (selection_specs and not any(self.matches(sp) for sp in selection_specs)
            or (not selection and not selection_expr)):
            return self

        index_dim = self.nodes.kdims[2].name
        dimensions = self.kdims+self.vdims
        node_selection = {index_dim: v for k, v in selection.items()  # noqa: B035
                          if k in self.kdims}
        if selection_expr:
            mask = selection_expr.apply(self.nodes, compute=False, keep_index=True)
            nodes = self.nodes[mask]
        else:
            nodes = self.nodes.select(**dict(selection, **node_selection))
        selection = {k: v for k, v in selection.items() if k in dimensions}

        # Compute mask for edges if nodes were selected on
        nodemask = None
        if len(nodes) != len(self.nodes):
            xdim, ydim = dimensions[:2]
            indices = list(nodes.dimension_values(2, False))
            if selection_mode == 'edges':
                mask1 = self.interface.select_mask(self, {xdim.name: indices})
                mask2 = self.interface.select_mask(self, {ydim.name: indices})
                nodemask = (mask1 | mask2)
                nodes = self.nodes
            else:
                nodemask = self.interface.select_mask(self, {xdim.name: indices,
                                                             ydim.name: indices})

        # Compute mask for edge selection
        mask = None
        if selection:
            mask = self.interface.select_mask(self, selection)

        # Combine masks
        if nodemask is not None:
            if mask is not None:
                mask &= nodemask
            else:
                mask = nodemask

        # Apply edge mask
        if mask is not None:
            data = self.interface.select(self, mask)
            if not np.all(mask):
                new_graph = self.clone((data, nodes))
                source = new_graph.dimension_values(0, expanded=False)
                target = new_graph.dimension_values(1, expanded=False)
                unique_nodes = np.unique(np.concatenate([source, target]))
                nodes = new_graph.nodes[:, :, list(unique_nodes)]
            paths = None
            if self._edgepaths:
                edgepaths = self._split_edgepaths
                paths = edgepaths.clone(edgepaths.interface.select_paths(edgepaths, mask))
                if len(self._edgepaths.data) == 1:
                    paths = paths.clone([paths.dframe()])
        else:
            data = self.data
            paths = self._edgepaths
        return self.clone((data, nodes, paths))

    @property
    def _split_edgepaths(self):
        if len(self) == len(self.edgepaths.data):
            return self.edgepaths
        else:
            return self.edgepaths.clone(split_path(self.edgepaths))

    def range(self, dimension, data_range=True, dimension_range=True):
        if self.nodes and dimension in self.nodes.dimensions():
            node_range = self.nodes.range(dimension, data_range, dimension_range)
            if self._edgepaths:
                path_range = self._edgepaths.range(dimension, data_range, dimension_range)
                return max_range([node_range, path_range])
            return node_range
        return super().range(dimension, data_range, dimension_range)

    def dimensions(self, selection='all', label=False):
        dimensions = super().dimensions(selection, label)
        if selection == 'ranges':
            if self._nodes is not None:
                node_dims = self.nodes.dimensions(selection, label)
            else:
                node_dims = self.node_type.kdims+self.node_type.vdims
                if label in ['name', True, 'short']:
                    node_dims = [d.name for d in node_dims]
                elif label in ['long', 'label']:
                    node_dims = [d.label for d in node_dims]
            return dimensions+node_dims
        return dimensions

    @property
    def nodes(self):
        """Computes the node positions the first time they are requested
        if no explicit node information was supplied.

        """
        if self._nodes is None:
            from ..operation.element import chain
            self._nodes = layout_nodes(self, only_nodes=True)
            self._nodes._dataset = None
            self._nodes._pipeline = chain.instance()
        return self._nodes

    @property
    def edgepaths(self):
        """Returns the fixed EdgePaths or computes direct connections
        between supplied nodes.

        """
        if self._edgepaths:
            return self._edgepaths
        paths = connect_edges_pd(self)
        return self.edge_type(paths, kdims=self.nodes.kdims[:2])

    @classmethod
    def from_networkx(cls, G, positions, nodes=None, **kwargs):
        """Generate a HoloViews Graph from a networkx.Graph object and
        networkx layout function or dictionary of node positions.
        Any keyword arguments will be passed to the layout
        function. By default it will extract all node and edge
        attributes from the networkx.Graph but explicit node
        information may also be supplied. Any non-scalar attributes,
        such as lists or dictionaries will be ignored.

        Parameters
        ----------
        G : networkx.Graph
            Graph to convert to Graph element
        positions : dict or callable
            Node positions
            Node positions defined as a dictionary mapping from
            node id to (x, y) tuple or networkx layout function
            which computes a positions dictionary
        kwargs : dict
            Keyword arguments for layout function

        Returns
        -------
        Graph element
        """
        if not isinstance(positions, dict):
            positions = positions(G, **kwargs)

        # Unpack edges
        edges = defaultdict(list)
        for start, end in G.edges():
            for attr, value in sorted(G.adj[start][end].items()):
                if isinstance(value, (list, dict)):
                    continue # Cannot handle list or dict attrs
                edges[attr].append(value)

            # Handle tuple node indexes (used in 2D grid Graphs)
            if isinstance(start, tuple):
                start = str(start)
            if isinstance(end, tuple):
                end = str(end)
            edges['start'].append(start)
            edges['end'].append(end)
        edge_cols = sorted([k for k in edges if k not in ('start', 'end')
                            and len(edges[k]) == len(edges['start'])])
        edge_vdims = [str(col) if isinstance(col, int) else col for col in edge_cols]
        edge_data = tuple(edges[col] for col in ['start', 'end', *edge_cols])

        # Unpack user node info
        xdim, ydim, idim = cls.node_type.kdims[:3]
        if nodes:
            node_columns = nodes.columns()
            idx_dim = nodes.kdims[0].name
            info_cols, values = zip(*((k, v) for k, v in node_columns.items() if k != idx_dim), strict=None)
            node_info = {i: vals for i, vals in zip(node_columns[idx_dim], zip(*values, strict=None), strict=None)}
        else:
            info_cols = []
            node_info = None
        node_columns = defaultdict(list)

        # Unpack node positions
        for idx, pos in sorted(positions.items()):
            node = G.nodes.get(idx)
            if node is None:
                continue
            x, y = pos
            node_columns[xdim.name].append(x)
            node_columns[ydim.name].append(y)
            for attr, value in node.items():
                if isinstance(value, (list, dict)):
                    continue
                node_columns[attr].append(value)
            for i, col in enumerate(info_cols):
                node_columns[col].append(node_info[idx][i])
            if isinstance(idx, tuple):
                idx = str(idx) # Tuple node indexes handled as strings
            node_columns[idim.name].append(idx)
        node_cols = sorted([k for k in node_columns if k not in cls.node_type.kdims
                            and len(node_columns[k]) == len(node_columns[xdim.name])])
        columns = [xdim.name, ydim.name, idim.name, *node_cols, *info_cols]
        node_data = tuple(node_columns[col] for col in columns)

        # Construct nodes
        vdims = []
        for col in node_cols:
            if isinstance(col, int):
                dim = str(col)
            elif nodes is not None and col in nodes.vdims:
                dim = nodes.get_dimension(col)
            else:
                dim = col
            vdims.append(dim)
        nodes = cls.node_type(node_data, vdims=vdims)

        # Construct graph
        return cls((edge_data, nodes), vdims=edge_vdims)

    @classmethod
    def from_sparse(cls, edges, nodes, **params):
        """Create a Graph element from a sparse adjacency array/matrix.

        Parameters
        ----------
        edges : scipy.sparse array/matrix
            A sparse array/matrix representing the graph adjacency
            array/matrix. The array/matrix should have shape (n_nodes, n_nodes)
            where non-zero entries indicate edges between nodes.
            The array/matrix will be converted to COO format.
        nodes : array-like or Dataset
            Node positions or node information. Can be a 2D array of (x, y)
            coordinates, a Dataset with node information, or a Nodes element.
        **params : dict, optional
            Additional parameters passed to the Graph constructor.

        Returns
        -------
        Graph
            A Graph element with edges defined by the sparse array/matrix and the
            provided node information.

        Raises
        ------
        TypeError
            If edges is not a scipy sparse array/matrix.

        Examples
        --------
        >>> from scipy.sparse import csr_array
        >>> # Create a simple 3-node graph
        >>> edges = csr_array([[0, 1, 0], [1, 0, 1], [0, 1, 0]])
        >>> nodes = {'x': [0, 1, 0.5], 'y': [0, 0, 1], 'index': [0, 1, 2]}
        >>> graph = Graph.from_sparse(edges, nodes)
        """
        from scipy.sparse import issparse

        if not issparse(edges):
            msg = f"edges expected to be a scipy.sparse array/matrix, not {type(edges).__name__}"
            raise TypeError(msg)

        keys = [*map(str, cls.kdims), "data"]
        if kdims := params.get('kdims'):
            if isinstance(kdims, str):
                keys[0] = kdims
            else:
                keys[:len(kdims)] = kdims
        if vdims := params.get('vdims'):
            if isinstance(vdims, str):
                keys[2] = vdims
            else:
                keys[2] = vdims[0]
        else:
            params["vdims"] = "data"

        edges = edges.tocoo()
        values = [edges.row, edges.col, edges.data]
        edges_data = dict(zip(keys, values, strict=True))
        return cls((edges_data, nodes), **params)


class TriMesh(Graph):
    """A TriMesh represents a mesh of triangles represented as the
    simplices and nodes. The simplices represent a indices into the
    nodes array. The mesh therefore follows a datastructure very
    similar to a graph, with the abstract connectivity between nodes
    stored on the TriMesh element itself, the node positions stored on
    a Nodes element and the concrete paths making up each triangle
    generated when required by accessing the edgepaths.

    Unlike a Graph each simplex is represented as the node indices of
    the three corners of each triangle.

    """

    kdims = param.List(default=['node1', 'node2', 'node3'],
                       bounds=(3, 3), doc="""
        Dimensions declaring the node indices of each triangle.""")

    group = param.String(default='TriMesh', constant=True)

    point_type = Points

    def __init__(self, data, kdims=None, vdims=None, **params):
        if isinstance(data, tuple):
            data = data + (None,)*(3-len(data))
            edges, nodes, edgepaths = data
        elif isinstance(data, type(self)):
            edges, nodes, edgepaths = data, data.nodes, data._edgepaths
        else:
            edges, nodes, edgepaths = data, None, None

        super().__init__(edges, kdims=kdims, vdims=vdims, **params)
        if nodes is None:
            if len(self) == 0:
                nodes = []
            else:
                raise ValueError("TriMesh expects both simplices and nodes "
                                 "to be supplied.")

        if isinstance(nodes, self.node_type):
            pass
        elif isinstance(nodes, self.point_type):
            # Add index to make it a valid Nodes object
            nodes = self.node_type(Dataset(nodes).add_dimension('index', 2, np.arange(len(nodes))))
        elif not isinstance(nodes, Dataset) or nodes.ndims in [2, 3]:
            if is_dataframe(nodes):
                coords = list(nodes.columns)[:2]
                index = nodes.index.name or 'index'
                nodes = self.node_type(nodes, [*coords, index])
            else:
                try:
                    points = self.point_type(nodes)
                    ds = Dataset(points).add_dimension('index', 2, np.arange(len(points)))
                    nodes = self.node_type(ds)
                except Exception as e:
                    raise ValueError(
                        "Nodes argument could not be interpreted, expected "
                        "data with two or three columns representing the "
                        "x/y positions and optionally the node indices.") from e
        if edgepaths is not None and not isinstance(edgepaths, self.edge_type):
            edgepaths = self.edge_type(edgepaths)

        self._nodes = nodes
        self._edgepaths = edgepaths

    @classmethod
    def from_vertices(cls, data):
        """Uses Delauney triangulation to compute triangle simplices for
        each point.

        """
        try:
            from scipy.spatial import Delaunay
        except ImportError:
            raise ImportError("Generating triangles from points requires "
                              "SciPy to be installed.") from None
        if not isinstance(data, Points):
            data = Points(data)
        if not len(data):
            return cls(([], []))
        tris = Delaunay(data.array([0, 1]))
        return cls((tris.simplices, data))

    def _initialize_edgepaths(self):
        """Returns the EdgePaths by generating a triangle for each simplex.

        """
        if self._edgepaths:
            return self._edgepaths
        elif not len(self):
            edgepaths = self.edge_type([], kdims=self.nodes.kdims[:2])
            self._edgepaths = edgepaths
            return edgepaths

        df = connect_tri_edges_pd(self)
        pts = df.values.reshape((len(df), 3, 2))
        paths = np.pad(
            pts[:, [0, 1, 2, 0], :].astype(float),
            pad_width=((0, 0), (0, 1), (0, 0)),
            mode='constant', constant_values=np.nan
        ).reshape(-1, 2)[:-1]
        edgepaths = self.edge_type([paths], kdims=self.nodes.kdims[:2],
                                   datatype=['multitabular'])
        self._edgepaths = edgepaths
        return edgepaths

    @property
    def edgepaths(self):
        """Returns the EdgePaths by generating a triangle for each simplex.

        """
        return self._initialize_edgepaths()

    def select(self, selection_expr=None, selection_specs=None, **selection):
        """Allows selecting data by the slices, sets and scalar values
        along a particular dimension. The indices should be supplied as
        keywords mapping between the selected dimension and
        value. Additionally selection_specs (taking the form of a list
        of type.group.label strings, types or functions) may be
        supplied, which will ensure the selection is only applied if the
        specs match the selected object.

        """
        self._initialize_edgepaths()
        return super().select(
            selection_expr=selection_expr, selection_specs=selection_specs,
            selection_mode='nodes', **selection
        )



class layout_chords(Operation):
    """layout_chords computes the locations of each node on a circle and
    the chords connecting them. The amount of radial angle devoted to
    each node and the number of chords are scaled by the value
    dimension of the Chord element. If the values are integers then
    the number of chords is directly scaled by the value, if the
    values are floats then the number of chords are apportioned such
    that the lowest value edge is given one chord and all other nodes
    are given nodes proportional to their weight. The max_chords
    parameter scales the number of chords to be assigned to an edge.

    The chords are computed by interpolating a cubic spline from the
    source to the target node in the graph, the number of samples to
    interpolate the spline with is given by the chord_samples
    parameter.

    """

    chord_samples = param.Integer(default=50, bounds=(0, None), doc="""
        Number of samples per chord for the spline interpolation.""")

    max_chords = param.Integer(default=500, doc="""
        Maximum number of chords to render.""")

    def _process(self, element, key=None):
        nodes_el = element._nodes
        if nodes_el:
            idx_dim = nodes_el.kdims[-1]
            nodes = nodes_el.dimension_values(idx_dim, expanded=False)
        else:
            source = element.dimension_values(0, expanded=False)
            target = element.dimension_values(1, expanded=False)
            nodes = np.unique(np.concatenate([source, target]))

        # Compute indices and values for connectivity matrix
        max_chords = self.p.max_chords
        src, tgt = (element.dimension_values(i) for i in range(2))
        src_idx = search_indices(src, nodes)
        tgt_idx = search_indices(tgt, nodes)
        if element.vdims:
            values = element.dimension_values(2)
            if dtype_kind(values) not in 'uif':
                values = np.ones(len(element), dtype='int')
            else:
                if dtype_kind(values) == 'f':
                    values = np.ceil(values*(1./values.min()))
                if values.sum() > max_chords:
                    values = np.ceil((values/float(values.sum()))*max_chords)
                    values = values.astype('int64')
        else:
            values = np.ones(len(element), dtype='int')

        # Compute connectivity matrix
        matrix = np.zeros((len(nodes), len(nodes)))
        for s, t, v in zip(src_idx, tgt_idx, values, strict=None):
            matrix[s, t] += v

        # Compute weighted angular slice for each connection
        weights_of_areas = (matrix.sum(axis=0) + matrix.sum(axis=1))
        areas_in_radians = (weights_of_areas / weights_of_areas.sum()) * (2 * np.pi)

        # We add a zero in the begging for the cumulative sum
        points = np.zeros(areas_in_radians.shape[0] + 1)
        points[1:] = areas_in_radians
        points = points.cumsum()

        # Compute mid-points for node positions
        midpoints = np.convolve(points, [0.5, 0.5], mode='valid')
        mxs = np.cos(midpoints)
        mys = np.sin(midpoints)

        # Compute angles of chords in each edge
        all_areas = []
        for i in range(areas_in_radians.shape[0]):
            n_conn = weights_of_areas[i]
            p0, p1 = points[i], points[i+1]
            angles = np.linspace(p0, p1, int(n_conn))
            coords = list(zip(np.cos(angles), np.sin(angles), strict=None))
            all_areas.append(coords)

        # Draw each chord by interpolating quadratic splines
        # Separate chords in each edge by NaNs
        empty = np.array([[np.nan, np.nan]])
        paths = []
        for i in range(len(element)):
            sidx, tidx = src_idx[i], tgt_idx[i]
            src_area, tgt_area = all_areas[sidx], all_areas[tidx]
            n_conns = matrix[sidx, tidx]
            subpaths = []
            for _ in range(int(n_conns)):
                if not src_area or not tgt_area:
                    continue
                x0, y0 = src_area.pop()
                if not tgt_area:
                    continue
                x1, y1 = tgt_area.pop()
                b = quadratic_bezier((x0, y0), (x1, y1), (x0/2., y0/2.),
                                     (x1/2., y1/2.), steps=self.p.chord_samples)
                subpaths.append(b)
                subpaths.append(empty)
            subpaths = [p for p in subpaths[:-1] if len(p)]
            if subpaths:
                paths.append(np.concatenate(subpaths))
            else:
                paths.append(np.empty((0, 2)))

        # Construct Chord element from components
        if nodes_el:
            if isinstance(nodes_el, Nodes):
                kdims = nodes_el.kdims
            else:
                kdims = [*Nodes.kdims[:2], idx_dim]
            vdims = [vd for vd in nodes_el.vdims if vd not in kdims]
            values = tuple(nodes_el.dimension_values(vd) for vd in vdims)
        else:
            kdims = Nodes.kdims
            values, vdims = (), []

        if len(nodes):
            node_data = (mxs, mys, nodes, *values)
        else:
            node_data = tuple([] for _ in kdims+vdims)

        nodes = Nodes(node_data, kdims=kdims, vdims=vdims)
        edges = EdgePaths(paths)
        chord = Chord((element.data, nodes, edges), compute=False)
        chord._angles = points
        return chord


class Chord(Graph):
    """Chord is a special type of Graph which computes the locations of
    each node on a circle and the chords connecting them. The amount
    of radial angle devoted to each node and the number of chords are
    scaled by a weight supplied as a value dimension.

    If the values are integers then the number of chords is directly
    scaled by the value, if the values are floats then the number of
    chords are apportioned such that the lowest value edge is given
    one chord and all other nodes are given nodes proportional to
    their weight.

    """

    group = param.String(default='Chord', constant=True)

    def __init__(self, data, kdims=None, vdims=None, compute=True, **params):
        if data is None or isinstance(data, list) and data == []:
            data = (([], [], []),)

        if isinstance(data, tuple):
            data = data + (None,)* (3-len(data))
            edges, nodes, edgepaths = data
        else:
            edges, nodes, edgepaths = data, None, None

        if nodes is not None:
            if not isinstance(nodes, Dataset):
                if nodes.ndims == 3:
                    nodes = Nodes(nodes)
                else:
                    nodes = Dataset(nodes)
                    nodes = nodes.clone(kdims=nodes.kdims[0],
                                        vdims=nodes.kdims[1:])
        super(Graph, self).__init__(edges, kdims=kdims, vdims=vdims, **params)
        if compute:
            self._nodes = nodes
            chord = layout_chords(self)
            self._nodes = chord.nodes
            self._edgepaths = chord.edgepaths
            self._angles = chord._angles
        else:
            if not isinstance(nodes, Nodes):
                raise TypeError(f"Expected Nodes object in data, found {type(nodes)}.")
            self._nodes = nodes
            if not isinstance(edgepaths, EdgePaths):
                raise TypeError(f"Expected EdgePaths object in data, found {type(edgepaths)}.")
            self._edgepaths = edgepaths
        self._validate()

    @property
    def edgepaths(self):
        return self._edgepaths

    @property
    def nodes(self):
        return self._nodes
