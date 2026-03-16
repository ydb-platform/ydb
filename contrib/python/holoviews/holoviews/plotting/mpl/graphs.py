import warnings

import numpy as np
import param
from matplotlib.collections import LineCollection, PolyCollection

from ...core.data import Dataset
from ...core.options import Cycle, abbreviated_exception
from ...core.util import dtype_kind, is_number, isscalar, search_indices, unique_array
from ...util.transform import dim
from ..mixins import ChordMixin, GraphMixin
from ..util import get_directed_graph_paths, process_cmap
from .element import ColorbarPlot
from .util import MPL_GE_3_10_1, filter_styles


class GraphPlot(GraphMixin, ColorbarPlot):

    arrowhead_length = param.Number(default=0.025, doc="""
      If directed option is enabled this determines the length of the
      arrows as fraction of the overall extent of the graph.""")

    directed = param.Boolean(default=False, doc="""
      Whether to draw arrows on the graph edges to indicate the
      directionality of each edge.""")

    # Deprecated options

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `node_color=dim('color')`""")


    edge_color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `edge_color=dim('color')`""")

    style_opts = ['edge_alpha', 'edge_color', 'edge_linestyle', 'edge_linewidth',
                  'node_alpha', 'node_color', 'node_edgecolors', 'node_facecolors',
                  'node_linewidth', 'node_marker', 'node_size', 'visible', 'cmap',
                  'edge_cmap', 'node_cmap']

    _style_groups = ['node', 'edge']

    _nonvectorized_styles = [
        'edge_alpha', 'edge_linestyle', 'edge_cmap', 'cmap', 'visible',
        'node_marker'
    ]

    filled = False

    def _compute_styles(self, element, ranges, style):
        elstyle = self.lookup_options(element, 'style')
        color = elstyle.kwargs.get('node_color')
        cdim = element.nodes.get_dimension(self.color_index)
        cmap = elstyle.kwargs.get('cmap', 'tab20')
        if color and 'node_color' in style:
            style['node_facecolors'] = style.pop('node_color')
        elif cdim:
            cs = element.nodes.dimension_values(self.color_index)
            # Check if numeric otherwise treat as categorical
            if dtype_kind(cs) == 'f':
                style['node_c'] = cs
            else:
                factors = unique_array(cs)
                cmap = color if isinstance(color, Cycle) else cmap
                if isinstance(cmap, dict):
                    colors = [cmap.get(f, cmap.get('NaN', {'color': self._default_nan})['color'])
                              for f in factors]
                else:
                    colors = process_cmap(cmap, len(factors))
                cs = search_indices(cs, factors)
                style['node_facecolors'] = [colors[v%len(colors)] for v in cs]
                style.pop('node_color', None)
            if 'node_c' in style:
                self._norm_kwargs(element.nodes, ranges, style, cdim)
        style['node_edgecolors'] = style.pop('node_edgecolors', 'none')
        if is_number(style.get('node_size')):
            style['node_s'] = style.pop('node_size')**2

        edge_cdim = element.get_dimension(self.edge_color_index)
        if not edge_cdim:
            if not isscalar(style.get('edge_color')):
                opt = 'edge_facecolors' if self.filled else 'edge_edgecolors'
                style[opt] = style.pop('edge_color')
            return style

        elstyle = self.lookup_options(element, 'style')
        cycle = elstyle.kwargs.get('edge_color')
        idx = element.get_dimension_index(edge_cdim)
        cvals = element.dimension_values(edge_cdim)
        if idx in [0, 1]:
            factors = element.nodes.dimension_values(2, expanded=False)
        elif idx == 2 and dtype_kind(cvals) in 'uif':
            factors = None
        else:
            factors = unique_array(cvals)
        if factors is None or (dtype_kind(factors) == 'f' and idx not in [0, 1]):
            style['edge_c'] = cvals
        else:
            cvals = search_indices(cvals, factors)
            factors = list(factors)
            cmap = elstyle.kwargs.get('edge_cmap', 'tab20')
            cmap = cycle if isinstance(cycle, Cycle) else cmap
            if isinstance(cmap, dict):
                colors = [cmap.get(f, cmap.get('NaN', {'color': self._default_nan})['color'])
                          for f in factors]
            else:
                colors = process_cmap(cmap, len(factors))
            style['edge_colors'] = [colors[v%len(colors)] for v in cvals]
            style.pop('edge_color', None)
        if 'edge_c' in style:
            self._norm_kwargs(element, ranges, style, edge_cdim, prefix='edge_')
        else:
            style.pop('edge_cmap', None)
        return style

    def get_data(self, element, ranges, style):
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)

        pxs, pys = (element.nodes.dimension_values(i) for i in range(2))
        dims = element.nodes.dimensions()
        self._compute_styles(element, ranges, style)

        if 'edge_vmin' in style:
            style['edge_clim'] = (style.pop('edge_vmin'), style.pop('edge_vmax'))
        if 'edge_c' in style:
            style['edge_array'] = style.pop('edge_c')

        if self.directed:
            xdim, ydim = element.nodes.kdims[:2]
            x_range = ranges[xdim.label]['combined']
            y_range = ranges[ydim.label]['combined']
            arrow_len = np.hypot(y_range[1]-y_range[0], x_range[1]-x_range[0])*self.arrowhead_length
            paths = get_directed_graph_paths(element, arrow_len)
        else:
            paths = element._split_edgepaths.split(datatype='array', dimensions=element.edgepaths.kdims)

        if self.invert_axes:
            paths = [p[:, ::-1] for p in paths]
        return {'nodes': (pxs, pys), 'edges': paths}, style, {'dimensions': dims}

    def init_artists(self, ax, plot_args, plot_kwargs):
        # Draw edges
        color_opts = ['c', 'cmap', 'vmin', 'vmax', 'norm']
        groups = [g for g in self._style_groups if g != 'edge']
        edge_opts = filter_styles(plot_kwargs, 'edge', groups, color_opts)
        if 'c' in edge_opts:
            edge_opts['array'] = edge_opts.pop('c')
        paths = plot_args['edges']
        if self.filled:
            coll = PolyCollection
            if 'colors' in edge_opts:
                edge_opts['facecolors'] = edge_opts.pop('colors')
        else:
            coll = LineCollection
        edgecolors = edge_opts.pop('edgecolors', None)
        edges = coll(paths, **edge_opts)
        if edgecolors is not None:
            edges.set_edgecolors(edgecolors)
        ax.add_collection(edges)

        # Draw nodes
        xs, ys = plot_args['nodes']
        groups = [g for g in self._style_groups if g != 'node']
        node_opts = filter_styles(plot_kwargs, 'node', groups)
        # Matplotlib 3.10.1 started emitting this UserWarning:
        #   You passed both c and facecolor/facecolors for the markers.
        #   c has precedence over facecolor/facecolors.
        if MPL_GE_3_10_1 and "c" in node_opts:
            node_opts.pop("facecolors", None)
        with warnings.catch_warnings():
            # scatter have a default cmap and with an empty array will emit this warning
            warnings.filterwarnings('ignore', "No data for colormapping provided via 'c'")
            nodes = ax.scatter(xs, ys, **node_opts)

        return {'nodes': nodes, 'edges': edges}

    def _update_nodes(self, element, data, style):
        nodes = self.handles['nodes']
        xs, ys = data['nodes']
        nodes.set_offsets(np.column_stack([xs, ys]))
        if 'node_facecolors' in style:
            nodes.set_facecolors(style['node_facecolors'])
        if 'node_edgecolors' in style:
            nodes.set_edgecolors(style['node_edgecolors'])
        if 'node_c' in style:
            nodes.set_array(style['node_c'])
        if 'node_vmin' in style:
            nodes.set_clim((style['node_vmin'], style['node_vmax']))
        if 'node_norm' in style:
            nodes.norm = style['norm']
        if 'node_linewidth' in style:
            nodes.set_linewidths(style['node_linewidth'])
        if 'node_s' in style:
            sizes = style['node_s']
            if isscalar(sizes):
                nodes.set_sizes([sizes])
            else:
                nodes.set_sizes(sizes)

    def _update_edges(self, element, data, style):
        edges = self.handles['edges']
        paths = data['edges']
        edges.set_paths(paths)
        edges.set_visible(style.get('visible', True))
        if 'edge_facecolors' in style:
            edges.set_facecolors(style['edge_facecolors'])
        if 'edge_edgecolors' in style:
            edges.set_edgecolors(style['edge_edgecolors'])
        if 'edge_array' in style:
            edges.set_array(style['edge_array'])
        elif 'edge_colors' in style:
            if self.filled:
                edges.set_facecolors(style['edge_colors'])
            else:
                edges.set_edgecolors(style['edge_colors'])
        if 'edge_clim' in style:
            edges.set_clim(style['edge_clim'])
        if 'edge_norm' in style:
            edges.norm = style['edge_norm']
        if 'edge_linewidth' in style:
            edges.set_linewidths(style['edge_linewidth'])

    def update_handles(self, key, axis, element, ranges, style):
        data, style, axis_kwargs = self.get_data(element, ranges, style)
        self._update_nodes(element, data, style)
        self._update_edges(element, data, style)
        return axis_kwargs


class TriMeshPlot(GraphPlot):

    filled = param.Boolean(default=False, doc="""
        Whether the triangles should be drawn as filled.""")

    style_opts = [*GraphPlot.style_opts, 'edge_facecolors']

    def get_data(self, element, ranges, style):
        edge_color = style.get('edge_color')
        if edge_color not in element.nodes:
            edge_color = self.edge_color_index
        simplex_dim = element.get_dimension(edge_color)
        vertex_dim = element.nodes.get_dimension(edge_color)
        if not isinstance(self.edge_color_index, int) and vertex_dim and not simplex_dim:
            simplices = element.array([0, 1, 2])
            z = element.nodes.dimension_values(vertex_dim)
            z = z[simplices].mean(axis=1)
            element = element.add_dimension(vertex_dim, len(element.vdims), z, vdim=True)
        element._initialize_edgepaths()
        return super().get_data(element, ranges, style)


class ChordPlot(ChordMixin, GraphPlot):

    labels = param.ClassSelector(class_=(str, dim), doc="""
        The dimension or dimension value transform used to draw labels from.""")

    # Deprecated options

    label_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
      Index of the dimension from which the node labels will be drawn""")

    style_opts = [*GraphPlot.style_opts, 'text_font_size', 'label_offset']

    _style_groups = ['edge', 'node', 'arc']

    def get_data(self, element, ranges, style):
        data, style, plot_kwargs = super().get_data(element, ranges, style)
        angles = element._angles
        paths = []
        for i in range(len(element.nodes)):
            start, end = angles[i:i+2]
            vals = np.linspace(start, end, 20)
            paths.append(np.column_stack([np.cos(vals), np.sin(vals)]))
        data['arcs'] = paths
        if 'node_c' in style:
            style['arc_array'] = style['node_c']
            style['arc_clim'] = style['node_vmin'], style['node_vmax']
            style['arc_cmap'] = style['node_cmap']
        elif 'node_facecolors' in style:
            style['arc_colors'] = style['node_facecolors']
        style['arc_linewidth'] = 10

        label_dim = element.nodes.get_dimension(self.label_index)
        labels = self.labels
        if label_dim and labels:
            self.param.warning(
                "Cannot declare style mapping for 'labels' option "
                "and declare a label_index; ignoring the label_index.")
        elif label_dim:
            labels = label_dim
        if isinstance(labels, str):
            labels = element.nodes.get_dimension(labels)

        if labels is None:
            return data, style, plot_kwargs

        nodes = element.nodes
        if element.vdims:
            values = element.dimension_values(element.vdims[0])
            if dtype_kind(values) in 'uif':
                edges = Dataset(element)[values>0]
                nodes = list(np.unique([edges.dimension_values(i) for i in range(2)]))
                nodes = element.nodes.select(**{element.nodes.kdims[2].name: nodes})
        offset = style.get('label_offset', 1.05)
        xs, ys = (nodes.dimension_values(i)*offset for i in range(2))
        if isinstance(labels, dim):
            text = labels.apply(element, flat=True)
        else:
            text = element.nodes.dimension_values(labels)
            text = [labels.pprint_value(v) for v in text]
        angles = np.rad2deg(np.arctan2(ys, xs))
        data['text'] = (xs, ys, text, angles)
        return data, style, plot_kwargs


    def init_artists(self, ax, plot_args, plot_kwargs):
        artists = {}
        if 'arcs' in plot_args:
            color_opts = ['c', 'cmap', 'vmin', 'vmax', 'norm']
            groups = [g for g in self._style_groups if g != 'arc']
            edge_opts = filter_styles(plot_kwargs, 'arc', groups, color_opts)
            paths = plot_args['arcs']
            edges = LineCollection(paths, **edge_opts)
            ax.add_collection(edges)
            artists['arcs'] = edges

        artists.update(super().init_artists(ax, plot_args, plot_kwargs))
        if 'text' in plot_args:
            fontsize = plot_kwargs.get('text_font_size', 8)
            labels = []
            for (x, y, l, a) in zip(*plot_args['text'], strict=None):
                label = ax.annotate(l, xy=(x, y), xycoords='data', rotation=a,
                                    horizontalalignment='left', fontsize=fontsize,
                                    verticalalignment='center', rotation_mode='anchor')
                labels.append(label)
            artists['labels'] = labels
        return artists

    def _update_arcs(self, element, data, style):
        edges = self.handles['arcs']
        paths = data['arcs']
        edges.set_paths(paths)
        edges.set_visible(style.get('visible', True))
        if 'arc_array' in style:
            edges.set_array(style['arc_array'])
        if 'arc_clim' in style:
            edges.set_clim(style['arc_clim'])
        if 'arc_norm' in style:
            edges.set_norm(style['arc_norm'])
        if 'arc_colors' in style:
            edges.set_edgecolors(style['arc_colors'])
        elif 'arc_edgecolors' in style:
            edges.set_edgecolors(style['arc_edgecolors'])

    def _update_labels(self, ax, element, data, style):
        labels = self.handles.get('labels', [])
        for label in labels:
            try:
                label.remove()
            except Exception:
                pass
        if 'text' not in data:
            self.handles['labels'] = []
            return
        labels = []
        fontsize = style.get('text_font_size', 8)
        for (x, y, l, a) in zip(*data['text'], strict=None):
            label = ax.annotate(l, xy=(x, y), xycoords='data', rotation=a,
                                horizontalalignment='left', fontsize=fontsize,
                                verticalalignment='center', rotation_mode='anchor')
            labels.append(label)
        self.handles['labels'] = labels

    def update_handles(self, key, axis, element, ranges, style):
        data, style, axis_kwargs = self.get_data(element, ranges, style)
        self._update_nodes(element, data, style)
        self._update_edges(element, data, style)
        self._update_arcs(element, data, style)
        self._update_labels(axis, element, data, style)
        return axis_kwargs
