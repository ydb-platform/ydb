import param
from matplotlib.collections import PatchCollection
from matplotlib.patches import Rectangle

from ...core.util import max_range
from ...util.transform import dim
from .graphs import GraphPlot
from .util import filter_styles


class SankeyPlot(GraphPlot):

    labels = param.ClassSelector(class_=(str, dim), doc="""
        The dimension or dimension value transform used to draw labels from.""")

    show_values = param.Boolean(default=True, doc="""
        Whether to show the values.""")

    label_position = param.Selector(default='right', objects=['left', 'right'],
                                          doc="""
        Whether node labels should be placed to the left or right.""")

    node_width = param.Number(default=15, doc="""
        Width of the nodes.""")

    node_padding = param.Integer(default=None, doc="""
        Number of pixels of padding relative to the bounds.""")

    iterations = param.Integer(default=32, doc="""
        Number of iterations to run the layout algorithm.""")

    node_sort = param.Boolean(default=True, doc="""
        Sort nodes in ascending breadth.""")

    # Deprecated options

    color_index = param.ClassSelector(default=2, class_=(str, int),
                                      allow_None=True, doc="""
        Index of the dimension from which the node labels will be drawn""")

    label_index = param.ClassSelector(default=2, class_=(str, int),
                                      allow_None=True, doc="""
        Index of the dimension from which the node labels will be drawn""")

    filled = True

    style_opts = [*GraphPlot.style_opts, 'label_text_font_size']

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        """A Chord plot is always drawn on a unit circle.

        """
        if range_type == 'extents':
            return element.nodes.extents
        xdim, ydim = element.nodes.kdims[:2]
        xpad = .05 if self.label_index is None else 0.25
        x0, x1 = ranges[xdim.label][range_type]
        y0, y1 = ranges[ydim.label][range_type]
        xdiff = (x1-x0)
        ydiff = (y1-y0)
        if self.label_position == 'right':
            x0, x1 = x0-(0.05*xdiff), x1+xpad*xdiff
        else:
            x0, x1 = x0-xpad*xdiff, x1+(0.05*xdiff)
        x0, x1 = max_range([xdim.range, (x0, x1)])
        y0, y1 = max_range([ydim.range, (y0-(0.05*ydiff), y1+(0.05*ydiff))])
        return (x0, y0, x1, y1)

    def get_data(self, element, ranges, style):
        data, style, axis_kwargs = super().get_data(element, ranges, style)
        rects, labels = [], []

        label_dim = element.nodes.get_dimension(self.label_index)
        labels = self.labels
        if label_dim and labels:
            if self.label_index not in [2, None]:
                self.param.warning(
                    "Cannot declare style mapping for 'labels' option "
                    "and declare a label_index; ignoring the label_index.")
        elif label_dim:
            labels = label_dim
        if isinstance(labels, str):
            labels = element.nodes.get_dimension(labels)

        if labels is None:
            text = []
        if isinstance(labels, dim):
            text = labels.apply(element, flat=True)
        else:
            text = element.nodes.dimension_values(labels)
            text = [labels.pprint_value(v) for v in text]

        value_dim = element.vdims[0]
        text_labels = []
        for i, node in enumerate(element._sankey['nodes']):
            x0, x1, y0, y1 = (node[a+i] for a in 'xy' for i in '01')
            rect = {'height': y1-y0, 'width': x1-x0, 'xy': (x0, y0)}
            rects.append(rect)
            if text:
                label = text[i]
            else:
                label = ''
            if self.show_values:
                value = value_dim.pprint_value(node['value'], print_unit=True)
                if label:
                    label = f'{label} - {value}'
                else:
                    label = value
            if label:
                x = x1+(x1-x0)/4. if self.label_position == 'right' else x0-(x1-x0)/4.
                text_labels.append((label, (x, (y0+y1)/2.)))
        data['rects'] = rects
        if text_labels:
            data['text'] = text_labels
        return data, style, axis_kwargs

    def _update_labels(self, ax, data, style):
        labels = self.handles.get('labels', [])
        for label in labels:
            try:
                label.remove()
            except Exception:
                pass
        if 'text' not in data:
            return []

        fontsize = style.get('label_text_font_size', 8)
        align = 'left' if self.label_position == 'right' else 'right'
        labels = []
        for text in data['text']:
            label = ax.annotate(*text, xycoords='data',
                                horizontalalignment=align, fontsize=fontsize,
                                verticalalignment='center', rotation_mode='anchor')
            labels.append(label)
        return labels

    def init_artists(self, ax, plot_args, plot_kwargs):
        fontsize = plot_kwargs.pop('label_text_font_size', 8)
        artists = super().init_artists(ax, plot_args, plot_kwargs)
        groups = [g for g in self._style_groups if g != 'node']
        node_opts = filter_styles(plot_kwargs, 'node', groups, ('s', 'node_s'))
        rects = [Rectangle(**rect) for rect in plot_args['rects']]
        if 'vmin' in node_opts:
            node_opts['clim'] = node_opts.pop('vmin'), node_opts.pop('vmax')
        if 'c' in node_opts:
            node_opts['array'] = node_opts.pop('c')
        artists['rects'] = ax.add_collection(PatchCollection(rects, **node_opts))
        plot_kwargs['label_text_font_size'] = fontsize
        artists['labels'] = self._update_labels(ax, plot_args, plot_kwargs)
        return artists

    def update_handles(self, key, axis, element, ranges, style):
        data, style, axis_kwargs = self.get_data(element, ranges, style)
        self._update_nodes(element, data, style)
        self._update_edges(element, data, style)
        self.handles['labels'] = self._update_labels(axis, data, style)
        rects = self.handles['rects']
        paths = [Rectangle(**r) for r in data['rects']]
        rects.set_paths(paths)
        if 'node_facecolors' in style:
            rects.set_facecolors(style['node_facecolors'])
        return axis_kwargs
