import numpy as np
import param
from bokeh.models import Patches

from ...core.data import Dataset
from ...core.util import dimension_sanitizer, max_range
from ...util.transform import dim
from .graphs import GraphPlot


class SankeyPlot(GraphPlot):

    labels = param.ClassSelector(class_=(str, dim), doc="""
        The dimension or dimension value transform used to draw labels from.""")

    label_position = param.Selector(default='right',
                                          objects=['left', 'right', 'outer', 'inner'],
                                          doc="""
        Whether node labels should be placed to the left, right, outer or inner.""")

    show_values = param.Boolean(default=True, doc="""
        Whether to show the values.""")

    show_legend = param.Boolean(default=False, doc="""
        Whether to show the values.""")

    node_width = param.Number(default=15, doc="""
        Width of the nodes.""")

    node_padding = param.Integer(default=None, doc="""
        Number of pixels of padding relative to the bounds.""")

    iterations = param.Integer(default=32, doc="""
        Number of iterations to run the layout algorithm.""")

    node_sort = param.Boolean(default=True, doc="""
        Sort nodes in ascending breadth.""")

    width = param.Integer(default=1000, allow_None=True, bounds=(0, None), doc="""
        The width of the component (in pixels). This can be either
        fixed or preferred width, depending on width sizing policy.""")

    height = param.Integer(default=600, allow_None=True, bounds=(0, None), doc="""
        The height of the component (in pixels).  This can be either
        fixed or preferred height, depending on height sizing policy.""")

    # Deprecated options

    color_index = param.ClassSelector(default=2, class_=(str, int),
                                      allow_None=True, doc="""
        Index of the dimension from which the node labels will be drawn""")

    label_index = param.ClassSelector(default=2, class_=(str, int),
                                      allow_None=True, doc="""
        Index of the dimension from which the node labels will be drawn""")

    _style_groups = dict(GraphPlot._style_groups, quad='node', text='label')

    _draw_order = ['graph', 'quad_1', 'text_1', 'text_2']

    style_opts = [*GraphPlot.style_opts, 'edge_fill_alpha', 'nodes_line_color', 'label_text_font_size']

    filled = True

    def _init_glyphs(self, plot, element, ranges, source):
        super()._init_glyphs(plot, element, ranges, source)
        renderer = plot.renderers.pop(plot.renderers.index(self.handles['glyph_renderer']))
        plot.renderers = [renderer, *plot.renderers]
        arc_renderer = self.handles['quad_1_glyph_renderer']
        scatter_renderer = self.handles['scatter_1_glyph_renderer']
        arc_renderer.view = scatter_renderer.view
        arc_renderer.data_source = scatter_renderer.data_source
        self.handles['quad_1_source'] = scatter_renderer.data_source
        self._sync_nodes()

    def get_data(self, element, ranges, style):
        data, mapping, style = super().get_data(element, ranges, style)
        self._compute_quads(element, data, mapping)
        style['nodes_line_color'] = 'black'
        self._compute_labels(element, data, mapping)
        self._patch_hover(element, data)
        return data, mapping, style

    def _init_glyph(self, plot, mapping, properties, key):
        if key == 'quad_1':
            properties.pop('size', None)
            mapping.pop('size', None)
        return super()._init_glyph(plot, mapping, properties, key)

    def _update_glyphs(self, element, ranges, style):
        self._sync_nodes()
        super()._update_glyphs(element, ranges, style)

    def _sync_nodes(self):
        arc_renderer = self.handles['quad_1_glyph_renderer']
        scatter_renderer = self.handles['scatter_1_glyph_renderer']
        for gtype in ('selection_', 'nonselection_', 'muted_', 'hover_', ''):
            glyph = getattr(scatter_renderer, gtype+'glyph')
            arc_glyph = getattr(arc_renderer, gtype+'glyph')
            if not glyph or not arc_glyph:
                continue
            scatter_props = glyph.properties_with_values(include_defaults=False)
            styles = {k: v for k, v in scatter_props.items()
                      if k in arc_glyph.properties()}
            arc_glyph.update(**styles)

    def _compute_quads(self, element, data, mapping):
        """Computes the node quad glyph data.x

        """
        quad_mapping = {'left': 'x0', 'right': 'x1', 'bottom': 'y0', 'top': 'y1'}
        quad_data = dict(data['scatter_1'])
        quad_data.update({'x0': [], 'x1': [], 'y0': [], 'y1': []})
        for node in element._sankey['nodes']:
            quad_data['x0'].append(node['x0'])
            quad_data['y0'].append(node['y0'])
            quad_data['x1'].append(node['x1'])
            quad_data['y1'].append(node['y1'])
            data['scatter_1'].update(quad_data)
        data['quad_1'] = data['scatter_1']
        mapping['quad_1'] = quad_mapping

    def _compute_labels(self, element, data, mapping):
        """Computes labels for the nodes and adds it to the data.

        """
        if element.vdims:
            edges = Dataset(element)[element[element.vdims[0].name]>0]
            nodes = list(np.unique([edges.dimension_values(i) for i in range(2)]))
            nodes = element.nodes.select(**{element.nodes.kdims[2].name: nodes})
        else:
            nodes = element

        label_dim = nodes.get_dimension(self.label_index)
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

        ys = nodes.dimension_values(1)

        nodes = element._sankey['nodes']
        if nodes:
            offset = (nodes[0]['x1']-nodes[0]['x0'])/4.
        else:
            offset = 0

        value_dim = element.vdims[0]
        text_labels = []

        is_outer_inner = self.label_position in ['outer', 'inner']

        # initialize label x-locations
        if self.label_position in ['right', 'outer']:
            xs = np.array([node['x1'] for node in nodes]) + offset
        else: # ['left', 'inner']
            xs = np.array([node['x0'] for node in nodes]) - offset

        for i, node in enumerate(nodes):
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
                text_labels.append(label)

        align = 'left' if self.label_position in ['right', 'outer'] else 'right'

        # handle labels that have different alignment due to inner/outer
        if is_outer_inner:
            xs_2 = xs.copy()
            text_labels_2 = text_labels.copy()
            for i, node in enumerate(nodes):
                if self.label_position == 'outer' and node['x0'] == 0:
                    text_labels[i] = ''
                    xs_2[i] = node['x0'] - offset
                elif self.label_position == 'inner' and node['x0'] == 0:
                    text_labels[i] = ''
                    xs_2[i] = node['x1'] + offset
                else:
                    text_labels_2[i] = ''

            align_2 = 'right' if align == 'left' else 'left'
            data['text_2'] = dict(x=xs_2, y=ys,
                                  text=[str(l) for l in text_labels_2])
            mapping['text_2'] = dict(text='text', x='x', y='y',
                                     text_baseline='middle', text_align=align_2)

        data['text_1'] = dict(x=xs, y=ys, text=[str(l) for l in text_labels])
        mapping['text_1'] = dict(text='text', x='x', y='y',
                                 text_baseline='middle', text_align=align)

    def _patch_hover(self, element, data):
        """Replace edge start and end hover data with label_index data.

        """
        if not (self.inspection_policy == 'edges' and 'hover' in self.handles):
            return
        lidx = element.nodes.get_dimension(self.label_index)
        src, tgt = (dimension_sanitizer(kd.name) for kd in element.kdims[:2])
        if src == 'start': src += '_values'
        if tgt == 'end':   tgt += '_values'
        lookup = dict(zip(*(element.nodes.dimension_values(d) for d in (2, lidx)), strict=None))
        src_vals = data['patches_1'][src]
        tgt_vals = data['patches_1'][tgt]
        data['patches_1'][src] = [lookup.get(v, v) for v in src_vals]
        data['patches_1'][tgt] = [lookup.get(v, v) for v in tgt_vals]

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        """Return the extents of the Sankey box

        """
        if range_type == 'extents':
            return element.nodes.extents
        xdim, ydim = element.nodes.kdims[:2]
        xpad = .05 if self.label_index is None else 0.25
        x0, x1 = ranges[xdim.label][range_type]
        y0, y1 = ranges[ydim.label][range_type]
        xdiff = (x1-x0)
        ydiff = (y1-y0)

        if self.label_position in ['right', 'outer']:
            x1 = x1 + xpad * xdiff
        else: # ['left', 'inner']
            x1 = x1 + (0.05 * xdiff)

        if self.label_position in ['left', 'outer']:
            x0 = x0 - xpad * xdiff
        else: # ['right', 'inner']
            x0 = x0 - (0.05 * xdiff)

        x0, x1 = max_range([xdim.range, (x0, x1)])
        y0, y1 = max_range([ydim.range, (y0-(0.05*ydiff), y1+(0.05*ydiff))])

        return (x0, y0, x1, y1)

    def _postprocess_hover(self, renderer, source):
        if self.inspection_policy == 'edges':
            if not isinstance(renderer.glyph, Patches):
                return
        elif isinstance(renderer.glyph, Patches):
            return
        super()._postprocess_hover(renderer, source)
