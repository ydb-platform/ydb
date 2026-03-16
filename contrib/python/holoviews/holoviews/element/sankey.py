import math
from functools import cmp_to_key
from itertools import cycle

import numpy as np
import param

from ..core.data import Dataset
from ..core.dimension import Dimension
from ..core.operation import Operation
from ..core.util import get_param_values, unique_array
from .graphs import EdgePaths, Graph, Nodes
from .util import quadratic_bezier

_Y_N_DECIMAL_DIGITS = 6
_Y_EPS = 10 ** -_Y_N_DECIMAL_DIGITS


class _layout_sankey(Operation):
    """Computes a Sankey diagram from a Graph element for internal use in
    the Sankey element constructor.

    Adapted from d3-sankey under BSD-3 license.

    Source : https://github.com/d3/d3-sankey/tree/v0.12.3

    """

    bounds = param.NumericTuple(default=(0, 0, 1000, 500))

    node_width = param.Number(default=15, doc="""
        Width of the nodes.""")

    node_padding = param.Integer(default=None, allow_None=True, doc="""
        Number of pixels of padding relative to the bounds.""")

    iterations = param.Integer(default=32, doc="""
        Number of iterations to run the layout algorithm.""")

    node_sort = param.Boolean(default=True, doc="""
        Sort nodes in ascending breadth.""")

    def _process(self, element, key=None):
        nodes, edges, graph = self.layout(element, **self.p)
        params = get_param_values(element)
        return Sankey((element.data, nodes, edges), sankey=graph, **params)

    def layout(self, element, **params):
        self.p = param.ParamOverrides(self, params)
        graph = {'nodes': [], 'links': []}
        self.computeNodeLinks(element, graph)
        self.computeNodeValues(graph)
        self.computeNodeDepths(graph)
        self.computeNodeHeights(graph)
        self.computeNodeBreadths(graph)
        self.computeLinkBreadths(graph)
        paths = self.computePaths(graph)

        node_data = []
        for node in graph['nodes']:
            node_data.append((
                np.mean([node['x0'], node['x1']]),
                np.mean([node['y0'], node['y1']]),
                node['index'],
                *node['values']
            ))
        if element.nodes.ndims == 3:
            kdims = element.nodes.kdims
        elif element.nodes.ndims:
            kdims = element.node_type.kdims[:2] + element.nodes.kdims[-1:]
        else:
            kdims = element.node_type.kdims
        nodes = element.node_type(node_data, kdims=kdims, vdims=element.nodes.vdims)
        edges = element.edge_type(paths)
        return nodes, edges, graph

    @classmethod
    def computeNodeLinks(cls, element, graph):
        """Populate the sourceLinks and targetLinks for each node.
        Also, if the source and target are not objects, assume they are indices.

        """
        index = element.nodes.kdims[-1]
        node_map = {}
        if element.nodes.vdims:
            values = zip(*(element.nodes.dimension_values(d)
                           for d in element.nodes.vdims), strict=None)
        else:
            values = cycle([tuple()])
        for idx, vals in zip(element.nodes.dimension_values(index), values, strict=None):
            node = {'index': idx, 'sourceLinks': [], 'targetLinks': [], 'values': vals}
            graph['nodes'].append(node)
            node_map[idx] = node

        links = [element.dimension_values(d) for d in element.dimensions()[:3]]
        for i, (src, tgt, value) in enumerate(zip(*links, strict=None)):
            source, target = node_map[src], node_map[tgt]
            link = dict(index=i, source=source, target=target, value=value)
            graph['links'].append(link)
            source['sourceLinks'].append(link)
            target['targetLinks'].append(link)

    @classmethod
    def computeNodeValues(cls, graph):
        """Compute the value (size) of each node by summing the associated links.

        """
        for node in graph['nodes']:
            source_val = np.sum([l['value'] for l in node['sourceLinks']])
            target_val = np.sum([l['value'] for l in node['targetLinks']])
            node['value'] = max([source_val, target_val])

    @classmethod
    def computeNodeDepths(cls, graph):
        nodes = graph['nodes']
        depth = 0
        while nodes:
            next_nodes = []
            for node in nodes:
                node['depth'] = depth
                for link in node['sourceLinks']:
                    next_nodes.append(link['target'])
            nodes = next_nodes
            depth += 1
            if depth > len(graph['nodes']):
                raise RecursionError('Sankey diagrams only support acyclic graphs.')
        return depth

    @classmethod
    def computeNodeHeights(cls, graph):
        nodes = graph['nodes']
        height = 0
        while nodes:
            next_nodes = []
            for node in nodes:
                node['height'] = height
                for link in node['targetLinks']:
                    next_nodes.append(link['source'])
            nodes = next_nodes
            height += 1
            if height > len(graph['nodes']):
                raise RecursionError('Sankey diagrams only support acyclic graphs.')
        return height

    def computeNodeColumns(self, graph):
        depth_upper_bound = max(x['depth'] for x in graph['nodes']) + 1
        x0, x1 = self.p.bounds[0], self.p.bounds[2]
        dx = self.p.node_width
        kx = (x1 - x0 - dx) / (depth_upper_bound - 1)
        columns = [[] for _ in range(depth_upper_bound)]
        for node in graph['nodes']:
            node['column'] = max(
                0,
                min(
                    depth_upper_bound - 1,
                    math.floor(
                        node['depth']
                        if node['sourceLinks']
                        else depth_upper_bound - 1
                    )
                )
            )
            node['x0'] = x0 + node['column'] * kx
            node['x1'] = node['x0'] + dx
            columns[node['column']].append(node)
        return columns

    @classmethod
    def ascendingBreadth(cls, a, b):
        return int(a['y0'] - b['y0'])

    @classmethod
    def ascendingSourceBreadth(cls, a, b):
        return (
            (
                cls.ascendingBreadth(a['source'], b['source'])
                if 'y0' in a['source'] and 'y0' in b['source']
                else None
            )
            or a['index'] - b['index']
        )

    @classmethod
    def ascendingTargetBreadth(cls, a, b):
        return (
            (
                cls.ascendingBreadth(a['target'], b['target'])
                if 'y0' in a['target'] and 'y0' in b['target']
                else None
            )
            or a['index'] - b['index']
        )

    @classmethod
    def reorderLinks(cls, nodes):
        for x in nodes:
            x['sourceLinks'].sort(key=cmp_to_key(cls.ascendingTargetBreadth))
            x['targetLinks'].sort(key=cmp_to_key(cls.ascendingSourceBreadth))

    def initializeNodeBreadths(self, columns, py):
        _, y0, _, y1 = self.p.bounds
        ky = min(
            (y1 - y0 - (len(c) - 1) * py) / sum(node['value'] for node in c)
            for c in columns
        )
        for nodes in columns:
            y = y0
            for node in nodes:
                node['y0'] = y
                node['y1'] = y + node['value'] * ky
                y = node['y1'] + py
                for link in node['sourceLinks']:
                    link['width'] = link['value'] * ky
            y = (y1 - y + py) / (len(nodes) + 1)
            for i, node in enumerate(nodes):
                node['y0'] += y * (i + 1)
                node['y1'] += y * (i + 1)
            self.reorderLinks(nodes)

    @classmethod
    def sourceTop(cls, source, target, py):
        y = target['y0'] - (len(target['targetLinks']) - 1) * py / 2
        for link in target['targetLinks']:
            if link['source'] is source:
                break
            y += link['width'] + py
        for link in source['sourceLinks']:
            if link['target'] is target:
                break
            y -= link['width']
        return y

    @classmethod
    def targetTop(cls, source, target, py):
        y = source['y0'] - (len(source['sourceLinks']) - 1) * py / 2
        for link in source['sourceLinks']:
            if link['target'] is target:
                break
            y += link['width'] + py
        for link in target['targetLinks']:
            if link['source'] is source:
                break
            y -= link['width']
        return y

    @classmethod
    def resolveCollisionsTopToBottom(cls, nodes, y, i, alpha, py):
        for node in nodes[i:]:
            dy = (y - node['y0']) * alpha
            if dy > _Y_EPS:
                node['y0'] += dy
                node['y1'] += dy
            y = node['y1'] + py

    @classmethod
    def resolveCollisionsBottomToTop(cls, nodes, y, i, alpha, py):
        # NOTE: don't change the `while` loop to `for`
        while i >= 0:
            node = nodes[i]
            dy = (node['y1'] - y) * alpha
            if dy > _Y_EPS:
                node['y0'] -= dy
                node['y1'] -= dy
            y = node['y0'] - py
            i -= 1

    def resolveCollisions(self, nodes, alpha, py):
        _, y0, _, y1 = self.p.bounds
        i = len(nodes) // 2
        subject = nodes[i]
        self.resolveCollisionsBottomToTop(nodes, subject['y0'] - py, i - 1, alpha, py)
        self.resolveCollisionsTopToBottom(nodes, subject['y1'] + py, i + 1, alpha, py)
        self.resolveCollisionsBottomToTop(nodes, y1, len(nodes) - 1, alpha, py)
        self.resolveCollisionsTopToBottom(nodes, y0, 0, alpha, py)

    @classmethod
    def reorderNodeLinks(cls, node):
        for link in node['targetLinks']:
            link['source']['sourceLinks'].sort(
                key=cmp_to_key(cls.ascendingTargetBreadth)
            )
        for link in node['sourceLinks']:
            link['target']['targetLinks'].sort(
                key=cmp_to_key(cls.ascendingSourceBreadth)
            )

    def relaxLeftToRight(self, columns, alpha, beta, py):
        for column in columns[1:]:
            for target in column:
                y = 0
                w = 0
                for link in target['targetLinks']:
                    source = link['source']
                    v = link['value'] * (target['column'] - source['column'])
                    y += self.targetTop(source, target, py) * v
                    w += v
                if w <= 0:
                    continue
                dy = (y / w - target['y0']) * alpha
                target['y0'] += dy
                target['y1'] += dy
                self.reorderNodeLinks(target)
            if self.p.node_sort:
                # TODO is the comparison operator valid?
                column.sort(key=cmp_to_key(self.ascendingBreadth))
            self.resolveCollisions(column, beta, py)

    def relaxRightToLeft(self, columns, alpha, beta, py):
        """Reposition each node based on its outgoing (source) links.

        """
        for column in columns[-2::-1]:
            for source in column:
                y = 0
                w = 0
                for link in source['sourceLinks']:
                    target = link['target']
                    v = link['value'] * (target['column'] - source['column'])
                    y += self.sourceTop(source, target, py) * v
                    w += v
                if w <= 0:
                    continue
                dy = (y / w - source['y0']) * alpha
                source['y0'] += dy
                source['y1'] += dy
                self.reorderNodeLinks(source)
            if self.p.node_sort:
                column.sort(key=cmp_to_key(self.ascendingBreadth))
            self.resolveCollisions(column, beta, py)

    def computeNodeBreadths(self, graph):
        columns = self.computeNodeColumns(graph)
        _, y0, _, y1 = self.p.bounds
        max_column_size = max(map(len, columns))
        # NOTE: the `max_default_padding` thing is a holoviews-specific hack
        max_default_padding = 20
        py = (
            self.p.node_padding
            if self.p.node_padding is not None
            else min((y1 - y0) / (max_column_size - 1), max_default_padding)
            if max_column_size > 1
            else max_default_padding
        )
        self.initializeNodeBreadths(columns, py)
        for i in range(self.p.iterations):
            alpha = 0.99 ** i
            beta = max(1 - alpha, (i + 1) / self.p.iterations)
            self.relaxRightToLeft(columns, alpha, beta, py)
            self.relaxLeftToRight(columns, alpha, beta, py)
        for node in graph['nodes']:
            node['y1'] = round(node['y1'], _Y_N_DECIMAL_DIGITS)

    @classmethod
    def computeLinkBreadths(cls, graph):
        for node in graph['nodes']:
            node['sourceLinks'].sort(key=cmp_to_key(cls.ascendingTargetBreadth))
            node['targetLinks'].sort(key=cmp_to_key(cls.ascendingSourceBreadth))

        for node in graph['nodes']:
            y0 = node['y0']
            y1 = y0
            for link in node['sourceLinks']:
                link['y0'] = y0 + link['width'] / 2
                y0 += link['width']
            for link in node['targetLinks']:
                link['y1'] = y1 + link['width'] / 2
                y1 += link['width']

    def computePaths(self, graph):
        paths = []
        for link in graph['links']:
            source, target = link['source'], link['target']
            x0 = source['x1']
            x1 = target['x0']
            xmid = (x0 + x1) / 2
            y0_upper = link['y0'] + link['width'] / 2
            y0_lower = link['y0'] - link['width'] / 2
            y1_upper = link['y1'] + link['width'] / 2
            y1_lower = link['y1'] - link['width'] / 2

            start = np.array([
                [x0, y0_upper],
                [x0, y0_lower],
            ])
            bottom = quadratic_bezier(
                (x0, y0_lower),
                (x1, y1_lower),
                (xmid, y0_lower),
                (xmid, y1_lower),
            )
            mid = np.array([
                [x1, y1_lower],
                [x1, y1_upper],
            ])
            top = quadratic_bezier(
                (x1, y1_upper),
                (x0, y0_upper),
                (xmid, y1_upper),
                (xmid, y0_upper),
            )
            spline = np.concatenate([start, bottom, mid, top])
            paths.append(spline)
        return paths


class Sankey(Graph):
    """Sankey is an acyclic, directed Graph type that represents the flow
    of some quantity between its nodes.

    """

    group = param.String(default='Sankey', constant=True)

    vdims = param.List(default=[Dimension('Value')])

    def __init__(self, data, kdims=None, vdims=None, **params):
        if data is None:
            data = []
        if isinstance(data, tuple):
            data = data + (None,)*(3-len(data))
            edges, nodes, edgepaths = data
        else:
            edges, nodes, edgepaths = data, None, None
        sankey_graph = params.pop('sankey', None)
        compute = not (sankey_graph and isinstance(nodes, Nodes) and isinstance(edgepaths, EdgePaths))
        super(Graph, self).__init__(edges, kdims=kdims, vdims=vdims, **params)
        if compute:
            if nodes is None:
                src = self.dimension_values(0, expanded=False)
                tgt = self.dimension_values(1, expanded=False)
                values = unique_array(np.concatenate([src, tgt]))
                nodes = Dataset(values, 'index')
            elif not isinstance(nodes, Dataset):
                try:
                    nodes = Dataset(nodes)
                except Exception:
                    nodes = Dataset(nodes, 'index')
            if not nodes.kdims:
                raise ValueError('Could not determine index in supplied node data. '
                                 'Ensure data has at least one key dimension, '
                                 'which matches the node ids on the edges.')
            self._nodes = nodes
            nodes, edgepaths, graph = _layout_sankey.instance().layout(self)
            self._nodes = nodes
            self._edgepaths = edgepaths
            self._sankey = graph
        else:
            if not isinstance(nodes, self.node_type):
                raise TypeError(f"Expected Nodes object in data, found {type(nodes)}.")
            self._nodes = nodes
            if not isinstance(edgepaths, self.edge_type):
                raise TypeError(f"Expected EdgePaths object in data, found {type(edgepaths)}.")
            self._edgepaths = edgepaths
            self._sankey = sankey_graph
        self._validate()

    def clone(self, data=None, shared_data=True, new_type=None, link=True,
              *args, **overrides):
        if data is None:
            overrides['sankey'] = self._sankey
        return super().clone(data, shared_data, new_type, link,
                             *args, **overrides)
