import param
from datashader.bundling import (
    directly_connect_edges as connect_edges,
    hammer_bundle,
)

from ..core import Operation
from .datashader import split_dataframe


class _connect_edges(Operation):

    split = param.Boolean(default=False, doc="""
        Determines whether bundled edges will be split into individual edges
        or concatenated with NaN separators.""")

    def _bundle(self, position_df, edges_df):
        raise NotImplementedError('_connect_edges is an abstract baseclass '
                                  'and does not implement any actual bundling.')

    def _process(self, element, key=None):
        index = element.nodes.kdims[2].name
        rename_edges = {d.name: v for d, v in zip(element.kdims[:2], ['source', 'target'], strict=True)}
        rename_nodes = {d.name: v for d, v in zip(element.nodes.kdims[:2], ['x', 'y'], strict=True)}
        position_df = element.nodes.redim(**rename_nodes).dframe([0, 1, 2]).set_index(index)
        edges_df = element.redim(**rename_edges).dframe([0, 1])
        paths = self._bundle(position_df, edges_df)
        paths = paths.rename(columns={v: k for k, v in rename_nodes.items()})
        paths = split_dataframe(paths) if self.p.split else [paths]
        return element.clone((element.data, element.nodes, paths))


class bundle_graph(_connect_edges, hammer_bundle):
    """Iteratively group edges and return as paths suitable for datashading.

    Breaks each edge into a path with multiple line segments, and
    iteratively curves this path to bundle edges into groups.

    """

    def _bundle(self, position_df, edges_df):
        from datashader.bundling import hammer_bundle
        return hammer_bundle.__call__(self, position_df, edges_df, **self.p)


class directly_connect_edges(_connect_edges, connect_edges):
    """Given a Graph object will directly connect all nodes.

    """

    def _bundle(self, position_df, edges_df):
        return connect_edges.__call__(self, position_df, edges_df)
