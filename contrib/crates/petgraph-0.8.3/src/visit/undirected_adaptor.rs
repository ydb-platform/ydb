use crate::visit::{
    Data, EdgeRef, GraphBase, GraphProp, GraphRef, IntoEdgeReferences, IntoEdges,
    IntoEdgesDirected, IntoNeighbors, IntoNeighborsDirected, IntoNodeIdentifiers,
    IntoNodeReferences, NodeCompactIndexable, NodeCount, NodeIndexable, Visitable,
};
use crate::Direction;

/// An edge direction removing graph adaptor.
#[derive(Copy, Clone, Debug)]
pub struct UndirectedAdaptor<G>(pub G);

impl<G: GraphRef> GraphRef for UndirectedAdaptor<G> {}

impl<G> IntoNeighbors for UndirectedAdaptor<G>
where
    G: IntoNeighborsDirected,
{
    type Neighbors = core::iter::Chain<G::NeighborsDirected, G::NeighborsDirected>;
    fn neighbors(self, n: G::NodeId) -> Self::Neighbors {
        self.0
            .neighbors_directed(n, Direction::Incoming)
            .chain(self.0.neighbors_directed(n, Direction::Outgoing))
    }
}

impl<G> IntoEdges for UndirectedAdaptor<G>
where
    G: IntoEdgesDirected,
{
    type Edges = core::iter::Chain<
        MaybeReversedEdges<G::EdgesDirected>,
        MaybeReversedEdges<G::EdgesDirected>,
    >;
    fn edges(self, a: Self::NodeId) -> Self::Edges {
        let incoming = MaybeReversedEdges {
            iter: self.0.edges_directed(a, Direction::Incoming),
            reversed: true,
        };
        let outgoing = MaybeReversedEdges {
            iter: self.0.edges_directed(a, Direction::Outgoing),
            reversed: false,
        };
        incoming.chain(outgoing)
    }
}

impl<G> GraphProp for UndirectedAdaptor<G>
where
    G: GraphBase,
{
    type EdgeType = crate::Undirected;

    fn is_directed(&self) -> bool {
        false
    }
}

/// An edges iterator which may reverse the edge orientation.
#[derive(Debug, Clone)]
pub struct MaybeReversedEdges<I> {
    iter: I,
    reversed: bool,
}

impl<I> Iterator for MaybeReversedEdges<I>
where
    I: Iterator,
    I::Item: EdgeRef,
{
    type Item = MaybeReversedEdgeReference<I::Item>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| MaybeReversedEdgeReference {
            inner: x,
            reversed: self.reversed,
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// An edge reference which may reverse the edge orientation.
#[derive(Copy, Clone, Debug)]
pub struct MaybeReversedEdgeReference<R> {
    inner: R,
    reversed: bool,
}

impl<R> EdgeRef for MaybeReversedEdgeReference<R>
where
    R: EdgeRef,
{
    type NodeId = R::NodeId;
    type EdgeId = R::EdgeId;
    type Weight = R::Weight;
    fn source(&self) -> Self::NodeId {
        if self.reversed {
            self.inner.target()
        } else {
            self.inner.source()
        }
    }
    fn target(&self) -> Self::NodeId {
        if self.reversed {
            self.inner.source()
        } else {
            self.inner.target()
        }
    }
    fn weight(&self) -> &Self::Weight {
        self.inner.weight()
    }
    fn id(&self) -> Self::EdgeId {
        self.inner.id()
    }
}

/// An edges iterator which may reverse the edge orientation.
#[derive(Debug, Clone)]
pub struct MaybeReversedEdgeReferences<I> {
    iter: I,
}

impl<I> Iterator for MaybeReversedEdgeReferences<I>
where
    I: Iterator,
    I::Item: EdgeRef,
{
    type Item = MaybeReversedEdgeReference<I::Item>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| MaybeReversedEdgeReference {
            inner: x,
            reversed: false,
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<G> IntoEdgeReferences for UndirectedAdaptor<G>
where
    G: IntoEdgeReferences,
{
    type EdgeRef = MaybeReversedEdgeReference<G::EdgeRef>;
    type EdgeReferences = MaybeReversedEdgeReferences<G::EdgeReferences>;

    fn edge_references(self) -> Self::EdgeReferences {
        MaybeReversedEdgeReferences {
            iter: self.0.edge_references(),
        }
    }
}

macro_rules! access0 {
    ($e:expr) => {
        $e.0
    };
}

GraphBase! {delegate_impl [[G], G, UndirectedAdaptor<G>, access0]}
Data! {delegate_impl [[G], G, UndirectedAdaptor<G>, access0]}
Visitable! {delegate_impl [[G], G, UndirectedAdaptor<G>, access0]}
NodeIndexable! {delegate_impl [[G], G, UndirectedAdaptor<G>, access0]}
NodeCompactIndexable! {delegate_impl [[G], G, UndirectedAdaptor<G>, access0]}
IntoNodeIdentifiers! {delegate_impl [[G], G, UndirectedAdaptor<G>, access0]}
IntoNodeReferences! {delegate_impl [[G], G, UndirectedAdaptor<G>, access0]}
NodeCount! {delegate_impl [[G], G, UndirectedAdaptor<G>, access0]}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;
    use std::collections::HashSet;

    use super::*;
    use crate::algo::astar::*;
    use crate::graph::{DiGraph, Graph};
    use crate::visit::Dfs;

    static LINEAR_EDGES: [(u32, u32); 5] = [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)];

    #[test]
    pub fn test_is_reachable() {
        // create a linear digraph, choose a node in the centre and check all nodes are visited
        // by a dfs

        let graph = DiGraph::<(), ()>::from_edges(LINEAR_EDGES);

        let mut nodes = graph.node_identifiers().collect::<Vec<_>>();
        nodes.sort();

        let graph = UndirectedAdaptor(&graph);

        use crate::visit::Walker;
        let mut visited_nodes: Vec<_> = Dfs::new(&graph, nodes[2]).iter(&graph).collect();
        visited_nodes.sort();
        assert_eq!(visited_nodes, nodes);
    }

    #[test]
    pub fn test_undirected_adaptor_can_traverse() {
        let graph = DiGraph::<(), ()>::from_edges(LINEAR_EDGES);
        let mut nodes = graph.node_identifiers().collect::<Vec<_>>();
        nodes.sort();
        let ungraph = UndirectedAdaptor(&graph);
        let path = astar(&ungraph, nodes[5], |n| n == nodes[0], |_| 1, |_| 0);

        let true_path = (0..=5).rev().map(|i| nodes[i]).collect::<Vec<_>>();

        let (cost, path) = path.unwrap();
        assert_eq!(cost, 5);
        assert_eq!(path, true_path);
    }

    #[test]
    pub fn test_undirected_edge_refs_point_both_ways() {
        let graph = DiGraph::<(), ()>::from_edges(LINEAR_EDGES);
        let mut nodes = graph.node_identifiers().collect::<Vec<_>>();
        nodes.sort();
        let ungraph = UndirectedAdaptor(&graph);

        let expected_edge_targets = [
            &[nodes[1]][..],
            &[nodes[0], nodes[2]],
            &[nodes[1], nodes[3]],
            &[nodes[2], nodes[4]],
            &[nodes[3], nodes[5]],
            &[nodes[4]],
        ];

        for i in 0..nodes.len() {
            let node = nodes[i];

            let targets = ungraph
                .edges(node)
                .map(|e| e.target())
                .collect::<HashSet<_>>();
            let expected = expected_edge_targets[i]
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            assert_eq!(targets, expected);
        }
    }

    #[test]
    pub fn test_neighbors_count() {
        {
            let graph = Graph::<(), ()>::from_edges(LINEAR_EDGES);
            let graph = UndirectedAdaptor(&graph);

            let mut nodes = graph.node_identifiers().collect::<Vec<_>>();
            nodes.sort();
            assert_eq!(graph.neighbors(nodes[1]).count(), 2);
        }

        {
            let graph = Graph::<(), ()>::from_edges(LINEAR_EDGES);
            let graph = UndirectedAdaptor(&graph);

            let mut nodes = graph.node_identifiers().collect::<Vec<_>>();
            nodes.sort();
            assert_eq!(graph.neighbors(nodes[1]).count(), 2);
        }
    }
}
