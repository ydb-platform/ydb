use alloc::{collections::VecDeque, vec, vec::Vec};
use core::ops::Sub;

use crate::{
    algo::PositiveMeasure,
    data::DataMap,
    prelude::Direction,
    visit::{
        EdgeCount, EdgeIndexable, EdgeRef, IntoEdges, IntoEdgesDirected, NodeCount, NodeIndexable,
        VisitMap, Visitable,
    },
};

fn residual_capacity<G>(
    network: G,
    edge: G::EdgeRef,
    vertex: G::NodeId,
    flow: G::EdgeWeight,
) -> G::EdgeWeight
where
    G: NodeIndexable + IntoEdges,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    if vertex == edge.source() {
        // backward edge
        flow
    } else if vertex == edge.target() {
        // forward edge
        *edge.weight() - flow
    } else {
        let end_point = NodeIndexable::to_index(&network, vertex);
        panic!("Illegal endpoint {}", end_point);
    }
}

/// Gets the other endpoint of graph edge, if any, otherwise panics.
fn other_endpoint<G>(network: G, edge: G::EdgeRef, vertex: G::NodeId) -> G::NodeId
where
    G: NodeIndexable + IntoEdges,
{
    if vertex == edge.source() {
        edge.target()
    } else if vertex == edge.target() {
        edge.source()
    } else {
        let end_point = NodeIndexable::to_index(&network, vertex);
        panic!("Illegal endpoint {}", end_point);
    }
}

/// Tells whether there is an augmented path in the graph
fn has_augmented_path<G>(
    network: G,
    source: G::NodeId,
    destination: G::NodeId,
    edge_to: &mut [Option<G::EdgeRef>],
    flows: &[G::EdgeWeight],
) -> bool
where
    G: NodeCount + IntoEdgesDirected + NodeIndexable + EdgeIndexable + Visitable,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    let mut visited = network.visit_map();
    let mut queue = VecDeque::new();
    visited.visit(source);
    queue.push_back(source);

    while let Some(vertex) = queue.pop_front() {
        let out_edges = network.edges_directed(vertex, Direction::Outgoing);
        let in_edges = network.edges_directed(vertex, Direction::Incoming);
        for edge in out_edges.chain(in_edges) {
            let next = other_endpoint(&network, edge, vertex);
            let edge_index: usize = EdgeIndexable::to_index(&network, edge.id());
            let residual_cap = residual_capacity(&network, edge, next, flows[edge_index]);
            if !visited.is_visited(&next) && (residual_cap > G::EdgeWeight::zero()) {
                visited.visit(next);
                edge_to[NodeIndexable::to_index(&network, next)] = Some(edge);
                if destination == next {
                    return true;
                }
                queue.push_back(next);
            }
        }
    }
    false
}

fn adjust_residual_flow<G>(
    network: G,
    edge: G::EdgeRef,
    vertex: G::NodeId,
    flow: G::EdgeWeight,
    delta: G::EdgeWeight,
) -> G::EdgeWeight
where
    G: NodeIndexable + IntoEdges,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    if vertex == edge.source() {
        // backward edge
        flow - delta
    } else if vertex == edge.target() {
        // forward edge
        flow + delta
    } else {
        let end_point = NodeIndexable::to_index(&network, vertex);
        panic!("Illegal endpoint {}", end_point);
    }
}

/// [Ford-Fulkerson][ff] algorithm in the [Edmonds-Karp][ek] variation.
/// Computes the [maximum flow] from `source` to `destination` in a weighted directed graph.
///
/// See also [`maximum_flow`][max flow mod] module for other maximum flow algorithms.
///
/// # Arguments
/// * `network`: a wieghted directed graph.
/// * `source`: a stream *source* node.
/// * `destination`: a stream *sink* node.
///
/// # Returns
/// Returns a tuple of two values:
/// * `N::EdgeWeight`: computed maximum flow;
/// * `Vec<N::EdgeWeight>`: the flow of each edge. The vector is indexed by the graph's edge indices.
///
/// # Complexity
/// * Time complexity: **O(|V||E|²)**.
/// * Auxiliary space: **O(|V| + |E|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
/// [maximum flow]: https://en.wikipedia.org/wiki/Maximum_flow_problem
/// [ff]: https://en.wikipedia.org/wiki/Ford%E2%80%93Fulkerson_algorithm
/// [ek]: https://en.wikipedia.org/wiki/Edmonds%E2%80%93Karp_algorithm
/// [max flow mod]: index.html
///
/// # Example
/// ```rust
/// use petgraph::Graph;
/// use petgraph::algo::ford_fulkerson;
/// // Example from CLRS book
/// let mut graph = Graph::<u8, u8>::new();
/// let source = graph.add_node(0);
/// let _ = graph.add_node(1);
/// let _ = graph.add_node(2);
/// let _ = graph.add_node(3);
/// let _ = graph.add_node(4);
/// let destination = graph.add_node(5);
/// graph.extend_with_edges(&[
///    (0, 1, 16),
///    (0, 2, 13),
///    (1, 2, 10),
///    (1, 3, 12),
///    (2, 1, 4),
///    (2, 4, 14),
///    (3, 2, 9),
///    (3, 5, 20),
///    (4, 3, 7),
///    (4, 5, 4),
/// ]);
/// let (max_flow, _) = ford_fulkerson(&graph, source, destination);
/// assert_eq!(23, max_flow);
/// ```
pub fn ford_fulkerson<G>(
    network: G,
    source: G::NodeId,
    destination: G::NodeId,
) -> (G::EdgeWeight, Vec<G::EdgeWeight>)
where
    G: NodeCount
        + EdgeCount
        + IntoEdgesDirected
        + EdgeIndexable
        + NodeIndexable
        + DataMap
        + Visitable,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    let mut edge_to = vec![None; network.node_count()];
    let mut flows = vec![G::EdgeWeight::zero(); network.edge_bound()];
    let mut max_flow = G::EdgeWeight::zero();
    while has_augmented_path(&network, source, destination, &mut edge_to, &flows) {
        let mut path_flow = G::EdgeWeight::max();

        // Find the bottleneck capacity of the path
        let mut vertex = destination;
        let mut vertex_index = NodeIndexable::to_index(&network, vertex);
        while let Some(edge) = edge_to[vertex_index] {
            let edge_index = EdgeIndexable::to_index(&network, edge.id());
            let residual_capacity = residual_capacity(&network, edge, vertex, flows[edge_index]);
            // Minimum between the current path flow and the residual capacity.
            path_flow = if path_flow > residual_capacity {
                residual_capacity
            } else {
                path_flow
            };
            vertex = other_endpoint(&network, edge, vertex);
            vertex_index = NodeIndexable::to_index(&network, vertex);
        }

        // Update the flow of each edge along the path
        let mut vertex = destination;
        let mut vertex_index = NodeIndexable::to_index(&network, vertex);
        while let Some(edge) = edge_to[vertex_index] {
            let edge_index = EdgeIndexable::to_index(&network, edge.id());
            flows[edge_index] =
                adjust_residual_flow(&network, edge, vertex, flows[edge_index], path_flow);
            vertex = other_endpoint(&network, edge, vertex);
            vertex_index = NodeIndexable::to_index(&network, vertex);
        }
        max_flow = max_flow + path_flow;
    }
    (max_flow, flows)
}
