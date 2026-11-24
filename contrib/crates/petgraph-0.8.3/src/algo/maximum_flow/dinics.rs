use alloc::{collections::VecDeque, vec, vec::Vec};
use core::ops::Sub;

use crate::{
    algo::{EdgeRef, PositiveMeasure},
    prelude::Direction,
    visit::{
        Data, EdgeCount, EdgeIndexable, IntoEdgeReferences, IntoEdges, IntoEdgesDirected,
        NodeCount, NodeIndexable, VisitMap, Visitable,
    },
};

/// Compute the maximum flow from `source` to `destination` in a directed graph.
/// Implements [Dinic's (or Dinitz's) algorithm][dinics], which builds successive
/// level graphs using breadth-first search and finds blocking flows within
/// them through depth-first searches.
///
/// For simplicity, the algorithm requires `N::EdgeWeight` to implement
/// only [PartialOrd] trait, and not [Ord], but will panic if it tries to
/// compare two elements that aren't comparable (i.e., given two edge weights `a`
/// and `b`, where neither `a >= b` nor `a < b`).
///
/// See also [`maximum_flow`][max flow mod] module for other maximum flow algorithms.
///
/// # Arguments
/// * `network` — A directed graph with positive edge weights, namely "flow capacities".
/// * `source` — The source node where flow originates.
/// * `destination` — The destination node where flow terminates.
///
/// # Returns
/// Returns a tuple of two values:
/// * `N::EdgeWeight`: computed maximum flow;
/// * `Vec<N::EdgeWeight>`: the flow of each edge. The vector is indexed by the graph's edge indices.
///
/// # Complexity
/// * Time complexity:
///   * In general: **O(|V|²|E|)**
///   * In networks with only unit capacities: **O(min{|V|²ᐟ³, |E|¹ᐟ²} |E|)**
/// * Auxiliary space: **O(|V| + |E|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
/// [dinics]: https://en.wikipedia.org/wiki/Dinic%27s_algorithm
/// [max flow mod]: index.html
///
/// # Example
/// ```rust
/// use petgraph::Graph;
/// use petgraph::algo::dinics;
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
/// let (max_flow, _) = dinics(&graph, source, destination);
/// assert_eq!(23, max_flow);
/// ```
pub fn dinics<G>(
    network: G,
    source: G::NodeId,
    destination: G::NodeId,
) -> (G::EdgeWeight, Vec<G::EdgeWeight>)
where
    G: NodeCount + EdgeCount + IntoEdgesDirected + EdgeIndexable + NodeIndexable + Visitable,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    let mut max_flow = G::EdgeWeight::zero();
    let mut flows = vec![G::EdgeWeight::zero(); network.edge_count()];
    let mut visited = network.visit_map();
    let mut level_edges = vec![Default::default(); network.node_bound()];

    let dest_index = NodeIndexable::to_index(&network, destination);
    while build_level_graph(&network, source, destination, &flows, &mut level_edges)[dest_index] > 0
    {
        let flow_increase = find_blocking_flow(
            network,
            source,
            destination,
            &mut flows,
            &mut level_edges,
            &mut visited,
        );
        max_flow = max_flow + flow_increase;
    }
    (max_flow, flows)
}

/// Makes a BFS that labels network vertices with levels representing
/// their distance to the source vertex, considering only edges with
/// positive residual capacity.
///
/// The source vertex is labeled as 1, and vertices not reachable are
/// labeled as 0.
///
/// Aggregates in `level_edges` the edges that connects each
/// vertex to its neighbours in the next level.
///
/// Returns the computed level graph.
fn build_level_graph<G>(
    network: G,
    source: G::NodeId,
    destination: G::NodeId,
    flows: &[G::EdgeWeight],
    level_edges: &mut [Vec<G::EdgeRef>],
) -> Vec<usize>
where
    G: NodeCount + IntoEdgesDirected + NodeIndexable + EdgeIndexable,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    let mut level_graph = vec![0; network.node_bound()];
    let mut bfs_queue = VecDeque::with_capacity(network.node_count());
    bfs_queue.push_back(source);

    level_graph[NodeIndexable::to_index(&network, source)] = 1;
    while let Some(vertex) = bfs_queue.pop_front() {
        let vertex_index = NodeIndexable::to_index(&network, vertex);
        let out_edges = network.edges_directed(vertex, Direction::Outgoing);
        let in_edges = network.edges_directed(vertex, Direction::Incoming);
        level_edges[vertex_index].clear();
        for edge in out_edges.chain(in_edges) {
            let next_vertex = other_endpoint(&network, edge, vertex);
            let edge_index = EdgeIndexable::to_index(&network, edge.id());
            let residual_cap = residual_capacity(&network, edge, next_vertex, flows[edge_index]);
            if residual_cap == G::EdgeWeight::zero() {
                continue;
            }
            let next_vertex_index = NodeIndexable::to_index(&network, next_vertex);
            if level_graph[next_vertex_index] == 0 {
                level_graph[next_vertex_index] = level_graph[vertex_index] + 1;
                level_edges[vertex_index].push(edge);
                if next_vertex != destination {
                    bfs_queue.push_back(next_vertex);
                }
            } else if level_graph[next_vertex_index] == level_graph[vertex_index] + 1 {
                level_edges[vertex_index].push(edge);
            }
        }
    }

    level_graph
}

/// Find blocking flow for current level graph by repeatingly finding
/// augmenting paths in it.
///
/// Attach computed flows to `flows` and returns the total flow increase from
/// edges available in `level_edges` at this iteration.
fn find_blocking_flow<G>(
    network: G,
    source: G::NodeId,
    destination: G::NodeId,
    flows: &mut [G::EdgeWeight],
    level_edges: &mut [Vec<G::EdgeRef>],
    visited: &mut G::Map,
) -> G::EdgeWeight
where
    G: NodeCount + IntoEdges + NodeIndexable + EdgeIndexable + Visitable,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    let mut flow_increase = G::EdgeWeight::zero();
    let mut edge_to = vec![None; network.node_bound()];
    while find_augmenting_path(
        &network,
        source,
        destination,
        flows,
        level_edges,
        visited,
        &mut edge_to,
    ) {
        let mut path_flow = G::EdgeWeight::max();

        // Find the bottleneck capacity of the path
        let mut vertex = destination;
        while let Some(edge) = edge_to[NodeIndexable::to_index(&network, vertex)] {
            let edge_index = EdgeIndexable::to_index(&network, edge.id());
            let residual_capacity = residual_capacity(&network, edge, vertex, flows[edge_index]);
            path_flow = min::<G>(path_flow, residual_capacity);
            vertex = other_endpoint(&network, edge, vertex);
        }

        // Update the flow of each edge along the discovered path
        let mut vertex = destination;
        while let Some(edge) = edge_to[NodeIndexable::to_index(&network, vertex)] {
            let edge_index = EdgeIndexable::to_index(&network, edge.id());
            flows[edge_index] =
                adjusted_residual_flow(&network, edge, vertex, flows[edge_index], path_flow);
            vertex = other_endpoint(&network, edge, vertex);
        }
        flow_increase = flow_increase + path_flow;
    }
    flow_increase
}

/// Makes a DFS to find an augmenting path from source to destination vertex
/// using previously computed `edge_levels` from level graph.
///
/// Returns a boolean indicating if an augmenting path to destination was found.
fn find_augmenting_path<G>(
    network: G,
    source: G::NodeId,
    destination: G::NodeId,
    flows: &[G::EdgeWeight],
    level_edges: &mut [Vec<G::EdgeRef>],
    visited: &mut G::Map,
    edge_to: &mut [Option<G::EdgeRef>],
) -> bool
where
    G: IntoEdges + NodeIndexable + EdgeIndexable + Visitable,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    network.reset_map(visited);
    let mut level_edges_i = vec![0; level_edges.len()];

    let mut dfs_stack = Vec::new();
    dfs_stack.push(source);
    visited.visit(source);
    while let Some(&vertex) = dfs_stack.last() {
        let vertex_index = NodeIndexable::to_index(&network, vertex);

        let mut found_next = false;
        while level_edges_i[vertex_index] < level_edges[vertex_index].len() {
            let curr_level_edges_i = level_edges_i[vertex_index];
            let edge = level_edges[vertex_index][curr_level_edges_i];
            let next_vertex = other_endpoint(&network, edge, vertex);

            let edge_index: usize = EdgeIndexable::to_index(&network, edge.id());
            let residual_cap = residual_capacity(&network, edge, next_vertex, flows[edge_index]);
            if residual_cap == G::EdgeWeight::zero() {
                level_edges[vertex_index].swap_remove(curr_level_edges_i);
                continue;
            }

            if !visited.is_visited(&next_vertex) {
                let next_vertex_index = NodeIndexable::to_index(&network, next_vertex);
                edge_to[next_vertex_index] = Some(edge);
                if destination == next_vertex {
                    return true;
                }
                dfs_stack.push(next_vertex);
                visited.visit(next_vertex);
                found_next = true;
                break;
            }
            level_edges_i[vertex_index] += 1;
        }
        if !found_next {
            dfs_stack.pop();
        }
    }
    false
}

/// Returns the adjusted residual flow for given edge and flow increase.
fn adjusted_residual_flow<G>(
    network: G,
    edge: G::EdgeRef,
    target_vertex: G::NodeId,
    flow: G::EdgeWeight,
    flow_increase: G::EdgeWeight,
) -> G::EdgeWeight
where
    G: NodeIndexable + IntoEdges,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    if target_vertex == edge.source() {
        // backward edge
        flow - flow_increase
    } else if target_vertex == edge.target() {
        // forward edge
        flow + flow_increase
    } else {
        let end_point = NodeIndexable::to_index(&network, target_vertex);
        panic!("Illegal endpoint {}", end_point);
    }
}

/// Returns the residual capacity of given edge.
fn residual_capacity<G>(
    network: G,
    edge: G::EdgeRef,
    target_vertex: G::NodeId,
    flow: G::EdgeWeight,
) -> G::EdgeWeight
where
    G: NodeIndexable + IntoEdges,
    G::EdgeWeight: Sub<Output = G::EdgeWeight> + PositiveMeasure,
{
    if target_vertex == edge.source() {
        // backward edge
        flow
    } else if target_vertex == edge.target() {
        // forward edge
        *edge.weight() - flow
    } else {
        let end_point = NodeIndexable::to_index(&network, target_vertex);
        panic!("Illegal endpoint {}", end_point);
    }
}

/// Returns the minimum value between given `a` and `b`.
/// Will panic if it tries to compare two elements that aren't comparable
/// (i.e., given two elements `a` and `b`, neither `a >= b` nor `a < b`).
fn min<G>(a: G::EdgeWeight, b: G::EdgeWeight) -> G::EdgeWeight
where
    G: Data,
    G::EdgeWeight: PartialOrd,
{
    if a < b {
        a
    } else if a >= b {
        b
    } else {
        panic!("Invalid edge weights. Impossible to get min value.");
    }
}

/// Gets the other endpoint of graph edge, if any, otherwise panics.
fn other_endpoint<G>(network: G, edge: G::EdgeRef, vertex: G::NodeId) -> G::NodeId
where
    G: NodeIndexable + IntoEdgeReferences,
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
