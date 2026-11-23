use alloc::{vec, vec::Vec};
use core::hash::Hash;

use hashbrown::HashMap;

use crate::algo::{BoundedMeasure, NegativeCycle};
use crate::visit::{
    EdgeRef, GraphProp, IntoEdgeReferences, IntoNodeIdentifiers, NodeCompactIndexable,
};

#[allow(clippy::type_complexity, clippy::needless_range_loop)]
/// [Floyd–Warshall algorithm](https://en.wikipedia.org/wiki/Floyd%E2%80%93Warshall_algorithm) is an algorithm for all pairs shortest path problem
///
/// Compute the length of each shortest path in a weighted graph with positive or negative edge weights (but with no negative cycles).
///
/// # Arguments
/// * `graph`: graph with no negative cycle.
/// * `edge_cost`: closure that returns cost of a particular edge.
///
/// # Returns
/// * `Ok`: (if graph contains no negative cycle) a [`struct@hashbrown::HashMap`] containing all pairs shortest paths.
/// * `Err`: if graph contains negative cycle.
///
/// # Complexity
/// * Time complexity: **O(|V|³)**.
/// * Auxiliary space: **O(|V|²)**.
///
/// where **|V|** is the number of nodes.
///
/// **Note**: If the graph is sparse (the number of edges is quite small),
/// consider using [`johnson`](fn@crate::algo::johnson)'s algorithm, which is likely to show better performance in such cases.
///
/// # Examples
/// ```rust
/// use petgraph::{prelude::*, Graph, Directed};
/// use petgraph::algo::floyd_warshall;
/// use hashbrown::HashMap;
///
/// let mut graph: Graph<(), (), Directed> = Graph::new();
/// let a = graph.add_node(());
/// let b = graph.add_node(());
/// let c = graph.add_node(());
/// let d = graph.add_node(());
///
/// graph.extend_with_edges(&[
///    (a, b),
///    (a, c),
///    (a, d),
///    (b, c),
///    (b, d),
///    (c, d)
/// ]);
///
/// let weight_map: HashMap<(NodeIndex, NodeIndex), i32> = [
///    ((a, a), 0), ((a, b), 1), ((a, c), 4), ((a, d), 10),
///    ((b, b), 0), ((b, c), 2), ((b, d), 2),
///    ((c, c), 0), ((c, d), 2)
/// ].iter().cloned().collect();
/// //     ----- b --------
/// //    |      ^         | 2
/// //    |    1 |    4    v
/// //  2 |      a ------> c
/// //    |   10 |         | 2
/// //    |      v         v
/// //     --->  d <-------
///
/// let inf = core::i32::MAX;
/// let expected_res: HashMap<(NodeIndex, NodeIndex), i32> = [
///    ((a, a), 0), ((a, b), 1), ((a, c), 3), ((a, d), 3),
///    ((b, a), inf), ((b, b), 0), ((b, c), 2), ((b, d), 2),
///    ((c, a), inf), ((c, b), inf), ((c, c), 0), ((c, d), 2),
///    ((d, a), inf), ((d, b), inf), ((d, c), inf), ((d, d), 0),
/// ].iter().cloned().collect();
///
///
/// let res = floyd_warshall(&graph, |edge| {
///     if let Some(weight) = weight_map.get(&(edge.source(), edge.target())) {
///         *weight
///     } else {
///         inf
///     }
/// }).unwrap();
///
/// let nodes = [a, b, c, d];
/// for node1 in &nodes {
///     for node2 in &nodes {
///         assert_eq!(res.get(&(*node1, *node2)).unwrap(), expected_res.get(&(*node1, *node2)).unwrap());
///     }
/// }
/// ```
pub fn floyd_warshall<G, F, K>(
    graph: G,
    edge_cost: F,
) -> Result<HashMap<(G::NodeId, G::NodeId), K>, NegativeCycle>
where
    G: NodeCompactIndexable + IntoEdgeReferences + IntoNodeIdentifiers + GraphProp,
    G::NodeId: Eq + Hash,
    F: FnMut(G::EdgeRef) -> K,
    K: BoundedMeasure + Copy,
{
    let num_of_nodes = graph.node_count();

    // |V|x|V| matrix
    let mut m_dist = Some(vec![vec![K::max(); num_of_nodes]; num_of_nodes]);

    _floyd_warshall_path(graph, edge_cost, &mut m_dist, &mut None)?;

    let mut distance_map: HashMap<(G::NodeId, G::NodeId), K> =
        HashMap::with_capacity(num_of_nodes * num_of_nodes);

    if let Some(dist) = m_dist {
        for i in 0..num_of_nodes {
            for j in 0..num_of_nodes {
                distance_map.insert((graph.from_index(i), graph.from_index(j)), dist[i][j]);
            }
        }
    }

    Ok(distance_map)
}

#[allow(clippy::type_complexity, clippy::needless_range_loop)]
/// [Floyd–Warshall algorithm](https://en.wikipedia.org/wiki/Floyd%E2%80%93Warshall_algorithm) is an algorithm for all pairs shortest path problem
///
/// Compute all pairs shortest paths in a weighted graph with positive or negative edge weights (but with no negative cycles).
/// Returns HashMap of shortest path lengths. Additionally, returns HashMap of intermediate nodes along shortest path for indicated edges.
///
/// # Arguments
/// * `graph`: graph with no negative cycle
/// * `edge_cost`: closure that returns cost of a particular edge
///
/// # Returns
/// * `Ok`: (if graph contains no negative cycle) a hashmap containing all pairs shortest path distances and a hashmap for all pairs shortest paths
/// * `Err`: if graph contains negative cycle.
///
/// # Complexity
/// * Time complexity: **O(|V|³)**
/// * Auxiliary space: **O(|V|²)**
///
/// # Examples
/// ```rust
/// use petgraph::{prelude::*, Graph, Directed};
/// use petgraph::algo::floyd_warshall::floyd_warshall_path;
/// use std::collections::HashMap;
///
/// let mut graph: Graph<(), (), Directed> = Graph::new();
/// let a = graph.add_node(());
/// let b = graph.add_node(());
/// let c = graph.add_node(());
/// let d = graph.add_node(());
///
/// graph.extend_with_edges(&[
///    (a, b),
///    (a, c),
///    (a, d),
///    (b, c),
///    (b, d),
///    (c, d)
/// ]);
///
/// let weight_map: HashMap<(NodeIndex, NodeIndex), i32> = [
///    ((a, a), 0), ((a, b), 1), ((a, c), 4), ((a, d), 10),
///    ((b, b), 0), ((b, c), 2), ((b, d), 2),
///    ((c, c), 0), ((c, d), 2)
/// ].iter().cloned().collect();
/// //     ----- b --------
/// //    |      ^         | 2
/// //    |    1 |    4    v
/// //  2 |      a ------> c
/// //    |   10 |         | 2
/// //    |      v         v
/// //     --->  d <-------
///
/// let inf = std::i32::MAX;
/// let expected_res: HashMap<(NodeIndex, NodeIndex), i32> = [
///    ((a, a), 0), ((a, b), 1), ((a, c), 3), ((a, d), 3),
///    ((b, a), inf), ((b, b), 0), ((b, c), 2), ((b, d), 2),
///    ((c, a), inf), ((c, b), inf), ((c, c), 0), ((c, d), 2),
///    ((d, a), inf), ((d, b), inf), ((d, c), inf), ((d, d), 0),
/// ].iter().cloned().collect();
///
///
/// let (res, prev) = floyd_warshall_path(&graph, |edge| {
///     if let Some(weight) = weight_map.get(&(edge.source(), edge.target())) {
///         *weight
///     } else {
///         inf
///     }
/// }).unwrap();
///
/// assert_eq!(prev[0][2], Some(1));
///
/// let nodes = [a, b, c, d];
/// for node1 in &nodes {
///     for node2 in &nodes {
///         assert_eq!(res.get(&(*node1, *node2)).unwrap(), expected_res.get(&(*node1, *node2)).unwrap());
///     }
/// }
///
/// ```
pub fn floyd_warshall_path<G, F, K>(
    graph: G,
    edge_cost: F,
) -> Result<(HashMap<(G::NodeId, G::NodeId), K>, Vec<Vec<Option<usize>>>), NegativeCycle>
where
    G: NodeCompactIndexable + IntoEdgeReferences + IntoNodeIdentifiers + GraphProp,
    G::NodeId: Eq + Hash,
    F: FnMut(G::EdgeRef) -> K,
    K: BoundedMeasure + Copy,
{
    let num_of_nodes = graph.node_count();

    // |V|x|V| matrix
    let mut m_dist = Some(vec![vec![K::max(); num_of_nodes]; num_of_nodes]);
    // `prev[source][target]` holds the penultimate vertex on path from `source` to `target`, except `prev[source][source]`, which always stores `source`.
    let mut m_prev = Some(vec![vec![None; num_of_nodes]; num_of_nodes]);

    _floyd_warshall_path(graph, edge_cost, &mut m_dist, &mut m_prev)?;

    let mut distance_map = HashMap::with_capacity(num_of_nodes * num_of_nodes);

    if let Some(dist) = m_dist {
        for i in 0..num_of_nodes {
            for j in 0..num_of_nodes {
                distance_map.insert((graph.from_index(i), graph.from_index(j)), dist[i][j]);
            }
        }
    }

    Ok((distance_map, m_prev.unwrap()))
}

/// Helper function to copy a value to a 2D array
fn set_object<K: Clone>(m_dist: &mut Option<Vec<Vec<K>>>, i: usize, j: usize, value: K) {
    if let Some(dist) = m_dist {
        dist[i][j] = value;
    }
}

/// Helper to check if the distance map is greater then a specific value
fn is_greater<K: PartialOrd>(
    m_dist: &mut Option<Vec<Vec<K>>>,
    i: usize,
    j: usize,
    value: K,
) -> bool {
    if let Some(dist) = m_dist {
        return dist[i][j] > value;
    }
    false
}

/// Helper that implements the floyd warshall routine, but paths are optional for memory overhead.
fn _floyd_warshall_path<G, F, K>(
    graph: G,
    mut edge_cost: F,
    m_dist: &mut Option<Vec<Vec<K>>>,
    m_prev: &mut Option<Vec<Vec<Option<usize>>>>,
) -> Result<(), NegativeCycle>
where
    G: NodeCompactIndexable + IntoEdgeReferences + IntoNodeIdentifiers + GraphProp,
    G::NodeId: Eq + Hash,
    F: FnMut(G::EdgeRef) -> K,
    K: BoundedMeasure + Copy,
{
    let num_of_nodes = graph.node_count();

    // Initialize distances and predecessors for edges
    for edge in graph.edge_references() {
        let source = graph.to_index(edge.source());
        let target = graph.to_index(edge.target());
        let cost = edge_cost(edge);
        if is_greater(m_dist, source, target, cost) {
            set_object(m_dist, source, target, cost);
            set_object(m_prev, source, target, Some(source));

            if !graph.is_directed() {
                set_object(m_dist, target, source, cost);
                set_object(m_prev, target, source, Some(target));
            }
        }
    }

    // Distance of each node to itself is the default value
    for node in graph.node_identifiers() {
        let index = graph.to_index(node);
        set_object(m_dist, index, index, K::default());
        set_object(m_prev, index, index, Some(index));
    }

    // Perform the Floyd-Warshall algorithm
    for k in 0..num_of_nodes {
        for i in 0..num_of_nodes {
            for j in 0..num_of_nodes {
                if let Some(dist) = m_dist {
                    let (result, overflow) = dist[i][k].overflowing_add(dist[k][j]);
                    if !overflow && dist[i][j] > result {
                        dist[i][j] = result;
                        if let Some(prev) = m_prev {
                            prev[i][j] = prev[k][j];
                        }
                    }
                }
            }
        }
    }

    // value less than 0(default value) indicates a negative cycle
    for i in 0..num_of_nodes {
        if let Some(dist) = m_dist {
            if dist[i][i] < K::default() {
                return Err(NegativeCycle(()));
            }
        }
    }
    Ok(())
}
