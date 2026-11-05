//! Shortest Path Faster Algorithm.
use alloc::collections::VecDeque;

use super::{bellman_ford::Paths, BoundedMeasure, NegativeCycle};
use crate::prelude::*;
use crate::visit::{IntoEdges, IntoNodeIdentifiers, NodeIndexable};
use alloc::{vec, vec::Vec};

/// Compute shortest paths from node `source` to all other.
///
/// Using the [Shortest Path Faster Algorithm][spfa].
/// Compute shortest distances from node `source` to all other.
///
/// Compute shortest paths lengths in a weighted graph with positive or negative edge weights,
/// but with no negative cycles.
///
/// ## Arguments
/// * `graph`: weighted graph.
/// * `source`: the source vertex, for which we calculate the lengths of the shortest paths to all the others.
/// * `edge_cost`: closure that returns the cost of a particular edge.
///
/// ## Returns
/// * `Err`: if graph contains negative cycle.
/// * `Ok`: a pair of a vector of shortest distances and a vector
///   of predecessors of each vertex along the shortest path.
///
/// ## Complexity
/// * Time complexity: **O(|V||E|)**, but it's generally assumed that in the average case it is **O(|E|)**.
/// * Auxiliary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
///
/// [spfa]: https://www.geeksforgeeks.org/shortest-path-faster-algorithm/
///
/// # Example
///
/// ```
/// use petgraph::Graph;
/// use petgraph::algo::spfa;
///
/// let mut g = Graph::new();
/// let a = g.add_node(()); // node with no weight
/// let b = g.add_node(());
/// let c = g.add_node(());
/// let d = g.add_node(());
/// let e = g.add_node(());
/// let f = g.add_node(());
/// g.extend_with_edges(&[
///     (0, 1, 3.0),
///     (0, 3, 2.0),
///     (1, 2, 1.0),
///     (1, 5, 7.0),
///     (2, 4, -4.0),
///     (3, 4, -1.0),
///     (4, 5, 1.0),
/// ]);
///
/// // Graph represented with the weight of each edge.
/// //
/// //     3       1
/// // a ----- b ----- c
/// // | 2     | 7     |
/// // d       f       | -4
/// // | -1    | 1     |
/// // \------ e ------/
///
/// let path = spfa(&g, a, |edge| *edge.weight());
/// assert!(path.is_ok());
/// let path = path.unwrap();
/// assert_eq!(path.distances, vec![0.0 ,     3.0,     4.0,    2.0,     0.0,     1.0]);
/// assert_eq!(path.predecessors, vec![None, Some(a), Some(b), Some(a), Some(c), Some(e)]);
///
///
/// // Negative cycle.
/// let graph = Graph::<(), f32>::from_edges(&[
///     (0, 1, 2.0), (1, 2, 2.0), (2, 0, -10.0)]);
///
/// assert!(spfa(&graph, 0.into(), |edge| *edge.weight()).is_err());
/// ```
pub fn spfa<G, F, K>(
    graph: G,
    source: G::NodeId,
    edge_cost: F,
) -> Result<Paths<G::NodeId, K>, NegativeCycle>
where
    G: IntoEdges + IntoNodeIdentifiers + NodeIndexable,
    F: FnMut(G::EdgeRef) -> K,
    K: BoundedMeasure + Copy,
{
    let ix = |i| graph.to_index(i);

    let pred = vec![None; graph.node_bound()];
    let mut dist = vec![K::max(); graph.node_bound()];
    dist[ix(source)] = K::default();

    // Queue of vertices capable of relaxation of the found shortest distances.
    let mut queue: VecDeque<G::NodeId> = VecDeque::with_capacity(graph.node_bound());
    let mut in_queue = vec![false; graph.node_bound()];

    queue.push_back(source);
    in_queue[ix(source)] = true;

    let (distances, predecessors) = spfa_loop(graph, dist, Some(pred), queue, in_queue, edge_cost)?;

    Ok(Paths {
        distances,
        predecessors: predecessors.unwrap_or_default(),
    })
}

/// The main cycle of the SPFA algorithm. Calculating the predecessors is optional.
///
/// The `queue` must be pre-initialized by at least one `source` node.
/// The content of `in_queue` must match to `queue`.
#[allow(clippy::type_complexity)]
pub(crate) fn spfa_loop<G, F, K>(
    graph: G,
    mut distances: Vec<K>,
    mut predecessors: Option<Vec<Option<G::NodeId>>>,
    mut queue: VecDeque<G::NodeId>,
    mut in_queue: Vec<bool>,
    mut edge_cost: F,
) -> Result<(Vec<K>, Option<Vec<Option<G::NodeId>>>), NegativeCycle>
where
    G: IntoEdges + IntoNodeIdentifiers + NodeIndexable,
    F: FnMut(G::EdgeRef) -> K,
    K: BoundedMeasure + Copy,
{
    let ix = |i| graph.to_index(i);

    // Keep track of how many times each vertex appeared
    // in the queue to be able to detect a negative cycle.
    let mut visits = vec![0; graph.node_bound()];

    while let Some(i) = queue.pop_front() {
        in_queue[ix(i)] = false;

        // In a graph without a negative cycle, no vertex can improve
        // the shortest distances by more than |V| times.
        if visits[ix(i)] >= graph.node_bound() {
            return Err(NegativeCycle(()));
        }
        visits[ix(i)] += 1;

        for edge in graph.edges(i) {
            let j = edge.target();
            let w = edge_cost(edge);

            let (dist, overflow) = distances[ix(i)].overflowing_add(w);

            if !overflow && dist < distances[ix(j)] {
                distances[ix(j)] = dist;
                if let Some(p) = predecessors.as_mut() {
                    p[ix(j)] = Some(i)
                }

                if !in_queue[ix(j)] {
                    in_queue[ix(j)] = true;
                    queue.push_back(j);
                }
            }
        }
    }

    Ok((distances, predecessors))
}
