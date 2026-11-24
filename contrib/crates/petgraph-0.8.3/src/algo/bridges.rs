use crate::visit::{
    EdgeRef, IntoEdgeReferences, IntoNeighbors, IntoNodeIdentifiers, NodeIndexable,
};

use alloc::{vec, vec::Vec};

/// Find all [bridges](https://en.wikipedia.org/wiki/Bridge_(graph_theory)) in a simple undirected graph.
///
/// # Arguments
/// * `graph`: a simple undirected graph.
///
/// # Returns
/// * `impl Iterator`:  the iterator of edge references `G::EdgeRef` representing the edges
///   of the input graph that are bridges. The order of the edges is arbitrary.
///
/// # Complexity
/// * Time complexity: **O(|V| + |E|)**.
/// * Auxiliary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
///
/// # Examples
///
/// ```
/// use petgraph::algo::bridges::bridges;
/// use petgraph::graph::UnGraph;
/// use petgraph::visit::EdgeRef;
///
/// // Create the following graph:
/// // 0----1    4
/// //      | __/|
/// // 5----2/---3
///
/// let mut g = UnGraph::new_undirected();
/// let n0 = g.add_node(());
/// let n1 = g.add_node(());
/// let n2 = g.add_node(());
/// let n3 = g.add_node(());
/// let n4 = g.add_node(());
/// let n5 = g.add_node(());
/// let e0 = g.add_edge(n0, n1, ());
/// let e1 = g.add_edge(n1, n2, ());
/// let e2 = g.add_edge(n2, n3, ());
/// let e3 = g.add_edge(n3, n4, ());
/// let e4 = g.add_edge(n2, n4, ());
/// let e5 = g.add_edge(n5, n2, ());
///
/// let bridges: Vec<_> = bridges(&g).map(|edge_ref| edge_ref.id()).collect();
///
/// // The bridges in this graph are the undirected edges {2, 5}, {1, 2}, {0, 1}.
/// assert_eq!(bridges, vec![e0, e1, e5]);
/// ```
pub fn bridges<G>(graph: G) -> impl Iterator<Item = G::EdgeRef>
where
    G: IntoNodeIdentifiers + IntoNeighbors + NodeIndexable + IntoEdgeReferences,
{
    let mut clock: usize = 0usize;
    // If and when a node was visited by the dfs
    let mut visit_time = vec![None; graph.node_bound()];
    // Lowest time on a node that is the target of a back-edge from the subtree rooted
    // at the indexed node.
    let mut earliest_backedge = vec![usize::MAX; graph.node_bound()];

    for start in 0..graph.node_bound() {
        // If node hasn't been visited yet, make it the root of a new dfs-tree in the forest.
        if visit_time[start].is_none() {
            visit_time[start] = Some(clock);
            clock += 1;

            // Perform a DFS starting at start
            let start = graph.from_index(start);
            let mut stack: Vec<(G::NodeId, G::Neighbors)> = vec![(start, graph.neighbors(start))];

            while let Some((stack_frame, rest_of_stack)) = stack.split_last_mut() {
                let &mut (node, ref mut neighbors) = stack_frame;
                let parent = rest_of_stack.last().map(|&(n, _)| n);

                let node_index = graph.to_index(node);

                if let Some(child) = neighbors.next() {
                    // Pre-order DFS
                    if parent != Some(child) {
                        let child_index = graph.to_index(child);

                        if let Some(time) = visit_time[child_index] {
                            earliest_backedge[node_index] = earliest_backedge[node_index].min(time);
                        } else {
                            visit_time[child_index] = Some(clock);
                            clock += 1;
                            stack.push((child, graph.neighbors(child)));
                        }
                    }
                } else {
                    // Post-order DFS
                    if let Some(parent) = parent {
                        let parent_index = graph.to_index(parent);
                        earliest_backedge[parent_index] =
                            earliest_backedge[parent_index].min(earliest_backedge[node_index]);
                    }
                    stack.pop();
                }
            }
        }
    }

    graph.edge_references().filter(move |edge| {
        let source_index = graph.to_index(edge.source());
        let target_index = graph.to_index(edge.target());

        // All nodes have been visited by the time we return, so unwraps are safe.
        // The node with the lower visit time is the "parent" in the dfs-forest created above.
        let (parent, node) =
            if visit_time[source_index].unwrap() < visit_time[target_index].unwrap() {
                (source_index, target_index)
            } else {
                (target_index, source_index)
            };

        // If there's no back-edge to before parent, then this the only way from parent to here
        // is directly from parent, so it's a bridge edge.
        earliest_backedge[node] > visit_time[parent].unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::EdgeReference;
    use crate::graph::UnGraph;
    use crate::visit::EdgeRef;

    #[test]
    fn test_bridges() {
        let mut g = UnGraph::<i8, i8>::new_undirected();
        let bridge_nodes = |g: &_| {
            bridges(g)
                .map(|e: EdgeReference<_>| (e.source(), e.target()))
                .collect::<Vec<_>>()
        };

        assert_eq!(bridge_nodes(&g), vec![]);
        let n0 = g.add_node(0);
        assert_eq!(bridge_nodes(&g), vec![]);
        let n1 = g.add_node(1);
        assert_eq!(bridge_nodes(&g), vec![]);
        g.add_edge(n0, n1, 0);
        assert_eq!(bridge_nodes(&g), vec![(n0, n1)]);
        let n2 = g.add_node(2);
        assert_eq!(bridge_nodes(&g), vec![(n0, n1)]);
        g.add_edge(n2, n1, 1);
        assert_eq!(bridge_nodes(&g), vec![(n0, n1), (n2, n1)]);
        g.add_edge(n0, n2, 2);
        assert_eq!(bridge_nodes(&g), vec![]);
        let n3 = g.add_node(3);
        let n4 = g.add_node(4);
        g.add_edge(n2, n3, 3);
        g.add_edge(n3, n4, 4);
        assert_eq!(bridge_nodes(&g), vec![(n2, n3), (n3, n4)]);
        g.add_edge(n3, n0, 5);
        assert_eq!(bridge_nodes(&g), vec![(n3, n4)]);
    }
}
