use alloc::vec;
use core::{
    hash::{BuildHasher, Hash},
    iter::{from_fn, FromIterator},
};

use hashbrown::HashSet;
use indexmap::IndexSet;

use crate::{
    visit::{IntoNeighborsDirected, NodeCount},
    Direction::Outgoing,
};

/// Calculate all simple paths with specified constraints from node `from` to node `to`.
///
/// A simple path is a path without repeating nodes.
/// The number of simple paths between a given pair of vertices can grow exponentially,
/// reaching `O(|V|!)` on complete graphs with `|V|` vertices.
///
/// So if you have a large enough graph, be prepared to wait on the results for years.
/// Or consider extracting only part of the simple paths using the adapter [`Iterator::take`].
/// Also note, that this algorithm does not check that a path exists between `from` and `to`. This may lead to very long running times and it may be worth it to check if a path exists before running this algorithm on large graphs.
///
/// This algorithm is adapted from [NetworkX](https://networkx.github.io/documentation/stable/reference/algorithms/generated/networkx.algorithms.simple_paths.all_simple_paths.html).
/// # Arguments
/// * `graph`: an input graph.
/// * `from`: an initial node of desired paths.
/// * `to`: a target node of desired paths.
/// * `min_intermediate_nodes`: the minimum number of nodes in the desired paths.
/// * `max_intermediate_nodes`: the maximum number of nodes in the desired paths (optional).
/// # Returns
/// Returns an iterator that produces all simple paths from `from` node to `to`, which contains at least `min_intermediate_nodes`
/// and at most `max_intermediate_nodes` intermediate nodes, if given, or limited by the graph's order otherwise.
///
/// # Complexity
/// * Time complexity: for computing the first **k** paths, the running time will be **O(k|V| + k|E|)**.
/// * Auxillary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
/// # Example
/// ```
/// use std::collections::hash_map::RandomState;
/// use petgraph::{algo, prelude::*};
///
/// let mut graph = DiGraph::<&str, i32>::new();
///
/// let a = graph.add_node("a");
/// let b = graph.add_node("b");
/// let c = graph.add_node("c");
/// let d = graph.add_node("d");
///
/// graph.extend_with_edges(&[(a, b, 1), (b, c, 1), (c, d, 1), (a, b, 1), (b, d, 1)]);
///
/// let paths = algo::all_simple_paths::<Vec<_>, _, RandomState>(&graph, a, d, 0, None)
///   .collect::<Vec<_>>();
///
/// assert_eq!(paths.len(), 4);
///
///
/// // Take only 2 paths.
/// let paths = algo::all_simple_paths::<Vec<_>, _, RandomState>(&graph, a, d, 0, None)
///   .take(2)
///   .collect::<Vec<_>>();
///
/// assert_eq!(paths.len(), 2);
///
/// ```
pub fn all_simple_paths<TargetColl, G, S>(
    graph: G,
    from: G::NodeId,
    to: G::NodeId,
    min_intermediate_nodes: usize,
    max_intermediate_nodes: Option<usize>,
) -> impl Iterator<Item = TargetColl>
where
    G: NodeCount,
    G: IntoNeighborsDirected,
    G::NodeId: Eq + Hash,
    TargetColl: FromIterator<G::NodeId>,
    S: BuildHasher + Default,
{
    // how many nodes are allowed in simple path up to target node
    // it is min/max allowed path length minus one, because it is more appropriate when implementing lookahead
    // than constantly add 1 to length of current path
    let max_length = if let Some(l) = max_intermediate_nodes {
        l + 1
    } else {
        graph.node_count() - 1
    };

    let min_length = min_intermediate_nodes + 1;

    // list of visited nodes
    let mut visited: IndexSet<G::NodeId, S> = IndexSet::from_iter(Some(from));
    // list of childs of currently exploring path nodes,
    // last elem is list of childs of last visited node
    let mut stack = vec![graph.neighbors_directed(from, Outgoing)];

    from_fn(move || {
        while let Some(children) = stack.last_mut() {
            if let Some(child) = children.next() {
                if visited.contains(&child) {
                    continue;
                }
                if visited.len() < max_length {
                    if child == to {
                        if visited.len() >= min_length {
                            let path = visited
                                .iter()
                                .cloned()
                                .chain(Some(to))
                                .collect::<TargetColl>();
                            return Some(path);
                        }
                    } else {
                        visited.insert(child);
                        stack.push(graph.neighbors_directed(child, Outgoing));
                    }
                } else {
                    if (child == to || children.any(|v| v == to && !visited.contains(&v)))
                        && visited.len() >= min_length
                    {
                        let path = visited
                            .iter()
                            .cloned()
                            .chain(Some(to))
                            .collect::<TargetColl>();
                        return Some(path);
                    }
                    stack.pop();
                    visited.pop();
                }
            } else {
                stack.pop();
                visited.pop();
            }
        }
        None
    })
}

/// Calculate all simple paths from a source node to any of several target nodes.
///
/// This function is a variant of [`all_simple_paths`] that accepts a `HashSet` of
/// target nodes instead of a single one. A path is yielded as soon as it reaches any
/// node in the `to` set.
///
/// # Performance Considerations
///
/// The efficiency of this function hinges on the graph's structure. It provides significant
/// performance gains on graphs where paths share long initial segments (e.g., trees and DAGs),
/// as the benefit of a single traversal outweighs the `HashSet` lookup overhead.
///
/// Conversely, in dense graphs where paths diverge quickly or for targets very close
/// to the source, the lookup overhead could make repeated calls to [`all_simple_paths`]
/// a faster alternative.
///
/// **Note**: If security is not a concern, a faster hasher (e.g., `FxBuildHasher`)
/// can be specified to minimize the `HashSet` lookup overhead.
///
/// # Arguments
/// * `graph`: an input graph.
/// * `from`: an initial node of desired paths.
/// * `to`: a `HashSet` of target nodes. A path is yielded as soon as it reaches any node in this set.
/// * `min_intermediate_nodes`: the minimum number of nodes in the desired paths.
/// * `max_intermediate_nodes`: the maximum number of nodes in the desired paths (optional).
/// # Returns
/// Returns an iterator that produces all simple paths from `from` node to any node in the `to` set, which contains at least `min_intermediate_nodes`
/// and at most `max_intermediate_nodes` intermediate nodes, if given, or limited by the graph's order otherwise.
///
/// # Complexity
/// * Time complexity: for computing the first **k** paths, the running time will be **O(k|V| + k|E|)**.
/// * Auxillary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
/// # Example
/// ```
/// use petgraph::{algo, prelude::*};
/// use hashbrown::HashSet;
/// use std::collections::hash_map::RandomState;
///
/// let mut graph = DiGraph::<&str, i32>::new();
///
/// let a = graph.add_node("a");
/// let b = graph.add_node("b");
/// let c = graph.add_node("c");
/// let d = graph.add_node("d");
/// graph.extend_with_edges(&[(a, b, 1), (b, c, 1), (b, d, 1)]);
///
/// // Find paths from "a" to either "c" or "d".
/// let targets = HashSet::from_iter([c, d]);
/// let mut paths = algo::all_simple_paths_multi::<Vec<_>, _, RandomState>(&graph, a, &targets, 0, None)
///     .collect::<Vec<_>>();
///
/// paths.sort_by_key(|p| p.clone());
/// let expected_paths = vec![
///     vec![a, b, c],
///     vec![a, b, d],
/// ];
///
/// assert_eq!(paths, expected_paths);
///
/// ```
pub fn all_simple_paths_multi<'a, TargetColl, G, S>(
    graph: G,
    from: G::NodeId,
    to: &'a HashSet<G::NodeId, S>,
    min_intermediate_nodes: usize,
    max_intermediate_nodes: Option<usize>,
) -> impl Iterator<Item = TargetColl> + 'a
where
    G: NodeCount + IntoNeighborsDirected + 'a,
    <G as IntoNeighborsDirected>::NeighborsDirected: 'a,
    G::NodeId: Eq + Hash,
    TargetColl: FromIterator<G::NodeId>,
    S: BuildHasher + Default,
{
    let max_nodes = if let Some(l) = max_intermediate_nodes {
        l + 2
    } else {
        graph.node_count()
    };

    let min_nodes = min_intermediate_nodes + 2;

    // list of visited nodes
    let mut visited: IndexSet<G::NodeId, S> = IndexSet::from_iter(Some(from));
    // list of childs of currently exploring path nodes,
    // last elem is list of childs of last visited node
    let mut stack = vec![graph.neighbors_directed(from, Outgoing)];

    from_fn(move || {
        while let Some(children) = stack.last_mut() {
            if let Some(child) = children.next() {
                if visited.contains(&child) {
                    continue;
                }

                let current_nodes = visited.len();
                let mut valid_path: Option<TargetColl> = None;

                // Check if we've reached a target node
                if to.contains(&child) && (current_nodes + 1) >= min_nodes {
                    valid_path = Some(
                        visited
                            .iter()
                            .cloned()
                            .chain(Some(child))
                            .collect::<TargetColl>(),
                    );
                }

                // Expand the search only if within max length and unexplored target nodes remain
                if (current_nodes < max_nodes)
                    && to.iter().any(|n| *n != child && !visited.contains(n))
                {
                    visited.insert(child);
                    stack.push(graph.neighbors_directed(child, Outgoing));
                }

                // yield the valid path if found
                if valid_path.is_some() {
                    return valid_path;
                }
            } else {
                // All neighbors of the current node have been explored
                stack.pop();
                visited.pop();
            }
        }
        None
    })
}

#[cfg(test)]
mod test {
    use alloc::{vec, vec::Vec};
    use core::iter::FromIterator;
    use std::{collections::hash_map::RandomState, println};

    use hashbrown::HashSet;
    use itertools::assert_equal;

    use super::{all_simple_paths, all_simple_paths_multi};
    use crate::{
        dot::Dot,
        prelude::{DiGraph, UnGraph},
    };

    #[test]
    fn test_all_simple_paths() {
        let graph = DiGraph::<i32, i32, _>::from_edges([
            (0, 1),
            (0, 2),
            (0, 3),
            (1, 2),
            (1, 3),
            (2, 3),
            (2, 4),
            (3, 2),
            (3, 4),
            (4, 2),
            (4, 5),
            (5, 2),
            (5, 3),
        ]);

        let expexted_simple_paths_0_to_5 = vec![
            vec![0usize, 1, 2, 3, 4, 5],
            vec![0, 1, 2, 4, 5],
            vec![0, 1, 3, 2, 4, 5],
            vec![0, 1, 3, 4, 5],
            vec![0, 2, 3, 4, 5],
            vec![0, 2, 4, 5],
            vec![0, 3, 2, 4, 5],
            vec![0, 3, 4, 5],
        ];

        println!("{}", Dot::new(&graph));
        let actual_simple_paths_0_to_5: HashSet<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0u32.into(), 5u32.into(), 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        assert_eq!(actual_simple_paths_0_to_5.len(), 8);
        assert_eq!(
            HashSet::from_iter(expexted_simple_paths_0_to_5),
            actual_simple_paths_0_to_5
        );
    }

    #[test]
    fn test_one_simple_path() {
        let graph = DiGraph::<i32, i32, _>::from_edges([(0, 1), (2, 1)]);

        let expexted_simple_paths_0_to_1 = &[vec![0usize, 1]];
        println!("{}", Dot::new(&graph));
        let actual_simple_paths_0_to_1: Vec<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0u32.into(), 1u32.into(), 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();

        assert_eq!(actual_simple_paths_0_to_1.len(), 1);
        assert_equal(expexted_simple_paths_0_to_1, &actual_simple_paths_0_to_1);
    }

    #[test]
    fn test_no_simple_paths() {
        let graph = DiGraph::<i32, i32, _>::from_edges([(0, 1), (2, 1)]);

        println!("{}", Dot::new(&graph));
        let actual_simple_paths_0_to_2: Vec<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0u32.into(), 2u32.into(), 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();

        assert_eq!(actual_simple_paths_0_to_2.len(), 0);
    }

    #[test]
    fn test_path_graph() {
        let graph = UnGraph::<i32, i32>::from_edges([(0, 1), (1, 2), (2, 3)]);
        let paths: Vec<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0u32.into(), 3u32.into(), 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        assert_eq!(paths, vec![vec![0, 1, 2, 3]]);
    }

    #[test]
    fn test_multi_target_paths() {
        let graph = UnGraph::<i32, i32>::from_edges([(0, 1), (1, 2), (2, 3), (2, 4)]);
        let targets = HashSet::from_iter([3.into(), 4.into()]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths_multi::<_, _, RandomState>(&graph, 0.into(), &targets, 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected: HashSet<Vec<_>> = HashSet::from_iter([vec![0, 1, 2, 3], vec![0, 1, 2, 4]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_digraph_multi_target_paths() {
        let graph = DiGraph::<i32, ()>::from_edges([(0, 1), (1, 2), (2, 3), (2, 4)]);
        let targets = HashSet::from_iter([3.into(), 4.into()]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths_multi::<_, _, RandomState>(&graph, 0.into(), &targets, 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected: HashSet<Vec<_>> = HashSet::from_iter([vec![0, 1, 2, 3], vec![0, 1, 2, 4]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_multi_target_paths_cutoff() {
        let graph = UnGraph::<i32, ()>::from_edges([(0, 1), (1, 2), (2, 3), (2, 4)]);
        let targets = HashSet::from_iter([3.into(), 4.into()]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths_multi::<_, _, RandomState>(&graph, 0.into(), &targets, 0, Some(2))
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected: HashSet<Vec<_>> = HashSet::from_iter([vec![0, 1, 2, 3], vec![0, 1, 2, 4]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_digraph_multi_target_paths_cutoff() {
        let graph = DiGraph::<i32, ()>::from_edges([(0, 1), (1, 2), (2, 3), (2, 4)]);
        let targets = HashSet::from_iter([3.into(), 4.into()]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths_multi::<_, _, RandomState>(&graph, 0.into(), &targets, 0, Some(2))
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected: HashSet<Vec<_>> = HashSet::from_iter([vec![0, 1, 2, 3], vec![0, 1, 2, 4]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_multi_target_paths_inline() {
        let graph = UnGraph::<i32, ()>::from_edges([(0, 1), (1, 2), (2, 3)]);
        let targets = HashSet::from_iter([2.into(), 3.into()]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths_multi::<_, _, RandomState>(&graph, 0.into(), &targets, 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected: HashSet<Vec<_>> = HashSet::from_iter([vec![0, 1, 2], vec![0, 1, 2, 3]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_all_simple_paths_ignores_cycle() {
        let graph = DiGraph::<i32, ()>::from_edges([(0, 1), (1, 2), (2, 0), (1, 3)]);
        let paths: Vec<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0.into(), 3.into(), 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        assert_eq!(paths, vec![vec![0, 1, 3]]);
    }

    #[test]
    fn test_multi_target_paths_in_cycle() {
        let graph = DiGraph::<i32, ()>::from_edges([(0, 1), (1, 2), (2, 0), (1, 3)]);
        let targets = HashSet::from_iter([2.into(), 3.into()]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths_multi::<_, _, RandomState>(&graph, 0.into(), &targets, 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected = HashSet::from_iter([vec![0, 1, 2], vec![0, 1, 3]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_simple_path_source_target() {
        let graph = UnGraph::<i32, ()>::from_edges([(0, 1), (1, 2), (2, 3)]);
        let paths: Vec<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 1.into(), 1.into(), 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        assert!(paths.is_empty());
    }

    #[test]
    fn test_simple_paths_cutoff() {
        let graph =
            UnGraph::<i32, ()>::from_edges([(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0.into(), 1.into(), 0, Some(0))
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        assert_eq!(paths, HashSet::from_iter([vec![0, 1]]));

        let paths: HashSet<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0.into(), 1.into(), 0, Some(1))
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected = HashSet::from_iter([vec![0, 1], vec![0, 2, 1], vec![0, 3, 1]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_multi_target_source_is_target() {
        let graph = UnGraph::<i32, ()>::from_edges([(0, 1), (1, 2)]);
        let targets = HashSet::from_iter([0.into(), 1.into(), 2.into()]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths_multi::<_, _, RandomState>(&graph, 0.into(), &targets, 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected = HashSet::from_iter([vec![0, 1], vec![0, 1, 2]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_all_simple_paths_empty() {
        let graph = UnGraph::<i32, ()>::from_edges([(0, 1), (1, 2), (2, 3)]);
        let paths: Vec<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0.into(), 3.into(), 0, Some(1))
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        assert!(paths.is_empty());
    }

    #[test]
    fn test_all_simple_paths_on_non_trivial_graph() {
        let graph = DiGraph::<i32, ()>::from_edges([
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 4),
            (0, 5),
            (1, 5),
            (1, 3),
            (5, 4),
            (4, 2),
            (4, 3),
        ]);
        let targets = HashSet::from_iter([2.into(), 3.into()]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths_multi::<_, _, RandomState>(&graph, 1.into(), &targets, 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected = HashSet::from_iter([
            vec![1, 2],
            vec![1, 5, 4, 2],
            vec![1, 3, 4, 2],
            vec![1, 3],
            vec![1, 2, 3],
            vec![1, 5, 4, 3],
            vec![1, 5, 4, 2, 3],
        ]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_all_simple_paths_directed() {
        let graph = DiGraph::<i32, ()>::from_edges([(1, 2), (2, 3), (3, 2), (2, 1)]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 1.into(), 3.into(), 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected = HashSet::from_iter([vec![1, 2, 3]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_all_simple_paths_corner_cases() {
        let mut graph = DiGraph::<i32, ()>::new();
        graph.add_node(0);
        graph.add_node(1);

        let paths: Vec<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0.into(), 0.into(), 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        assert!(paths.is_empty());

        let paths: Vec<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0.into(), 1.into(), 0, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        assert!(paths.is_empty());
    }

    #[test]
    fn test_simple_paths_min_intermediate_nodes() {
        let graph =
            UnGraph::<i32, ()>::from_edges([(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths::<_, _, RandomState>(&graph, 0.into(), 1.into(), 2, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected = HashSet::from_iter([vec![0, 2, 3, 1], vec![0, 3, 2, 1]]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_multi_target_paths_min_intermediate_nodes() {
        let graph =
            UnGraph::<i32, ()>::from_edges([(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]);
        let targets = HashSet::from_iter([1.into(), 3.into()]);
        let paths: HashSet<Vec<_>> =
            all_simple_paths_multi::<_, _, RandomState>(&graph, 0.into(), &targets, 2, None)
                .map(|v: Vec<_>| v.into_iter().map(|i| i.index()).collect())
                .collect();
        let expected = HashSet::from_iter([
            vec![0, 2, 3, 1],
            vec![0, 3, 2, 1],
            vec![0, 1, 2, 3],
            vec![0, 2, 1, 3],
        ]);
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_simple_paths_from_node_to_itself_in_complete_graph() {
        // Create a directed graph
        let mut graph = DiGraph::new();

        let a = graph.add_node("A");
        let b = graph.add_node("B");
        let c = graph.add_node("C");

        // Add edges for complete graph (every node connected to every other node)
        graph.add_edge(a, b, ());
        graph.add_edge(a, c, ());
        graph.add_edge(b, a, ());
        graph.add_edge(b, c, ());
        graph.add_edge(c, a, ());
        graph.add_edge(c, b, ());

        // Find all simple paths from A to A
        let paths: Vec<Vec<_>> =
            all_simple_paths::<Vec<_>, _, RandomState>(&graph, a, a, 0, None).collect();

        // The only simple path from a node to itself is a path of length 1 (just the node itself).
        assert!(paths.is_empty());
    }
}
