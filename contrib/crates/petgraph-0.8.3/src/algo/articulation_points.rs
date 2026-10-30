use alloc::{vec, vec::Vec};
use core::{cmp::min, hash::Hash};

use fixedbitset::FixedBitSet;
use hashbrown::{HashMap, HashSet};

use crate::visit;
use crate::visit::{EdgeRef, IntoEdges, IntoNodeReferences, NodeIndexable, NodeRef};

/// Find articulation points in a graph using [Tarjan's algorithm](https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm).
///
/// Compute the articulation points of a graph (Nodes, which would increase the number of connected components in the graph.
///
/// # Arguments
/// * `graph`: A directed graph.
///
/// # Returns
/// * `HashSet`: [`struct@hashbrown::HashSet`] of the node ids which correspond to the articulation points of the graph.
///
/// # Complexity
/// - Time complexity: **O(|V| + |E|)**,
/// - Auxiliary space: **O(|V|)**,
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
///
/// # Examples
/// ```rust
/// use petgraph::{
///     algo::articulation_points,
///     graph::{NodeIndex, UnGraph},
///     algo::articulation_points::articulation_points,
/// };
///
/// let mut gr = UnGraph::<&str, ()>::new_undirected();
/// let a = gr.add_node("A");
/// let b = gr.add_node("B");
/// let c = gr.add_node("C");
///
/// gr.add_edge(a, b, ());
/// gr.add_edge(b, c, ());
///
/// let articulation_points: Vec<&str> = articulation_points(&gr)
///     .into_iter()
///     .map(|node_idx| gr[node_idx])
///     .collect();
///
/// // Articulation Points: ["B"]
/// println!("Articulation Points: {:?}", articulation_points);
/// ```
pub fn articulation_points<G>(g: G) -> HashSet<G::NodeId>
where
    G: IntoNodeReferences + IntoEdges + NodeIndexable + visit::GraphProp,
    G::NodeWeight: Clone,
    G::EdgeWeight: Clone + PartialOrd,
    G::NodeId: Eq + Hash,
{
    let graph_size = g.node_references().size_hint().0;
    let mut auxiliary_const = ArticulationPointTracker::new(graph_size);

    for node in g.node_references() {
        let node_id = g.to_index(node.id());
        if !auxiliary_const.visited[node_id] {
            _dfs(&g, node_id, &mut auxiliary_const);
        }
    }

    auxiliary_const
        .articulation_points
        .into_iter()
        .map(|id| g.from_index(id))
        .collect()
}

/// Small helper enum that defines the various splitup recursion steps of Tarjan's algorithm.
enum RecursionStep {
    BaseStep(usize),
    ProcessChildStep(usize, usize),
    NoBackEdgeConditionCheck(usize, usize),
    RootMoreThanTwoChildrenCheck(usize),
}

/// Internal auxiliary helper struct for global variables.
struct ArticulationPointTracker {
    visited: FixedBitSet,
    low: Vec<usize>,
    disc: Vec<usize>,
    parent: Vec<usize>,
    time: usize,
    articulation_points: HashSet<usize>,
}

impl ArticulationPointTracker {
    fn new(graph_size: usize) -> Self {
        Self {
            visited: FixedBitSet::with_capacity(graph_size),
            low: vec![usize::MAX; graph_size],
            disc: vec![usize::MAX; graph_size],
            parent: vec![usize::MAX; graph_size],
            articulation_points: HashSet::with_capacity(graph_size),
            time: 0,
        }
    }
}

/// Helper that performs the required DFS in an iterative manner.
fn _dfs<G>(g: &G, target_node: usize, articulation_point_tracker: &mut ArticulationPointTracker)
where
    G: IntoEdges + NodeIndexable,
{
    let mut stack: Vec<RecursionStep> = vec![RecursionStep::BaseStep(target_node)];
    let mut children_count: HashMap<usize, usize> = HashMap::new();

    while let Some(recursion_step) = stack.pop() {
        match recursion_step {
            RecursionStep::BaseStep(current_node) => {
                articulation_point_tracker.visited.insert(current_node);
                articulation_point_tracker.disc[current_node] = articulation_point_tracker.time;
                articulation_point_tracker.low[current_node] = articulation_point_tracker.time;
                articulation_point_tracker.time += 1;

                stack.push(RecursionStep::RootMoreThanTwoChildrenCheck(current_node));
                for edge in g.edges(g.from_index(current_node)) {
                    let child_node = g.to_index(edge.target());
                    stack.push(RecursionStep::ProcessChildStep(current_node, child_node));
                }
            }
            RecursionStep::ProcessChildStep(current_node, child_node) => {
                if !articulation_point_tracker.visited.contains(child_node) {
                    articulation_point_tracker.parent[child_node] = current_node;
                    children_count
                        .entry(current_node)
                        .and_modify(|c| *c += 1)
                        .or_insert(1);

                    stack.push(RecursionStep::NoBackEdgeConditionCheck(
                        current_node,
                        child_node,
                    ));
                    stack.push(RecursionStep::BaseStep(child_node));
                } else if child_node != articulation_point_tracker.parent[current_node] {
                    articulation_point_tracker.low[current_node] = min(
                        articulation_point_tracker.low[current_node],
                        articulation_point_tracker.disc[child_node],
                    );
                }
            }
            RecursionStep::NoBackEdgeConditionCheck(current_node, child_node) => {
                articulation_point_tracker.low[current_node] = min(
                    articulation_point_tracker.low[current_node],
                    articulation_point_tracker.low[child_node],
                );

                if articulation_point_tracker.parent[current_node] != usize::MAX
                    && articulation_point_tracker.low[child_node]
                        >= articulation_point_tracker.disc[current_node]
                {
                    articulation_point_tracker
                        .articulation_points
                        .insert(current_node);
                }
            }

            RecursionStep::RootMoreThanTwoChildrenCheck(current_node) => {
                let child_count = children_count.get(&current_node).cloned().unwrap_or(0);
                if articulation_point_tracker.parent[current_node] == usize::MAX && child_count > 1
                {
                    articulation_point_tracker
                        .articulation_points
                        .insert(current_node);
                }
            }
        }
    }
}
