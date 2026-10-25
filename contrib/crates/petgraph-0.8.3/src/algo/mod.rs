/*!
This module contains most of `petgraph`'s algorithms to operate on graphs. Some, very simple search
algorithms like depth-first search or breadth-first search are implemented in the
[`visit`](crate::visit) module.

The `algo` module contains multiple submodules, each implementing a specific algorithm or set of
algorithms. Some functions, like [`connected_components`], are implemented directly in this module.

It is a goal to gradually migrate the algorithms to be based on graph traits
so that they are generally applicable. For now, some of these still require
the `Graph` type.
*/

pub mod articulation_points;
pub mod astar;
pub mod bellman_ford;
pub mod bridges;
pub mod coloring;
pub mod dijkstra;
pub mod dominators;
pub mod feedback_arc_set;
pub mod floyd_warshall;
pub mod ford_fulkerson;
pub mod isomorphism;
pub mod johnson;
pub mod k_shortest_path;
pub mod matching;
pub mod maximal_cliques;
pub mod maximum_flow;
pub mod min_spanning_tree;
pub mod page_rank;
pub mod scc;
pub mod simple_paths;
pub mod spfa;
#[cfg(feature = "stable_graph")]
pub mod steiner_tree;
pub mod tred;

use alloc::{vec, vec::Vec};

use crate::prelude::*;

use super::graph::IndexType;
use super::unionfind::UnionFind;
use super::visit::{
    GraphBase, GraphRef, IntoEdgeReferences, IntoNeighbors, IntoNeighborsDirected,
    IntoNodeIdentifiers, NodeCompactIndexable, NodeIndexable, Reversed, VisitMap, Visitable,
};
use super::EdgeType;
use crate::visit::Walker;

pub use astar::astar;
pub use bellman_ford::{bellman_ford, find_negative_cycle};
pub use bridges::bridges;
pub use coloring::dsatur_coloring;
pub use dijkstra::{bidirectional_dijkstra, dijkstra};
pub use feedback_arc_set::greedy_feedback_arc_set;
pub use floyd_warshall::floyd_warshall;
pub use isomorphism::{
    is_isomorphic, is_isomorphic_matching, is_isomorphic_subgraph, is_isomorphic_subgraph_matching,
    subgraph_isomorphisms_iter,
};
pub use johnson::johnson;
pub use k_shortest_path::k_shortest_path;
pub use matching::{greedy_matching, maximum_matching, Matching};
pub use maximal_cliques::maximal_cliques;
pub use maximum_flow::{dinics, ford_fulkerson};
pub use min_spanning_tree::{min_spanning_tree, min_spanning_tree_prim};
pub use page_rank::page_rank;
#[allow(deprecated)]
pub use scc::scc;
pub use scc::{
    kosaraju_scc::kosaraju_scc,
    tarjan_scc::{tarjan_scc, TarjanScc},
};
pub use simple_paths::{all_simple_paths, all_simple_paths_multi};
pub use spfa::spfa;
#[cfg(feature = "stable_graph")]
pub use steiner_tree::steiner_tree;

#[cfg(feature = "rayon")]
pub use johnson::parallel_johnson;

/// Return the number of connected components of the graph.
///
/// For a directed graph, this is the *weakly* connected components.
///
/// # Arguments
/// * `g`: an input graph.
///
/// # Returns
/// * `usize`: the number of connected components if `g` is undirected
///   or number of *weakly* connected components if `g` is directed.
///
/// # Complexity
/// * Time complexity: amortized **O(|E| + |V|log|V|)**.
/// * Auxiliary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
/// # Example
/// ```rust
/// use petgraph::Graph;
/// use petgraph::algo::connected_components;
/// use petgraph::prelude::*;
///
/// let mut graph : Graph<(),(),Directed>= Graph::new();
/// let a = graph.add_node(()); // node with no weight
/// let b = graph.add_node(());
/// let c = graph.add_node(());
/// let d = graph.add_node(());
/// let e = graph.add_node(());
/// let f = graph.add_node(());
/// let g = graph.add_node(());
/// let h = graph.add_node(());
///
/// graph.extend_with_edges(&[
///     (a, b),
///     (b, c),
///     (c, d),
///     (d, a),
///     (e, f),
///     (f, g),
///     (g, h),
///     (h, e)
/// ]);
/// // a ----> b       e ----> f
/// // ^       |       ^       |
/// // |       v       |       v
/// // d <---- c       h <---- g
///
/// assert_eq!(connected_components(&graph),2);
/// graph.add_edge(b,e,());
/// assert_eq!(connected_components(&graph),1);
/// ```
pub fn connected_components<G>(g: G) -> usize
where
    G: NodeCompactIndexable + IntoEdgeReferences,
{
    let mut node_sets = UnionFind::new(g.node_bound());
    for edge in g.edge_references() {
        let (a, b) = (edge.source(), edge.target());

        // union the two nodes of the edge
        node_sets.union(g.to_index(a), g.to_index(b));
    }

    let mut labels = node_sets.into_labeling();
    labels.sort_unstable();
    labels.dedup();
    labels.len()
}

/// Return `true` if the input graph contains a cycle.
///
/// Always treats the input graph as if undirected.
///
/// # Arguments:
/// `g`: an input graph that always treated as undirected.
///
/// # Returns
/// `true`: if the input graph contains a cycle.
/// `false`: otherwise.
///
/// # Complexity
/// * Time complexity: amortized **O(|E|)**.
/// * Auxiliary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
pub fn is_cyclic_undirected<G>(g: G) -> bool
where
    G: NodeIndexable + IntoEdgeReferences,
{
    let mut edge_sets = UnionFind::new(g.node_bound());
    for edge in g.edge_references() {
        let (a, b) = (edge.source(), edge.target());

        // union the two nodes of the edge
        //  -- if they were already the same, then we have a cycle
        if !edge_sets.union(g.to_index(a), g.to_index(b)) {
            return true;
        }
    }
    false
}

/// Perform a topological sort of a directed graph.
///
/// `toposort` returns `Err` on graphs with cycles.
/// To handle graphs with cycles, use the one of scc algorithms or
/// [`DfsPostOrder`](struct@crate::visit::DfsPostOrder)
///   instead of this function.
///
/// The implementation is iterative.
///
/// # Arguments
/// * `g`: an acyclic directed graph.
/// * `space`: optional [`DfsSpace`]. If `space` is not `None`,
///   it is used instead of creating a new workspace for graph traversal.
///
/// # Returns
/// * `Ok`: a vector of nodes in topological order: each node is ordered before its successors
///   (if the graph was acyclic).
/// * `Err`: [`Cycle`] if the graph was not acyclic. Self loops are also cycles this case.
///
/// # Complexity
/// * Time complexity: **O(|V| + |E|)**.
/// * Auxiliary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
pub fn toposort<G>(
    g: G,
    space: Option<&mut DfsSpace<G::NodeId, G::Map>>,
) -> Result<Vec<G::NodeId>, Cycle<G::NodeId>>
where
    G: IntoNeighborsDirected + IntoNodeIdentifiers + Visitable,
{
    // based on kosaraju scc
    with_dfs(g, space, |dfs| {
        dfs.reset(g);
        let mut finished = g.visit_map();

        let mut finish_stack = Vec::new();
        for i in g.node_identifiers() {
            if dfs.discovered.is_visited(&i) {
                continue;
            }
            dfs.stack.push(i);
            while let Some(&nx) = dfs.stack.last() {
                if dfs.discovered.visit(nx) {
                    // First time visiting `nx`: Push neighbors, don't pop `nx`
                    for succ in g.neighbors(nx) {
                        if succ == nx {
                            // self cycle
                            return Err(Cycle(nx));
                        }
                        if !dfs.discovered.is_visited(&succ) {
                            dfs.stack.push(succ);
                        }
                    }
                } else {
                    dfs.stack.pop();
                    if finished.visit(nx) {
                        // Second time: All reachable nodes must have been finished
                        finish_stack.push(nx);
                    }
                }
            }
        }
        finish_stack.reverse();

        dfs.reset(g);
        for &i in &finish_stack {
            dfs.move_to(i);
            let mut cycle = false;
            while let Some(j) = dfs.next(Reversed(g)) {
                if cycle {
                    return Err(Cycle(j));
                }
                cycle = true;
            }
        }

        Ok(finish_stack)
    })
}

/// Return `true` if the input directed graph contains a cycle.
///
/// This implementation is recursive; use [`toposort`] if an alternative is needed.
///
/// # Arguments:
/// `g`: a directed graph.
///
/// # Returns
/// `true`: if the input graph contains a cycle.
/// `false`: otherwise.
///
/// # Complexity
/// * Time complexity: **O(|V| + |E|)**.
/// * Auxiliary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
pub fn is_cyclic_directed<G>(g: G) -> bool
where
    G: IntoNodeIdentifiers + IntoNeighbors + Visitable,
{
    use crate::visit::{depth_first_search, DfsEvent};

    depth_first_search(g, g.node_identifiers(), |event| match event {
        DfsEvent::BackEdge(_, _) => Err(()),
        _ => Ok(()),
    })
    .is_err()
}

type DfsSpaceType<G> = DfsSpace<<G as GraphBase>::NodeId, <G as Visitable>::Map>;

/// Workspace for a graph traversal.
#[derive(Clone, Debug)]
pub struct DfsSpace<N, VM> {
    dfs: Dfs<N, VM>,
}

impl<N, VM> DfsSpace<N, VM>
where
    N: Copy + PartialEq,
    VM: VisitMap<N>,
{
    pub fn new<G>(g: G) -> Self
    where
        G: GraphRef + Visitable<NodeId = N, Map = VM>,
    {
        DfsSpace { dfs: Dfs::empty(g) }
    }
}

impl<N, VM> Default for DfsSpace<N, VM>
where
    VM: VisitMap<N> + Default,
{
    fn default() -> Self {
        DfsSpace {
            dfs: Dfs {
                stack: <_>::default(),
                discovered: <_>::default(),
            },
        }
    }
}

/// Create a Dfs if it's needed
fn with_dfs<G, F, R>(g: G, space: Option<&mut DfsSpaceType<G>>, f: F) -> R
where
    G: GraphRef + Visitable,
    F: FnOnce(&mut Dfs<G::NodeId, G::Map>) -> R,
{
    let mut local_visitor;
    let dfs = if let Some(v) = space {
        &mut v.dfs
    } else {
        local_visitor = Dfs::empty(g);
        &mut local_visitor
    };
    f(dfs)
}

/// Check if there exists a path starting at `from` and reaching `to`.
///
/// If `from` and `to` are equal, this function returns true.
///
/// # Arguments:
/// * `g`: an input graph.
/// * `from`: the first node of a desired path.
/// * `to`: the last node of a desired path.
/// * `space`: optional [`DfsSpace`]. If `space` is not `None`,
///   it is used instead of creating a new workspace for graph traversal.
///
/// # Returns
/// * `true`: if there exists a path starting at `from` and reaching
///   `to` or `from` and `to` are equal.
/// * `false`: otherwise.
///
/// # Complexity
/// * Time complexity: **O(|V| + |E|)**.
/// * Auxiliary space: **O(|V|)** or **O(1)** if `space` was provided.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
pub fn has_path_connecting<G>(
    g: G,
    from: G::NodeId,
    to: G::NodeId,
    space: Option<&mut DfsSpace<G::NodeId, G::Map>>,
) -> bool
where
    G: IntoNeighbors + Visitable,
{
    with_dfs(g, space, |dfs| {
        dfs.reset(g);
        dfs.move_to(from);
        dfs.iter(g).any(|x| x == to)
    })
}

/// [Graph] Condense every strongly connected component into a single node and return the result.
///
/// # Arguments
/// * `g`: an input [`Graph`].
/// * `make_acyclic`: if `true`, self-loops and multi edges are ignored, guaranteeing that
///   the output is acyclic.
///
/// # Returns
/// Returns a `Graph` with nodes `Vec<N>` representing strongly connected components.
///
/// # Complexity
/// * Time complexity: **O(|V| + |E|)**.
/// * Auxiliary space: **O(|V| + |E|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
/// # Examples
/// ```rust
/// use petgraph::Graph;
/// use petgraph::algo::condensation;
/// use petgraph::prelude::*;
///
/// let mut graph : Graph<(),(),Directed> = Graph::new();
/// let a = graph.add_node(()); // node with no weight
/// let b = graph.add_node(());
/// let c = graph.add_node(());
/// let d = graph.add_node(());
/// let e = graph.add_node(());
/// let f = graph.add_node(());
/// let g = graph.add_node(());
/// let h = graph.add_node(());
///
/// graph.extend_with_edges(&[
///     (a, b),
///     (b, c),
///     (c, d),
///     (d, a),
///     (b, e),
///     (e, f),
///     (f, g),
///     (g, h),
///     (h, e)
/// ]);
///
/// // a ----> b ----> e ----> f
/// // ^       |       ^       |
/// // |       v       |       v
/// // d <---- c       h <---- g
///
/// let condensed_graph = condensation(graph,false);
/// let A = NodeIndex::new(0);
/// let B = NodeIndex::new(1);
/// assert_eq!(condensed_graph.node_count(), 2);
/// assert_eq!(condensed_graph.edge_count(), 9);
/// assert_eq!(condensed_graph.neighbors(A).collect::<Vec<_>>(), vec![A, A, A, A]);
/// assert_eq!(condensed_graph.neighbors(B).collect::<Vec<_>>(), vec![A, B, B, B, B]);
/// ```
/// If `make_acyclic` is true, self-loops and multi edges are ignored:
///
/// ```rust
/// # use petgraph::Graph;
/// # use petgraph::algo::condensation;
/// # use petgraph::prelude::*;
/// #
/// # let mut graph : Graph<(),(),Directed> = Graph::new();
/// # let a = graph.add_node(()); // node with no weight
/// # let b = graph.add_node(());
/// # let c = graph.add_node(());
/// # let d = graph.add_node(());
/// # let e = graph.add_node(());
/// # let f = graph.add_node(());
/// # let g = graph.add_node(());
/// # let h = graph.add_node(());
/// #
/// # graph.extend_with_edges(&[
/// #    (a, b),
/// #    (b, c),
/// #    (c, d),
/// #    (d, a),
/// #    (b, e),
/// #    (e, f),
/// #    (f, g),
/// #    (g, h),
/// #    (h, e)
/// # ]);
/// let acyclic_condensed_graph = condensation(graph, true);
/// let A = NodeIndex::new(0);
/// let B = NodeIndex::new(1);
/// assert_eq!(acyclic_condensed_graph.node_count(), 2);
/// assert_eq!(acyclic_condensed_graph.edge_count(), 1);
/// assert_eq!(acyclic_condensed_graph.neighbors(B).collect::<Vec<_>>(), vec![A]);
/// ```
pub fn condensation<N, E, Ty, Ix>(
    g: Graph<N, E, Ty, Ix>,
    make_acyclic: bool,
) -> Graph<Vec<N>, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    let sccs = kosaraju_scc(&g);
    let mut condensed: Graph<Vec<N>, E, Ty, Ix> = Graph::with_capacity(sccs.len(), g.edge_count());

    // Build a map from old indices to new ones.
    let mut node_map = vec![NodeIndex::end(); g.node_count()];
    for comp in sccs {
        let new_nix = condensed.add_node(Vec::new());
        for nix in comp {
            node_map[nix.index()] = new_nix;
        }
    }

    // Consume nodes and edges of the old graph and insert them into the new one.
    let (nodes, edges) = g.into_nodes_edges();
    for (nix, node) in nodes.into_iter().enumerate() {
        condensed[node_map[nix]].push(node.weight);
    }
    for edge in edges {
        let source = node_map[edge.source().index()];
        let target = node_map[edge.target().index()];
        if make_acyclic {
            if source != target {
                condensed.update_edge(source, target, edge.weight);
            }
        } else {
            condensed.add_edge(source, target, edge.weight);
        }
    }
    condensed
}

/// An algorithm error: a cycle was found in the graph.
#[derive(Clone, Debug, PartialEq)]
pub struct Cycle<N>(pub(crate) N);

impl<N> Cycle<N> {
    /// Return a node id that participates in the cycle
    pub fn node_id(&self) -> N
    where
        N: Copy,
    {
        self.0
    }
}

/// An algorithm error: a cycle of negative weights was found in the graph.
#[derive(Clone, Debug, PartialEq)]
pub struct NegativeCycle(pub ());

/// Return `true` if the graph\* is bipartite.
///
/// A graph is bipartite if its nodes can be divided into
/// two disjoint and indepedent sets U and V such that every edge connects U to one in V.
///
/// This algorithm implements 2-coloring algorithm based on the BFS algorithm.
/// Always treats the input graph as if undirected.
///
/// \* The algorithm checks only the subgraph that is reachable from the `start`.
///
/// # Arguments
/// * `g`: an input graph.
/// * `start`: some node of the graph.
///
/// # Returns
/// * `true`: if the subgraph accessible from the start node is bipartite.
/// * `false`: if such a subgraph is not bipartite.
///
/// # Complexity
/// * Time complexity: **O(|V| + |E|)**.
/// * Auxiliary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
pub fn is_bipartite_undirected<G, N, VM>(g: G, start: N) -> bool
where
    G: GraphRef + Visitable<NodeId = N, Map = VM> + IntoNeighbors<NodeId = N>,
    N: Copy + PartialEq + core::fmt::Debug,
    VM: VisitMap<N>,
{
    let mut red = g.visit_map();
    red.visit(start);
    let mut blue = g.visit_map();

    let mut stack = ::alloc::collections::VecDeque::new();
    stack.push_front(start);

    while let Some(node) = stack.pop_front() {
        let is_red = red.is_visited(&node);
        let is_blue = blue.is_visited(&node);

        assert!(is_red ^ is_blue);

        for neighbour in g.neighbors(node) {
            let is_neigbour_red = red.is_visited(&neighbour);
            let is_neigbour_blue = blue.is_visited(&neighbour);

            if (is_red && is_neigbour_red) || (is_blue && is_neigbour_blue) {
                return false;
            }

            if !is_neigbour_red && !is_neigbour_blue {
                //hasn't been visited yet

                match (is_red, is_blue) {
                    (true, false) => {
                        blue.visit(neighbour);
                    }
                    (false, true) => {
                        red.visit(neighbour);
                    }
                    (_, _) => {
                        panic!("Invariant doesn't hold");
                    }
                }

                stack.push_back(neighbour);
            }
        }
    }

    true
}

use core::fmt::Debug;
use core::ops::Add;

/// Associated data that can be used for measures (such as length).
pub trait Measure: Debug + PartialOrd + Add<Self, Output = Self> + Default + Clone {}

impl<M> Measure for M where M: Debug + PartialOrd + Add<M, Output = M> + Default + Clone {}

/// A floating-point measure.
pub trait FloatMeasure: Measure + Copy {
    fn zero() -> Self;
    fn infinite() -> Self;
    fn from_f32(val: f32) -> Self;
    fn from_f64(val: f64) -> Self;
}

impl FloatMeasure for f32 {
    fn zero() -> Self {
        0.
    }
    fn infinite() -> Self {
        1. / 0.
    }
    fn from_f32(val: f32) -> Self {
        val
    }
    fn from_f64(val: f64) -> Self {
        val as f32
    }
}

impl FloatMeasure for f64 {
    fn zero() -> Self {
        0.
    }
    fn infinite() -> Self {
        1. / 0.
    }
    fn from_f32(val: f32) -> Self {
        val as f64
    }
    fn from_f64(val: f64) -> Self {
        val
    }
}

pub trait BoundedMeasure: Measure + core::ops::Sub<Self, Output = Self> {
    fn min() -> Self;
    fn max() -> Self;
    fn overflowing_add(self, rhs: Self) -> (Self, bool);
    fn from_f32(val: f32) -> Self;
    fn from_f64(val: f64) -> Self;
}

macro_rules! impl_bounded_measure_integer(
    ( $( $t:ident ),* ) => {
        $(
            impl BoundedMeasure for $t {
                fn min() -> Self {
                    $t::MIN
                }

                fn max() -> Self {
                    $t::MAX
                }

                fn overflowing_add(self, rhs: Self) -> (Self, bool) {
                    self.overflowing_add(rhs)
                }

                fn from_f32(val: f32) -> Self {
                    val as $t
                }

                fn from_f64(val: f64) -> Self {
                    val as $t
                }
            }
        )*
    };
);

impl_bounded_measure_integer!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize);

macro_rules! impl_bounded_measure_float(
    ( $( $t:ident ),* ) => {
        $(
            impl BoundedMeasure for $t {
                fn min() -> Self {
                    $t::MIN
                }

                fn max() -> Self {
                    $t::MAX
                }

                fn overflowing_add(self, rhs: Self) -> (Self, bool) {
                    // for an overflow: a + b > max: both values need to be positive and a > max - b must be satisfied
                    let overflow = self > Self::default() && rhs > Self::default() && self > $t::MAX - rhs;

                    // for an underflow: a + b < min: overflow can not happen and both values must be negative and a < min - b must be satisfied
                    let underflow = !overflow && self < Self::default() && rhs < Self::default() && self < $t::MIN - rhs;

                    (self + rhs, overflow || underflow)
                }

                fn from_f32(val: f32) -> Self {
                    val as $t
                }

                fn from_f64(val: f64) -> Self {
                    val as $t
                }
            }
        )*
    };
);

impl_bounded_measure_float!(f32, f64);

/// A floating-point measure that can be computed from `usize`
/// and with a default measure of proximity.  
pub trait UnitMeasure:
    Measure
    + core::ops::Sub<Self, Output = Self>
    + core::ops::Mul<Self, Output = Self>
    + core::ops::Div<Self, Output = Self>
    + core::iter::Sum
{
    fn zero() -> Self;
    fn one() -> Self;
    fn from_usize(nb: usize) -> Self;
    fn default_tol() -> Self;
    fn from_f32(val: f32) -> Self;
    fn from_f64(val: f64) -> Self;
}

macro_rules! impl_unit_measure(
    ( $( $t:ident ),* )=> {
        $(
            impl UnitMeasure for $t {
                fn zero() -> Self {
                    0 as $t
                }
                fn one() -> Self {
                    1 as $t
                }

                fn from_usize(nb: usize) -> Self {
                    nb as $t
                }

                fn default_tol() -> Self {
                    1e-6 as $t
                }

                fn from_f32(val: f32) -> Self {
                    val as $t
                }

                fn from_f64(val: f64) -> Self {
                    val as $t
                }
            }

        )*
    }
);
impl_unit_measure!(f32, f64);

/// Some measure of positive numbers, assuming positive
/// float-pointing numbers
pub trait PositiveMeasure: Measure + Copy {
    fn zero() -> Self;
    fn max() -> Self;
}

macro_rules! impl_positive_measure(
    ( $( $t:ident ),* )=> {
        $(
            impl PositiveMeasure for $t {
                fn zero() -> Self {
                    0 as $t
                }
                fn max() -> Self {
                    $t::MAX
                }
            }

        )*
    }
);

impl_positive_measure!(u8, u16, u32, u64, u128, usize, f32, f64);
