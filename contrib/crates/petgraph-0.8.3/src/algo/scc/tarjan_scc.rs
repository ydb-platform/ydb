use alloc::vec::Vec;
use core::num::NonZeroUsize;

use crate::visit::{IntoNeighbors, IntoNodeIdentifiers, NodeIndexable};

#[derive(Copy, Clone, Debug)]
struct NodeData {
    rootindex: Option<NonZeroUsize>,
}

/// A reusable state for computing the *strongly connected components* using [Tarjan's algorithm][1].
///
/// [1]: https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
#[derive(Debug)]
pub struct TarjanScc<N> {
    index: usize,
    componentcount: usize,
    nodes: Vec<NodeData>,
    stack: Vec<N>,
}

impl<N> Default for TarjanScc<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<N> TarjanScc<N> {
    /// Creates a new `TarjanScc`
    pub fn new() -> Self {
        TarjanScc {
            index: 1,                   // Invariant: index < componentcount at all times.
            componentcount: usize::MAX, // Will hold if componentcount is initialized to number of nodes - 1 or higher.
            nodes: Vec::new(),
            stack: Vec::new(),
        }
    }

    /// Compute the *strongly connected components* using Algorithm 3 in
    /// [A Space-Efficient Algorithm for Finding Strongly Connected Components][1] by David J. Pierce,
    /// which is a memory-efficient variation of [Tarjan's algorithm][2].
    ///
    ///
    /// [1]: https://homepages.ecs.vuw.ac.nz/~djp/files/P05.pdf
    /// [2]: https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    ///
    /// Calls `f` for each strongly connected component (scc).
    /// The order of node ids within each scc is arbitrary, but the order of
    /// the sccs is their postorder (reverse topological sort).
    ///
    /// For an undirected graph, the sccs are simply the connected components.
    ///
    /// This implementation is recursive and does one pass over the nodes.
    pub fn run<G, F>(&mut self, g: G, mut f: F)
    where
        G: IntoNodeIdentifiers<NodeId = N> + IntoNeighbors<NodeId = N> + NodeIndexable<NodeId = N>,
        F: FnMut(&[N]),
        N: Copy + PartialEq,
    {
        self.nodes.clear();
        self.nodes
            .resize(g.node_bound(), NodeData { rootindex: None });

        for n in g.node_identifiers() {
            let visited = self.nodes[g.to_index(n)].rootindex.is_some();
            if !visited {
                self.visit(n, g, &mut f);
            }
        }

        debug_assert!(self.stack.is_empty());
    }

    fn visit<G, F>(&mut self, v: G::NodeId, g: G, f: &mut F)
    where
        G: IntoNeighbors<NodeId = N> + NodeIndexable<NodeId = N>,
        F: FnMut(&[N]),
        N: Copy + PartialEq,
    {
        macro_rules! node {
            ($node:expr) => {
                self.nodes[g.to_index($node)]
            };
        }

        let node_v = &mut node![v];
        debug_assert!(node_v.rootindex.is_none());

        let mut v_is_local_root = true;
        let v_index = self.index;
        node_v.rootindex = NonZeroUsize::new(v_index);
        self.index += 1;

        for w in g.neighbors(v) {
            if node![w].rootindex.is_none() {
                self.visit(w, g, f);
            }
            if node![w].rootindex < node![v].rootindex {
                node![v].rootindex = node![w].rootindex;
                v_is_local_root = false
            }
        }

        if v_is_local_root {
            // Pop the stack and generate an SCC.
            let mut indexadjustment = 1;
            let c = NonZeroUsize::new(self.componentcount);
            let nodes = &mut self.nodes;
            let start = self
                .stack
                .iter()
                .rposition(|&w| {
                    if nodes[g.to_index(v)].rootindex > nodes[g.to_index(w)].rootindex {
                        true
                    } else {
                        nodes[g.to_index(w)].rootindex = c;
                        indexadjustment += 1;
                        false
                    }
                })
                .map(|x| x + 1)
                .unwrap_or_default();
            nodes[g.to_index(v)].rootindex = c;
            self.stack.push(v); // Pushing the component root to the back right before getting rid of it is somewhat ugly, but it lets it be included in f.
            f(&self.stack[start..]);
            self.stack.truncate(start);
            self.index -= indexadjustment; // Backtrack index back to where it was before we ever encountered the component.
            self.componentcount -= 1;
        } else {
            self.stack.push(v); // Stack is filled up when backtracking, unlike in Tarjans original algorithm.
        }
    }

    /// Returns the index of the component in which v has been assigned. Allows for using self as a lookup table for an scc decomposition produced by self.run().
    pub fn node_component_index<G>(&self, g: G, v: N) -> usize
    where
        G: IntoNeighbors<NodeId = N> + NodeIndexable<NodeId = N>,
        N: Copy + PartialEq,
    {
        let rindex: usize = self.nodes[g.to_index(v)]
            .rootindex
            .map(NonZeroUsize::get)
            .unwrap_or(0); // Compiles to no-op.
        debug_assert!(
            rindex != 0,
            "Tried to get the component index of an unvisited node."
        );
        debug_assert!(
            rindex > self.componentcount,
            "Given node has been visited but not yet assigned to a component."
        );
        usize::MAX - rindex
    }
}

/// Compute the *strongly connected components* using [Tarjan's algorithm][1].
///
/// This implementation is recursive and does one pass over the nodes. It is based on
/// [A Space-Efficient Algorithm for Finding Strongly Connected Components][2] by David J. Pierce,
/// to provide a memory-efficient implementation of [Tarjan's algorithm][1].
///
/// # Arguments
/// * `g`: a directed or undirected graph.
///
/// # Returns
/// Return a vector where each element is a strongly connected component (scc).
/// The order of node ids within each scc is arbitrary, but the order of
/// the sccs is their postorder (reverse topological sort).
///
/// For an undirected graph, the sccs are simply the connected components.
///
/// # Complexity
/// * Time complexity: **O(|V| + |E|)**.
/// * Auxiliary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
/// # Examples
///
/// ```rust
/// use petgraph::Graph;
/// use petgraph::algo::tarjan_scc;
/// use petgraph::prelude::*;
///
/// let mut graph: Graph<char, (), Directed> = Graph::new();
/// let a = graph.add_node('A');
/// let b = graph.add_node('B');
/// let c = graph.add_node('C');
/// let d = graph.add_node('D');
/// let e = graph.add_node('E');
///
/// graph.extend_with_edges(&[
///     (a, b), (b, c), (c, a),  // SCC 1: A -> B -> C -> A
///     (b, d), (d, e),          // D and E form individual SCCs
/// ]);
///
/// // Graph structure:
/// // A --> B ---> D ---> E
/// // ↑    ↑
/// // └--- C
///
/// let sccs = tarjan_scc(&graph);
/// assert_eq!(sccs.len(), 3); // Three strongly connected components
///
/// // Find the sizes of each SCC
/// let mut scc_sizes: Vec<usize> = sccs.iter().map(|scc| scc.len()).collect();
/// scc_sizes.sort();
/// assert_eq!(scc_sizes, vec![1, 1, 3]); // Two single-node SCCs and one 3-node SCC
/// ```
///
/// Using `TarjanScc` for reusable computation:
///
/// ```rust
/// use petgraph::Graph;
/// use petgraph::algo::TarjanScc;
/// use petgraph::prelude::*;
///
/// let mut graph: Graph<u32, (), Directed> = Graph::new();
/// let n1 = graph.add_node(1);
/// let n2 = graph.add_node(2);
/// let n3 = graph.add_node(3);
/// let n4 = graph.add_node(4);
///
/// graph.extend_with_edges(&[
///     (n1, n2), (n2, n3), (n3, n1),  // Cycle: 1 -> 2 -> 3 -> 1
///     (n2, n4),                       // Branch to isolated node 4
/// ]);
///
/// let mut tarjan = TarjanScc::new();
/// let mut sccs = Vec::new();
///
/// tarjan.run(&graph, |scc| {
///     sccs.push(scc.to_vec());
/// });
///
/// assert_eq!(sccs.len(), 2); // Two SCCs: {1,2,3} and {4}
/// ```
///
/// For an undirected graph (SCCs are just connected components):
///
/// ```rust
/// use petgraph::Graph;
/// use petgraph::algo::tarjan_scc;
/// use petgraph::prelude::*;
///
/// let mut graph: Graph<i32, (), Undirected> = Graph::new_undirected();
/// let a = graph.add_node(1);
/// let b = graph.add_node(2);
/// let c = graph.add_node(3);
/// let d = graph.add_node(4);
///
/// graph.extend_with_edges(&[
///     (a, b), // Component 1: {1, 2}
///     (c, d), // Component 2: {3, 4}
/// ]);
///
/// let sccs = tarjan_scc(&graph);
/// assert_eq!(sccs.len(), 2); // Two connected components
///
/// for scc in &sccs {
///     assert_eq!(scc.len(), 2); // Each component has 2 nodes
/// }
/// ```
///
/// [1]: https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
/// [2]: https://www.researchgate.net/publication/283024636_A_space-efficient_algorithm_for_finding_strongly_connected_components
pub fn tarjan_scc<G>(g: G) -> Vec<Vec<G::NodeId>>
where
    G: IntoNodeIdentifiers + IntoNeighbors + NodeIndexable,
{
    let mut sccs = Vec::new();
    {
        let mut tarjan_scc = TarjanScc::new();
        tarjan_scc.run(g, |scc| sccs.push(scc.to_vec()));
    }
    sccs
}
