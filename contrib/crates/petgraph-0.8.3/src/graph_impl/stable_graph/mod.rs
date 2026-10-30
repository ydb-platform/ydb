//! `StableGraph` keeps indices stable across removals.
//!
//! Depends on `feature = "stable_graph"`.
//!

use alloc::vec;
use core::{
    cmp, fmt, iter,
    marker::PhantomData,
    mem::size_of,
    ops::{Index, IndexMut},
    slice,
};

use fixedbitset::FixedBitSet;

use super::{index_twice, Edge, Frozen, GraphError, Node, Pair, DIRECTIONS};
use crate::iter_format::{DebugMap, IterFormatExt, NoPretty};
use crate::iter_utils::IterUtilsExt;
use crate::visit::{self, EdgeIndexable, EdgeRef, IntoEdgeReferences, NodeIndexable};
use crate::{
    Directed, Direction, EdgeType, Graph, Incoming, IntoWeightedEdge, Outgoing, Undirected,
};

// reexport those things that are shared with Graph
#[doc(no_inline)]
pub use crate::graph::{
    edge_index, node_index, DefaultIx, EdgeIndex, GraphIndex, IndexType, NodeIndex,
};

#[cfg(feature = "serde-1")]
mod serialization;

/// `StableGraph<N, E, Ty, Ix>` is a graph datastructure using an adjacency
/// list representation.
///
/// The graph **does not invalidate** any unrelated node or edge indices when
/// items are removed.
///
/// `StableGraph` is parameterized over:
///
/// - Associated data `N` for nodes and `E` for edges, also called *weights*.
///   The associated data can be of arbitrary type.
/// - Edge type `Ty` that determines whether the graph edges are directed or undirected.
/// - Index type `Ix`, which determines the maximum size of the graph.
///
/// The graph uses **O(|V| + |E|)** space where V is the set of nodes and E is the
/// set of edges, and allows fast node and edge insert and efficient graph search.
///
/// It implements **O(e')** edge lookup and edge and node removals, where **e'**
/// is some local measure of edge count.
///
/// - Nodes and edges are each numbered in an interval from *0* to some number
///   *m*, but *not all* indices in the range are valid, since gaps are formed
///   by deletions.
///
/// - You can select graph index integer type after the size of the graph. A smaller
///   size may have better performance.
///
/// - Using indices allows mutation while traversing the graph, see `Dfs`.
///
/// - The `StableGraph` is a regular rust collection and is `Send` and `Sync`
///   (as long as associated data `N` and `E` are).
///
/// - Indices don't allow as much compile time checking as references.
///
/// Depends on crate feature `stable_graph` (default). *Stable Graph is still
/// missing a few methods compared to Graph. You can contribute to help it
/// achieve parity.*
pub struct StableGraph<N, E, Ty = Directed, Ix = DefaultIx> {
    g: Graph<Option<N>, Option<E>, Ty, Ix>,
    node_count: usize,
    edge_count: usize,

    // node and edge free lists (both work the same way)
    //
    // free_node, if not NodeIndex::end(), points to a node index
    // that is vacant (after a deletion).
    // The free nodes form a doubly linked list using the fields Node.next[0]
    // for forward references and Node.next[1] for backwards ones.
    // The nodes are stored as EdgeIndex, and the _into_edge()/_into_node()
    // methods convert.
    // free_edge, if not EdgeIndex::end(), points to a free edge.
    // The edges only form a singly linked list using Edge.next[0] to store
    // the forward reference.
    free_node: NodeIndex<Ix>,
    free_edge: EdgeIndex<Ix>,
}

/// A `StableGraph` with directed edges.
///
/// For example, an edge from *1* to *2* is distinct from an edge from *2* to
/// *1*.
pub type StableDiGraph<N, E, Ix = DefaultIx> = StableGraph<N, E, Directed, Ix>;

/// A `StableGraph` with undirected edges.
///
/// For example, an edge between *1* and *2* is equivalent to an edge between
/// *2* and *1*.
pub type StableUnGraph<N, E, Ix = DefaultIx> = StableGraph<N, E, Undirected, Ix>;

impl<N, E, Ty, Ix> fmt::Debug for StableGraph<N, E, Ty, Ix>
where
    N: fmt::Debug,
    E: fmt::Debug,
    Ty: EdgeType,
    Ix: IndexType,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let etype = if self.is_directed() {
            "Directed"
        } else {
            "Undirected"
        };
        let mut fmt_struct = f.debug_struct("StableGraph");
        fmt_struct.field("Ty", &etype);
        fmt_struct.field("node_count", &self.node_count);
        fmt_struct.field("edge_count", &self.edge_count);
        if self.g.edges.iter().any(|e| e.weight.is_some()) {
            fmt_struct.field(
                "edges",
                &self
                    .g
                    .edges
                    .iter()
                    .filter(|e| e.weight.is_some())
                    .map(|e| NoPretty((e.source().index(), e.target().index())))
                    .format(", "),
            );
        }
        // skip weights if they are ZST!
        if size_of::<N>() != 0 {
            fmt_struct.field(
                "node weights",
                &DebugMap(|| {
                    self.g
                        .nodes
                        .iter()
                        .map(|n| n.weight.as_ref())
                        .enumerate()
                        .filter_map(|(i, wo)| wo.map(move |w| (i, w)))
                }),
            );
        }
        if size_of::<E>() != 0 {
            fmt_struct.field(
                "edge weights",
                &DebugMap(|| {
                    self.g
                        .edges
                        .iter()
                        .map(|n| n.weight.as_ref())
                        .enumerate()
                        .filter_map(|(i, wo)| wo.map(move |w| (i, w)))
                }),
            );
        }
        fmt_struct.field("free_node", &self.free_node);
        fmt_struct.field("free_edge", &self.free_edge);
        fmt_struct.finish()
    }
}

#[derive(Debug)]
pub struct StableGraphNode<N, Ix> {
    pub index: NodeIndex<Ix>,
    pub weight: N,
}

impl<N, Ix> Clone for StableGraphNode<N, Ix>
where
    N: Clone,
    Ix: Copy,
{
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            weight: self.weight.clone(),
        }
    }
}

#[derive(Debug)]
pub struct StableGraphEdge<E, Ix> {
    pub index: EdgeIndex<Ix>,
    pub source: NodeIndex<Ix>,
    pub target: NodeIndex<Ix>,
    pub weight: E,
}

impl<E, Ix> Clone for StableGraphEdge<E, Ix>
where
    E: Clone,
    Ix: Copy,
{
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            source: self.source,
            target: self.target,
            weight: self.weight.clone(),
        }
    }
}

impl<N, E> StableGraph<N, E, Directed> {
    /// Create a new `StableGraph` with directed edges.
    ///
    /// This is a convenience method. See `StableGraph::with_capacity`
    /// or `StableGraph::default` for a constructor that is generic in all the
    /// type parameters of `StableGraph`.
    pub fn new() -> Self {
        Self::with_capacity(0, 0)
    }
}

impl<N, E, Ty, Ix> StableGraph<N, E, Ty, Ix>
where
    Ix: IndexType,
{
    /// Create a new `StableGraph` with estimated capacity.
    pub fn with_capacity(nodes: usize, edges: usize) -> Self {
        StableGraph {
            g: Graph::with_capacity(nodes, edges),
            node_count: 0,
            edge_count: 0,
            free_node: NodeIndex::end(),
            free_edge: EdgeIndex::end(),
        }
    }
}

impl<N, E, Ty, Ix> StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    /// Return the current node and edge capacity of the graph.
    pub fn capacity(&self) -> (usize, usize) {
        self.g.capacity()
    }

    /// Reverse the direction of all edges
    pub fn reverse(&mut self) {
        // swap edge endpoints,
        // edge incoming / outgoing lists,
        // node incoming / outgoing lists
        for edge in &mut self.g.edges {
            if edge.weight.is_some() {
                edge.node.swap(0, 1);
                edge.next.swap(0, 1);
            }
        }
        for node in &mut self.g.nodes {
            if node.weight.is_some() {
                node.next.swap(0, 1);
            }
        }
    }

    /// Remove all nodes and edges
    pub fn clear(&mut self) {
        self.node_count = 0;
        self.edge_count = 0;
        self.free_node = NodeIndex::end();
        self.free_edge = EdgeIndex::end();
        self.g.clear();
    }

    /// Remove all edges
    pub fn clear_edges(&mut self) {
        self.edge_count = 0;
        self.free_edge = EdgeIndex::end();
        self.g.edges.clear();
        // clear edges without touching the free list
        for node in &mut self.g.nodes {
            if node.weight.is_some() {
                node.next = [EdgeIndex::end(), EdgeIndex::end()];
            }
        }
    }

    /// Return the number of nodes (also called vertices) in the graph.
    ///
    /// Computes in **O(1)** time.
    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// Return the number of edges in the graph.
    ///
    /// Computes in **O(1)** time.
    pub fn edge_count(&self) -> usize {
        self.edge_count
    }

    /// Reserves capacity for at least additional more nodes to be inserted in
    /// the graph. Graph may reserve more space to avoid frequent reallocations.
    ///
    /// Panics if the new capacity overflows usize.
    #[track_caller]
    pub fn reserve_nodes(&mut self, additional: usize) {
        self.g.reserve_nodes(additional);
    }

    /// Reserves capacity for at least additional more edges to be inserted in
    /// the graph. Graph may reserve more space to avoid frequent reallocations.
    ///
    /// Panics if the new capacity overflows usize.
    #[track_caller]
    pub fn reserve_edges(&mut self, additional: usize) {
        self.g.reserve_edges(additional);
    }

    /// Reserves the minimum capacity for exactly additional more nodes to be
    /// inserted in the graph. Does nothing if the capacity is already
    /// sufficient.
    ///
    /// Prefer reserve_nodes if future insertions are expected.
    ///
    /// Panics if the new capacity overflows usize.
    #[track_caller]
    pub fn reserve_exact_nodes(&mut self, additional: usize) {
        self.g.reserve_exact_nodes(additional);
    }

    /// Reserves the minimum capacity for exactly additional more edges to be
    /// inserted in the graph.
    /// Does nothing if the capacity is already sufficient.
    ///
    /// Prefer reserve_edges if future insertions are expected.
    ///
    /// Panics if the new capacity overflows usize.
    #[track_caller]
    pub fn reserve_exact_edges(&mut self, additional: usize) {
        self.g.reserve_exact_edges(additional);
    }

    /// Shrinks the capacity of the underlying nodes collection as much as possible.
    pub fn shrink_to_fit_nodes(&mut self) {
        self.g.shrink_to_fit_nodes();
    }

    /// Shrinks the capacity of the underlying edges collection as much as possible.
    pub fn shrink_to_fit_edges(&mut self) {
        self.g.shrink_to_fit_edges();
    }

    /// Shrinks the capacity of the graph as much as possible.
    pub fn shrink_to_fit(&mut self) {
        self.g.shrink_to_fit();
    }

    /// Whether the graph has directed edges or not.
    #[inline]
    pub fn is_directed(&self) -> bool {
        Ty::is_directed()
    }

    /// Add a node (also called vertex) with associated data `weight` to the graph.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Return the index of the new node.
    ///
    /// **Panics** if the `StableGraph` is at the maximum number of nodes for
    /// its index type.
    #[track_caller]
    pub fn add_node(&mut self, weight: N) -> NodeIndex<Ix> {
        self.try_add_node(weight).unwrap()
    }

    /// Add a node (also called vertex) with associated data `weight` to the graph.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Return the index of the new node.
    ///
    /// Return [`GraphError::NodeIxLimit`] if the `StableGraph` is at the maximum number of nodes for its index.
    pub fn try_add_node(&mut self, weight: N) -> Result<NodeIndex<Ix>, GraphError> {
        if self.free_node != NodeIndex::end() {
            let node_idx = self.free_node;
            self.occupy_vacant_node(node_idx, weight);
            Ok(node_idx)
        } else {
            self.node_count += 1;
            self.g.try_add_node(Some(weight))
        }
    }

    /// free_node: Which free list to update for the vacancy
    fn add_vacant_node(&mut self, free_node: &mut NodeIndex<Ix>) {
        let node_idx = self.g.add_node(None);
        // link the free list
        let node_slot = &mut self.g.nodes[node_idx.index()];
        node_slot.next = [free_node._into_edge(), EdgeIndex::end()];
        if *free_node != NodeIndex::end() {
            self.g.nodes[free_node.index()].next[1] = node_idx._into_edge();
        }
        *free_node = node_idx;
    }

    /// Remove `a` from the graph if it exists, and return its weight.
    /// If it doesn't exist in the graph, return `None`.
    ///
    /// The node index `a` is invalidated, but none other.
    /// Edge indices are invalidated as they would be following the removal of
    /// each edge with an endpoint in `a`.
    ///
    /// Computes in **O(e')** time, where **e'** is the number of affected
    /// edges, including *n* calls to `.remove_edge()` where *n* is the number
    /// of edges with an endpoint in `a`.
    pub fn remove_node(&mut self, a: NodeIndex<Ix>) -> Option<N> {
        let node_weight = self.g.nodes.get_mut(a.index())?.weight.take()?;
        for d in &DIRECTIONS {
            let k = d.index();

            // Remove all edges from and to this node.
            loop {
                let next = self.g.nodes[a.index()].next[k];
                if next == EdgeIndex::end() {
                    break;
                }
                let ret = self.remove_edge(next);
                debug_assert!(ret.is_some());
                let _ = ret;
            }
        }

        let node_slot = &mut self.g.nodes[a.index()];
        //let node_weight = replace(&mut self.g.nodes[a.index()].weight, Entry::Empty(self.free_node));
        //self.g.nodes[a.index()].next = [EdgeIndex::end(), EdgeIndex::end()];
        node_slot.next = [self.free_node._into_edge(), EdgeIndex::end()];
        if self.free_node != NodeIndex::end() {
            self.g.nodes[self.free_node.index()].next[1] = a._into_edge();
        }
        self.free_node = a;
        self.node_count -= 1;

        Some(node_weight)
    }

    pub fn contains_node(&self, a: NodeIndex<Ix>) -> bool {
        self.get_node(a).is_some()
    }

    // Return the Node if it is not vacant (non-None weight)
    fn get_node(&self, a: NodeIndex<Ix>) -> Option<&Node<Option<N>, Ix>> {
        self.g
            .nodes
            .get(a.index())
            .and_then(|node| node.weight.as_ref().map(move |_| node))
    }

    /// Add an edge from `a` to `b` to the graph, with its associated
    /// data `weight`.
    ///
    /// Return the index of the new edge.
    ///
    /// Computes in **O(1)** time.
    ///
    /// **Panics** if any of the nodes don't exist.<br>
    /// **Panics** if the `StableGraph` is at the maximum number of edges for
    /// its index type.
    ///
    /// **Note:** `StableGraph` allows adding parallel (“duplicate”) edges.
    #[track_caller]
    pub fn add_edge(&mut self, a: NodeIndex<Ix>, b: NodeIndex<Ix>, weight: E) -> EdgeIndex<Ix> {
        let res = self.try_add_edge(a, b, weight);
        if let Err(GraphError::NodeMissed(i)) = res {
            panic!(
                "StableGraph::add_edge: node index {} is not a node in the graph",
                i
            );
        }
        res.unwrap()
    }

    /// Try to add an edge from `a` to `b` to the graph, with its associated
    /// data `weight`.
    ///
    /// Return the index of the new edge.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Possible errors:
    /// - [`GraphError::NodeMissed`] - if any of the nodes don't exist.<br>
    /// - [`GraphError::EdgeIxLimit`] if the `StableGraph` is at the maximum number of edges for its index
    ///   type (N/A if usize).
    ///
    /// **Note:** `StableGraph` allows adding parallel (“duplicate”) edges.
    pub fn try_add_edge(
        &mut self,
        a: NodeIndex<Ix>,
        b: NodeIndex<Ix>,
        weight: E,
    ) -> Result<EdgeIndex<Ix>, GraphError> {
        let edge_idx;
        let mut new_edge = None::<Edge<_, _>>;
        {
            let edge: &mut Edge<_, _>;

            if self.free_edge != EdgeIndex::end() {
                edge_idx = self.free_edge;
                edge = &mut self.g.edges[edge_idx.index()];
                let _old = edge.weight.replace(weight);
                debug_assert!(_old.is_none());
                self.free_edge = edge.next[0];
                edge.node = [a, b];
            } else {
                edge_idx = EdgeIndex::new(self.g.edges.len());
                if !(<Ix as IndexType>::max().index() == !0 || EdgeIndex::end() != edge_idx) {
                    return Err(GraphError::EdgeIxLimit);
                }
                new_edge = Some(Edge {
                    weight: Some(weight),
                    node: [a, b],
                    next: [EdgeIndex::end(); 2],
                });
                edge = new_edge.as_mut().unwrap();
            }

            let wrong_index = match index_twice(&mut self.g.nodes, a.index(), b.index()) {
                Pair::None => Some(cmp::max(a.index(), b.index())),
                Pair::One(an) => {
                    if an.weight.is_none() {
                        Some(a.index())
                    } else {
                        edge.next = an.next;
                        an.next[0] = edge_idx;
                        an.next[1] = edge_idx;
                        None
                    }
                }
                Pair::Both(an, bn) => {
                    // a and b are different indices
                    if an.weight.is_none() {
                        Some(a.index())
                    } else if bn.weight.is_none() {
                        Some(b.index())
                    } else {
                        edge.next = [an.next[0], bn.next[1]];
                        an.next[0] = edge_idx;
                        bn.next[1] = edge_idx;
                        None
                    }
                }
            };
            if let Some(i) = wrong_index {
                return Err(GraphError::NodeMissed(i));
            }
            self.edge_count += 1;
        }
        if let Some(edge) = new_edge {
            self.g.edges.push(edge);
        }
        Ok(edge_idx)
    }

    /// free_edge: Which free list to update for the vacancy
    fn add_vacant_edge(&mut self, free_edge: &mut EdgeIndex<Ix>) {
        let edge_idx = EdgeIndex::new(self.g.edges.len());
        debug_assert!(edge_idx != EdgeIndex::end());
        let mut edge = Edge {
            weight: None,
            node: [NodeIndex::end(); 2],
            next: [EdgeIndex::end(); 2],
        };
        edge.next[0] = *free_edge;
        *free_edge = edge_idx;
        self.g.edges.push(edge);
    }

    /// Add or update an edge from `a` to `b`.
    /// If the edge already exists, its weight is updated.
    ///
    /// Return the index of the affected edge.
    ///
    /// Computes in **O(e')** time, where **e'** is the number of edges
    /// connected to `a` (and `b`, if the graph edges are undirected).
    ///
    /// **Panics** if any of the nodes don't exist
    /// or the stable graph is at the maximum number of edges for its index (when adding new edge).
    #[track_caller]
    pub fn update_edge(&mut self, a: NodeIndex<Ix>, b: NodeIndex<Ix>, weight: E) -> EdgeIndex<Ix> {
        self.try_update_edge(a, b, weight).unwrap()
    }

    /// Try to add or update an edge from `a` to `b`.
    /// If the edge already exists, its weight is updated.
    ///
    /// Return the index of the affected edge.
    ///
    /// Computes in **O(e')** time, where **e'** is the number of edges
    /// connected to `a` (and `b`, if the graph edges are undirected).
    ///
    /// Possible errors:
    /// - [`GraphError::NodeMissed`] - if any of the nodes don't exist.<br>
    /// - [`GraphError::EdgeIxLimit`] if the `StableGraph` is at the maximum number of edges for its index
    ///   type (N/A if usize).
    pub fn try_update_edge(
        &mut self,
        a: NodeIndex<Ix>,
        b: NodeIndex<Ix>,
        weight: E,
    ) -> Result<EdgeIndex<Ix>, GraphError> {
        if let Some(ix) = self.find_edge(a, b) {
            self[ix] = weight;
            return Ok(ix);
        }
        self.try_add_edge(a, b, weight)
    }

    /// Remove an edge and return its edge weight, or `None` if it didn't exist.
    ///
    /// Invalidates the edge index `e` but no other.
    ///
    /// Computes in **O(e')** time, where **e'** is the number of edges
    /// connected to the same endpoints as `e`.
    pub fn remove_edge(&mut self, e: EdgeIndex<Ix>) -> Option<E> {
        // every edge is part of two lists,
        // outgoing and incoming edges.
        // Remove it from both
        let (is_edge, edge_node, edge_next) = match self.g.edges.get(e.index()) {
            None => return None,
            Some(x) => (x.weight.is_some(), x.node, x.next),
        };
        if !is_edge {
            return None;
        }

        // Remove the edge from its in and out lists by replacing it with
        // a link to the next in the list.
        self.g.change_edge_links(edge_node, e, edge_next);

        // Clear the edge and put it in the free list
        let edge = &mut self.g.edges[e.index()];
        edge.next = [self.free_edge, EdgeIndex::end()];
        edge.node = [NodeIndex::end(), NodeIndex::end()];
        self.free_edge = e;
        self.edge_count -= 1;
        edge.weight.take()
    }

    /// Access the weight for node `a`.
    ///
    /// Also available with indexing syntax: `&graph[a]`.
    pub fn node_weight(&self, a: NodeIndex<Ix>) -> Option<&N> {
        match self.g.nodes.get(a.index()) {
            Some(no) => no.weight.as_ref(),
            None => None,
        }
    }

    /// Access the weight for node `a`, mutably.
    ///
    /// Also available with indexing syntax: `&mut graph[a]`.
    pub fn node_weight_mut(&mut self, a: NodeIndex<Ix>) -> Option<&mut N> {
        match self.g.nodes.get_mut(a.index()) {
            Some(no) => no.weight.as_mut(),
            None => None,
        }
    }

    /// Return an iterator yielding immutable access to all node weights.
    ///
    /// The order in which weights are yielded matches the order of their node
    /// indices.
    pub fn node_weights(&self) -> impl Iterator<Item = &N> {
        self.g
            .node_weights()
            .filter_map(|maybe_node| maybe_node.as_ref())
    }
    /// Return an iterator yielding mutable access to all node weights.
    ///
    /// The order in which weights are yielded matches the order of their node
    /// indices.
    pub fn node_weights_mut(&mut self) -> impl Iterator<Item = &mut N> {
        self.g
            .node_weights_mut()
            .filter_map(|maybe_node| maybe_node.as_mut())
    }

    /// Return an iterator over the node indices of the graph
    pub fn node_indices(&self) -> NodeIndices<'_, N, Ix> {
        NodeIndices {
            iter: self.raw_nodes().iter().enumerate(),
        }
    }

    /// Access the weight for edge `e`.
    ///
    /// Also available with indexing syntax: `&graph[e]`.
    pub fn edge_weight(&self, e: EdgeIndex<Ix>) -> Option<&E> {
        match self.g.edges.get(e.index()) {
            Some(ed) => ed.weight.as_ref(),
            None => None,
        }
    }

    /// Access the weight for edge `e`, mutably
    ///
    /// Also available with indexing syntax: `&mut graph[e]`.
    pub fn edge_weight_mut(&mut self, e: EdgeIndex<Ix>) -> Option<&mut E> {
        match self.g.edges.get_mut(e.index()) {
            Some(ed) => ed.weight.as_mut(),
            None => None,
        }
    }

    /// Return an iterator yielding immutable access to all edge weights.
    ///
    /// The order in which weights are yielded matches the order of their edge
    /// indices.
    pub fn edge_weights(&self) -> impl Iterator<Item = &E> {
        self.g
            .edge_weights()
            .filter_map(|maybe_edge| maybe_edge.as_ref())
    }
    /// Return an iterator yielding mutable access to all edge weights.
    ///
    /// The order in which weights are yielded matches the order of their edge
    /// indices.
    pub fn edge_weights_mut(&mut self) -> impl Iterator<Item = &mut E> {
        self.g
            .edge_weights_mut()
            .filter_map(|maybe_edge| maybe_edge.as_mut())
    }

    /// Access the source and target nodes for `e`.
    pub fn edge_endpoints(&self, e: EdgeIndex<Ix>) -> Option<(NodeIndex<Ix>, NodeIndex<Ix>)> {
        match self.g.edges.get(e.index()) {
            Some(ed) if ed.weight.is_some() => Some((ed.source(), ed.target())),
            _otherwise => None,
        }
    }

    /// Return an iterator over the edge indices of the graph.
    ///
    /// Note: the iterator borrows a graph in contrast to the behavior of
    /// [`Graph::edge_indices`](fn@crate::Graph::edge_indices).
    pub fn edge_indices(&self) -> EdgeIndices<'_, E, Ix> {
        EdgeIndices {
            iter: self.raw_edges().iter().enumerate(),
        }
    }

    /// Return an iterator over all the edges connecting `a` and `b`.
    ///
    /// - `Directed`: Outgoing edges from `a`.
    /// - `Undirected`: All edges connected to `a`.
    ///
    /// Iterator element type is `EdgeReference<E, Ix>`.
    pub fn edges_connecting(
        &self,
        a: NodeIndex<Ix>,
        b: NodeIndex<Ix>,
    ) -> EdgesConnecting<'_, E, Ty, Ix> {
        EdgesConnecting {
            target_node: b,
            edges: self.edges_directed(a, Outgoing),
            ty: PhantomData,
        }
    }

    /// Lookup if there is an edge from `a` to `b`.
    ///
    /// Computes in **O(e')** time, where **e'** is the number of edges
    /// connected to `a` (and `b`, if the graph edges are undirected).
    pub fn contains_edge(&self, a: NodeIndex<Ix>, b: NodeIndex<Ix>) -> bool {
        self.find_edge(a, b).is_some()
    }

    /// Lookup an edge from `a` to `b`.
    ///
    /// Computes in **O(e')** time, where **e'** is the number of edges
    /// connected to `a` (and `b`, if the graph edges are undirected).
    pub fn find_edge(&self, a: NodeIndex<Ix>, b: NodeIndex<Ix>) -> Option<EdgeIndex<Ix>> {
        if !self.is_directed() {
            self.find_edge_undirected(a, b).map(|(ix, _)| ix)
        } else {
            match self.get_node(a) {
                None => None,
                Some(node) => self.g.find_edge_directed_from_node(node, b),
            }
        }
    }

    /// Lookup an edge between `a` and `b`, in either direction.
    ///
    /// If the graph is undirected, then this is equivalent to `.find_edge()`.
    ///
    /// Return the edge index and its directionality, with `Outgoing` meaning
    /// from `a` to `b` and `Incoming` the reverse,
    /// or `None` if the edge does not exist.
    pub fn find_edge_undirected(
        &self,
        a: NodeIndex<Ix>,
        b: NodeIndex<Ix>,
    ) -> Option<(EdgeIndex<Ix>, Direction)> {
        match self.get_node(a) {
            None => None,
            Some(node) => self.g.find_edge_undirected_from_node(node, b),
        }
    }

    /// Return an iterator of all nodes with an edge starting from `a`.
    ///
    /// - `Directed`: Outgoing edges from `a`.
    /// - `Undirected`: All edges connected to `a`.
    ///
    /// Produces an empty iterator if the node doesn't exist.<br>
    /// Iterator element type is `NodeIndex<Ix>`.
    ///
    /// For the iteration order for `Directed` and `Undirected` graphs respectively,
    /// please refer to the documentation of [`Graph::neighbors_directed`].
    ///
    /// Use [`.neighbors(a).detach()`][1] to get a neighbor walker that does
    /// not borrow from the graph.
    ///
    /// [1]: struct.Neighbors.html#method.detach
    pub fn neighbors(&self, a: NodeIndex<Ix>) -> Neighbors<'_, E, Ix> {
        self.neighbors_directed(a, Outgoing)
    }

    /// Return an iterator of all neighbors that have an edge between them and `a`,
    /// in the specified direction.
    /// If the graph's edges are undirected, this is equivalent to *.neighbors(a)*.
    ///
    /// - `Directed`, `Outgoing`: All edges from `a`.
    /// - `Directed`, `Incoming`: All edges to `a`.
    /// - `Undirected`: All edges connected to `a`.
    ///
    /// Produces an empty iterator if the node doesn't exist.<br>
    /// Iterator element type is `NodeIndex<Ix>`.
    ///
    /// For a `Directed` graph, neighbors are listed in reverse order of their
    /// addition to the graph, so the most recently added edge's neighbor is
    /// listed first.
    ///
    /// For the ordering in case of an `Undirected` graph, please refer to
    /// the documentation of [`Graph::neighbors_undirected`].
    ///
    /// Use [`.neighbors_directed(a, dir).detach()`][1] to get a neighbor walker that does
    /// not borrow from the graph.
    ///
    /// [1]: struct.Neighbors.html#method.detach
    pub fn neighbors_directed(&self, a: NodeIndex<Ix>, dir: Direction) -> Neighbors<'_, E, Ix> {
        let mut iter = self.neighbors_undirected(a);
        if self.is_directed() {
            let k = dir.index();
            iter.next[1 - k] = EdgeIndex::end();
            iter.skip_start = NodeIndex::end();
        }
        iter
    }

    /// Return an iterator of all neighbors that have an edge between them and `a`,
    /// in either direction.
    /// If the graph's edges are undirected, this is equivalent to *.neighbors(a)*.
    ///
    /// - `Directed` and `Undirected`: All edges connected to `a`.
    ///
    /// Produces an empty iterator if the node doesn't exist.<br>
    /// Iterator element type is `NodeIndex<Ix>`.
    ///
    /// All outgoing neighbors are listed first followed by all incoming neighbors.
    /// The ordering among the outgoing and incoming neighbors respectively is the
    /// reverse order of their addition to the graph. That is, the most recently
    /// added edge's neighbor is listed first. Outgoing and incoming in this case
    /// refer to the ordering in which the endpoints were listed when adding the
    /// edge (`g.add_edge(a, b, w)` or `g.add_edge(b, a, w)`).
    ///
    /// Use [`.neighbors_undirected(a).detach()`][1] to get a neighbor walker that does
    /// not borrow from the graph.
    ///
    /// [1]: struct.Neighbors.html#method.detach
    pub fn neighbors_undirected(&self, a: NodeIndex<Ix>) -> Neighbors<'_, E, Ix> {
        Neighbors {
            skip_start: a,
            edges: &self.g.edges,
            next: match self.get_node(a) {
                None => [EdgeIndex::end(), EdgeIndex::end()],
                Some(n) => n.next,
            },
        }
    }

    /// Return an iterator of all edges of `a`.
    ///
    /// - `Directed`: Outgoing edges from `a`.
    /// - `Undirected`: All edges connected to `a`.
    ///
    /// Produces an empty iterator if the node doesn't exist.<br>
    /// Iterator element type is `EdgeReference<E, Ix>`.
    ///
    /// For a `Directed` graph, edges are listed in reverse order of their
    /// addition to the graph, so the most recently added edge is listed first.
    ///
    /// For the ordering in case of an `Undirected` graph, please refer to
    /// the `Undirected` case in the documentation of [`Graph::edges_directed`].
    pub fn edges(&self, a: NodeIndex<Ix>) -> Edges<'_, E, Ty, Ix> {
        self.edges_directed(a, Outgoing)
    }

    /// Return an iterator of all edges of `a`, in the specified direction.
    ///
    /// - `Directed`, `Outgoing`: All edges from `a`.
    /// - `Directed`, `Incoming`: All edges to `a`.
    /// - `Undirected`, `Outgoing`: All edges connected to `a`, with `a` being the source of each
    ///   edge.
    /// - `Undirected`, `Incoming`: All edges connected to `a`, with `a` being the target of each
    ///   edge.
    ///
    /// Produces an empty iterator if the node `a` doesn't exist.<br>
    /// Iterator element type is `EdgeReference<E, Ix>`.
    ///
    /// For a `Directed` graph, edges are listed in reverse order of their
    /// addition to the graph, so the most recently added edge is listed first.
    ///
    /// For an `Undirected` graph, the outgoing edges are listed first, then
    /// all incoming edges. The ordering among the outgoing and incoming edges
    /// respectively is the reverse order of their addition to the graph,
    /// similar to the `Directed` case. Outgoing and incoming in this case
    /// refer to the ordering in which the endpoints were listed when adding the
    /// edge (`g.add_edge(a, b, w)` or `g.add_edge(b, a, w)`).
    pub fn edges_directed(&self, a: NodeIndex<Ix>, dir: Direction) -> Edges<'_, E, Ty, Ix> {
        Edges {
            skip_start: a,
            edges: &self.g.edges,
            direction: dir,
            next: match self.get_node(a) {
                None => [EdgeIndex::end(), EdgeIndex::end()],
                Some(n) => n.next,
            },
            ty: PhantomData,
        }
    }

    /// Return an iterator over either the nodes without edges to them
    /// (`Incoming`) or from them (`Outgoing`).
    ///
    /// An *internal* node has both incoming and outgoing edges.
    /// The nodes in `.externals(Incoming)` are the source nodes and
    /// `.externals(Outgoing)` are the sinks of the graph.
    ///
    /// For a graph with undirected edges, both the sinks and the sources are
    /// just the nodes without edges.
    ///
    /// The whole iteration computes in **O(|V|)** time where V is the set of nodes.
    pub fn externals(&self, dir: Direction) -> Externals<'_, N, Ty, Ix> {
        Externals {
            iter: self.raw_nodes().iter().enumerate(),
            dir,
            ty: PhantomData,
        }
    }

    /// Convert the `StableGraph` into iterators of Nodes and Edges
    pub fn into_nodes_edges_iters(
        self,
    ) -> (
        impl Iterator<Item = StableGraphNode<N, Ix>>,
        impl Iterator<Item = StableGraphEdge<E, Ix>>,
    ) {
        let (inner_nodes, inner_edges) = self.g.into_nodes_edges();

        (
            inner_nodes
                .into_iter()
                .enumerate()
                .filter(|(_, node)| node.weight.is_some())
                .map(|(index, node)| StableGraphNode {
                    index: NodeIndex::new(index),
                    weight: node.weight.unwrap(),
                }),
            inner_edges
                .into_iter()
                .enumerate()
                .filter(|(_, edge)| edge.weight.is_some())
                .map(|(index, edge)| StableGraphEdge {
                    index: EdgeIndex::new(index),
                    source: edge.source(),
                    target: edge.target(),
                    weight: edge.weight.unwrap(),
                }),
        )
    }

    /// Index the `StableGraph` by two indices, any combination of
    /// node or edge indices is fine.
    ///
    /// **Panics** if the indices are equal or if they are not found.
    #[track_caller]
    pub fn index_twice_mut<T, U>(
        &mut self,
        i: T,
        j: U,
    ) -> (
        &mut <Self as Index<T>>::Output,
        &mut <Self as Index<U>>::Output,
    )
    where
        Self: IndexMut<T> + IndexMut<U>,
        T: GraphIndex,
        U: GraphIndex,
    {
        assert!(T::is_node_index() != U::is_node_index() || i.index() != j.index());

        // Allow two mutable indexes here -- they are nonoverlapping
        unsafe {
            let self_mut = self as *mut _;
            (
                <Self as IndexMut<T>>::index_mut(&mut *self_mut, i),
                <Self as IndexMut<U>>::index_mut(&mut *self_mut, j),
            )
        }
    }

    /// Keep all nodes that return `true` from the `visit` closure,
    /// remove the others.
    ///
    /// `visit` is provided a proxy reference to the graph, so that
    /// the graph can be walked and associated data modified.
    ///
    /// The order nodes are visited is not specified.
    ///
    /// The node indices of the removed nodes are invalidated, but none other.
    /// Edge indices are invalidated as they would be following the removal of
    /// each edge with an endpoint in a removed node.
    ///
    /// Computes in **O(n + e')** time, where **n** is the number of node indices and
    ///  **e'** is the number of affected edges, including *n* calls to `.remove_edge()`
    /// where *n* is the number of edges with an endpoint in a removed node.
    pub fn retain_nodes<F>(&mut self, mut visit: F)
    where
        F: FnMut(Frozen<Self>, NodeIndex<Ix>) -> bool,
    {
        for i in 0..self.node_bound() {
            let ix = node_index(i);
            if self.contains_node(ix) && !visit(Frozen(self), ix) {
                self.remove_node(ix);
            }
        }
        self.check_free_lists();
    }

    /// Keep all edges that return `true` from the `visit` closure,
    /// remove the others.
    ///
    /// `visit` is provided a proxy reference to the graph, so that
    /// the graph can be walked and associated data modified.
    ///
    /// The order edges are visited is not specified.
    ///
    /// The edge indices of the removed edes are invalidated, but none other.
    ///
    /// Computes in **O(e'')** time, **e'** is the number of affected edges,
    /// including the calls to `.remove_edge()` for each removed edge.
    pub fn retain_edges<F>(&mut self, mut visit: F)
    where
        F: FnMut(Frozen<Self>, EdgeIndex<Ix>) -> bool,
    {
        for i in 0..self.edge_bound() {
            let ix = edge_index(i);
            if self.edge_weight(ix).is_some() && !visit(Frozen(self), ix) {
                self.remove_edge(ix);
            }
        }
        self.check_free_lists();
    }

    /// Create a new `StableGraph` from an iterable of edges.
    ///
    /// Node weights `N` are set to default values.
    /// Edge weights `E` may either be specified in the list,
    /// or they are filled with default values.
    ///
    /// Nodes are inserted automatically to match the edges.
    ///
    /// ```
    /// use petgraph::stable_graph::StableGraph;
    ///
    /// let gr = StableGraph::<(), i32>::from_edges(&[
    ///     (0, 1), (0, 2), (0, 3),
    ///     (1, 2), (1, 3),
    ///     (2, 3),
    /// ]);
    /// ```
    pub fn from_edges<I>(iterable: I) -> Self
    where
        I: IntoIterator,
        I::Item: IntoWeightedEdge<E>,
        <I::Item as IntoWeightedEdge<E>>::NodeId: Into<NodeIndex<Ix>>,
        N: Default,
    {
        let mut g = Self::with_capacity(0, 0);
        g.extend_with_edges(iterable);
        g
    }

    /// Create a new `StableGraph` by mapping node and
    /// edge weights to new values.
    ///
    /// The resulting graph has the same structure and the same
    /// graph indices as `self`.
    ///
    /// If you want a consuming version of this function, see [`map_owned`](struct.StableGraph.html#method.map_owned).
    ///
    /// ```
    /// use petgraph::stable_graph::StableGraph;
    ///
    /// // Create an undirected graph with city names as node data and their distances as edge data.
    /// let mut g = StableGraph::<String, u32>::new();
    ///
    /// let bie = g.add_node("Bielefeld".to_owned());
    /// let del = g.add_node("New Delhi".to_owned());
    /// let mex = g.add_node("Mexico City".to_owned());
    /// let syd = g.add_node("Sydney".to_owned());
    ///
    /// // Add distances in kilometers as edge data.
    /// g.extend_with_edges(&[
    ///     (bie, del, 6_000),
    ///     (bie, mex, 10_000),
    ///     (bie, syd, 16_000),
    ///     (del, mex, 14_000),
    ///     (del, syd, 12_000),
    ///     (mex, syd, 15_000),
    /// ]);
    ///
    /// // We might now want to change up the distances to be in miles instead and to be strings.
    /// // We can do this using the `map` method, which takes two closures for the node and edge data,
    /// // respectively, and returns a new graph with the transformed data.
    /// let g_miles: StableGraph<String, i32> = g.map(
    ///     |_, city| city.to_owned(),
    ///     |_, &distance| (distance as f64 * 0.621371).round() as i32,
    /// );
    ///
    /// for &edge_weight in g_miles.edge_weights() {
    ///     assert!(edge_weight < 10_000);
    /// }
    /// ```
    pub fn map<'a, F, G, N2, E2>(
        &'a self,
        mut node_map: F,
        mut edge_map: G,
    ) -> StableGraph<N2, E2, Ty, Ix>
    where
        F: FnMut(NodeIndex<Ix>, &'a N) -> N2,
        G: FnMut(EdgeIndex<Ix>, &'a E) -> E2,
    {
        let g = self.g.map(
            move |i, w| w.as_ref().map(|w| node_map(i, w)),
            move |i, w| w.as_ref().map(|w| edge_map(i, w)),
        );
        StableGraph {
            g,
            node_count: self.node_count,
            edge_count: self.edge_count,
            free_node: self.free_node,
            free_edge: self.free_edge,
        }
    }

    /// Create a new `StableGraph` by mapping node and
    /// edge weights to new values, consuming the current graph.
    ///
    /// The resulting graph has the same structure and the same
    /// graph indices as `self`.
    ///
    /// If you want a non-consuming version of this function, see [`map`](struct.StableGraph.html#method.map).
    ///
    /// ```
    /// use petgraph::stable_graph::StableGraph;
    ///
    /// // Create an undirected graph with city names as node data and their distances as edge data.
    /// let mut g = StableGraph::<String, u32>::new();
    ///
    /// let bie = g.add_node("Bielefeld".to_owned());
    /// let del = g.add_node("New Delhi".to_owned());
    /// let mex = g.add_node("Mexico City".to_owned());
    /// let syd = g.add_node("Sydney".to_owned());
    ///
    /// // Add distances in kilometers as edge data.
    /// g.extend_with_edges(&[
    ///     (bie, del, 6_000),
    ///     (bie, mex, 10_000),
    ///     (bie, syd, 16_000),
    ///     (del, mex, 14_000),
    ///     (del, syd, 12_000),
    ///     (mex, syd, 15_000),
    /// ]);
    ///
    /// // We might now want to change up the distances to be in miles instead and to be strings.
    /// // We can do this using the `map` method, which takes two closures for the node and edge data,
    /// // respectively, and returns a new graph with the transformed data.
    /// let g_miles: StableGraph<String, i32> = g.map_owned(
    ///     |_, city| city,
    ///     |_, distance| (distance as f64 * 0.621371).round() as i32,
    /// );
    ///
    /// for &edge_weight in g_miles.edge_weights() {
    ///     assert!(edge_weight < 10_000);
    /// }
    /// ```
    pub fn map_owned<F, G, N2, E2>(
        self,
        mut node_map: F,
        mut edge_map: G,
    ) -> StableGraph<N2, E2, Ty, Ix>
    where
        F: FnMut(NodeIndex<Ix>, N) -> N2,
        G: FnMut(EdgeIndex<Ix>, E) -> E2,
    {
        let g = self.g.map_owned(
            move |i, w| w.map(|w| node_map(i, w)),
            move |i, w| w.map(|w| edge_map(i, w)),
        );
        StableGraph {
            g,
            node_count: self.node_count,
            edge_count: self.edge_count,
            free_node: self.free_node,
            free_edge: self.free_edge,
        }
    }

    /// Create a new `StableGraph` by mapping nodes and edges.
    /// A node or edge may be mapped to `None` to exclude it from
    /// the resulting graph.
    ///
    /// Nodes are mapped first with the `node_map` closure, then
    /// `edge_map` is called for the edges that have not had any endpoint
    /// removed.
    ///
    /// The resulting graph has the structure of a subgraph of the original graph.
    /// Nodes and edges that are not removed maintain their old node or edge
    /// indices.
    ///
    /// If you want a consuming version of this function, see [`filter_map_owned`](struct.StableGraph.html#method.filter_map_owned).
    ///
    /// ```
    /// use petgraph::stable_graph::StableGraph;
    ///
    /// // Create a graph with integer node weights
    /// let mut g = StableGraph::<u32, ()>::new();
    /// let a = g.add_node(0);
    /// let b = g.add_node(2);
    /// let c = g.add_node(5);
    /// let d = g.add_node(7);
    /// let e = g.add_node(4);
    /// g.extend_with_edges(&[(a, b, ()), (a, c, ()), (b, d, ()), (c, d, ()), (d, e, ())]);
    ///
    /// // Filter the graph such that only nodes with weight greater than 2 are kept.
    /// let g_filtered = g.filter_map(
    ///     |_, &node_weight| if node_weight > 2 { Some(node_weight) } else { None },
    ///     |_, &edge_weight| Some(edge_weight),
    /// );
    ///
    /// assert_eq!(g_filtered.node_count(), 3);
    /// // Node and edge indices are preserved.
    /// assert_eq!(g_filtered.node_weight(c), Some(&5));
    /// ```
    pub fn filter_map<'a, F, G, N2, E2>(
        &'a self,
        mut node_map: F,
        mut edge_map: G,
    ) -> StableGraph<N2, E2, Ty, Ix>
    where
        F: FnMut(NodeIndex<Ix>, &'a N) -> Option<N2>,
        G: FnMut(EdgeIndex<Ix>, &'a E) -> Option<E2>,
    {
        let node_bound = self.node_bound();
        let edge_bound = self.edge_bound();
        let mut result_g = StableGraph::with_capacity(node_bound, edge_bound);
        // use separate free lists so that
        // add_node / add_edge below do not reuse the tombstones
        let mut free_node = NodeIndex::end();
        let mut free_edge = EdgeIndex::end();

        // the stable graph keeps the node map itself

        for (i, node) in self.raw_nodes().iter().enumerate() {
            if i >= node_bound {
                break;
            }
            if let Some(node_weight) = node.weight.as_ref() {
                if let Some(new_weight) = node_map(NodeIndex::new(i), node_weight) {
                    result_g.add_node(new_weight);
                    continue;
                }
            }
            result_g.add_vacant_node(&mut free_node);
        }
        for (i, edge) in self.raw_edges().iter().enumerate() {
            if i >= edge_bound {
                break;
            }
            let source = edge.source();
            let target = edge.target();
            if let Some(edge_weight) = edge.weight.as_ref() {
                if result_g.contains_node(source) && result_g.contains_node(target) {
                    if let Some(new_weight) = edge_map(EdgeIndex::new(i), edge_weight) {
                        result_g.add_edge(source, target, new_weight);
                        continue;
                    }
                }
            }
            result_g.add_vacant_edge(&mut free_edge);
        }
        result_g.free_node = free_node;
        result_g.free_edge = free_edge;
        result_g.check_free_lists();
        result_g
    }

    /// Create a new `StableGraph` by mapping nodes and edges, consuming the current graph.
    /// A node or edge may be mapped to `None` to exclude it from
    /// the resulting graph.
    ///
    /// Nodes are mapped first with the `node_map` closure, then
    /// `edge_map` is called for the edges that have not had any endpoint
    /// removed.
    ///
    /// The resulting graph has the structure of a subgraph of the original graph.
    /// Nodes and edges that are not removed maintain their old node or edge
    /// indices.
    ///
    /// If you want a non-consuming version of this function, see [`filter_map`](struct.StableGraph.html#method.filter_map).
    ///
    /// ```
    /// use petgraph::stable_graph::StableGraph;
    ///
    /// // Create a graph with integer node weights
    /// let mut g = StableGraph::<u32, ()>::new();
    /// let a = g.add_node(0);
    /// let b = g.add_node(2);
    /// let c = g.add_node(5);
    /// let d = g.add_node(7);
    /// let e = g.add_node(4);
    /// g.extend_with_edges(&[(a, b, ()), (a, c, ()), (b, d, ()), (c, d, ()), (d, e, ())]);
    ///
    /// // Filter the graph such that only nodes with weight greater than 2 are kept.
    /// let g_filtered = g.filter_map_owned(
    ///     |_, node_weight| if node_weight > 2 { Some(node_weight) } else { None },
    ///     |_, edge_weight| Some(edge_weight),
    /// );
    ///
    /// assert_eq!(g_filtered.node_count(), 3);
    /// // Node and edge indices are preserved.
    /// assert_eq!(g_filtered.node_weight(c), Some(&5));
    /// ```
    pub fn filter_map_owned<F, G, N2, E2>(
        self,
        mut node_map: F,
        mut edge_map: G,
    ) -> StableGraph<N2, E2, Ty, Ix>
    where
        F: FnMut(NodeIndex<Ix>, N) -> Option<N2>,
        G: FnMut(EdgeIndex<Ix>, E) -> Option<E2>,
    {
        let node_bound = self.node_bound();
        let edge_bound = self.edge_bound();
        let mut result_g = StableGraph::with_capacity(node_bound, edge_bound);
        // use separate free lists so that
        // add_node / add_edge below do not reuse the tombstones
        let mut free_node = NodeIndex::end();
        let mut free_edge = EdgeIndex::end();

        // the stable graph keeps the node map itself

        let (nodes, edges) = self.g.into_nodes_edges();

        for (i, node) in nodes.into_iter().enumerate() {
            if i >= node_bound {
                break;
            }
            if let Some(node_weight) = node.weight {
                if let Some(new_weight) = node_map(NodeIndex::new(i), node_weight) {
                    result_g.add_node(new_weight);
                    continue;
                }
            }
            result_g.add_vacant_node(&mut free_node);
        }
        for (i, edge) in edges.into_iter().enumerate() {
            if i >= edge_bound {
                break;
            }
            let source = edge.source();
            let target = edge.target();
            if let Some(edge_weight) = edge.weight {
                if result_g.contains_node(source) && result_g.contains_node(target) {
                    if let Some(new_weight) = edge_map(EdgeIndex::new(i), edge_weight) {
                        result_g.add_edge(source, target, new_weight);
                        continue;
                    }
                }
            }
            result_g.add_vacant_edge(&mut free_edge);
        }
        result_g.free_node = free_node;
        result_g.free_edge = free_edge;
        result_g.check_free_lists();
        result_g
    }

    /// Extend the graph from an iterable of edges.
    ///
    /// Node weights `N` are set to default values.
    /// Edge weights `E` may either be specified in the list,
    /// or they are filled with default values.
    ///
    /// Nodes are inserted automatically to match the edges.
    ///
    /// ```
    /// use petgraph::stable_graph::StableGraph;
    ///
    /// let mut g = StableGraph::<(), i32>::new();
    /// let a = g.add_node(());
    /// let b = g.add_node(());
    /// let c = g.add_node(());
    /// let d = g.add_node(());
    ///
    /// g.extend_with_edges(&[
    ///   (a, b, 7),
    ///   (a, c, 8),
    ///   (a, d, 9),
    ///  (b, c, 10),
    /// ]);
    /// ```
    pub fn extend_with_edges<I>(&mut self, iterable: I)
    where
        I: IntoIterator,
        I::Item: IntoWeightedEdge<E>,
        <I::Item as IntoWeightedEdge<E>>::NodeId: Into<NodeIndex<Ix>>,
        N: Default,
    {
        let iter = iterable.into_iter();

        for elt in iter {
            let (source, target, weight) = elt.into_weighted_edge();
            let (source, target) = (source.into(), target.into());
            self.ensure_node_exists(source);
            self.ensure_node_exists(target);
            self.add_edge(source, target, weight);
        }
    }

    //
    // internal methods
    //
    fn raw_nodes(&self) -> &[Node<Option<N>, Ix>] {
        self.g.raw_nodes()
    }

    fn raw_edges(&self) -> &[Edge<Option<E>, Ix>] {
        self.g.raw_edges()
    }

    /// Create a new node using a vacant position,
    /// updating the free nodes doubly linked list.
    fn occupy_vacant_node(&mut self, node_idx: NodeIndex<Ix>, weight: N) {
        let node_slot = &mut self.g.nodes[node_idx.index()];
        let _old = node_slot.weight.replace(weight);
        debug_assert!(_old.is_none());
        let previous_node = node_slot.next[1];
        let next_node = node_slot.next[0];
        node_slot.next = [EdgeIndex::end(), EdgeIndex::end()];
        if previous_node != EdgeIndex::end() {
            self.g.nodes[previous_node.index()].next[0] = next_node;
        }
        if next_node != EdgeIndex::end() {
            self.g.nodes[next_node.index()].next[1] = previous_node;
        }
        if self.free_node == node_idx {
            self.free_node = next_node._into_node();
        }
        self.node_count += 1;
    }

    /// Create the node if it does not exist,
    /// adding vacant nodes for padding if needed.
    fn ensure_node_exists(&mut self, node_ix: NodeIndex<Ix>)
    where
        N: Default,
    {
        if let Some(Some(_)) = self.g.node_weight(node_ix) {
            return;
        }
        while node_ix.index() >= self.g.node_count() {
            let mut free_node = self.free_node;
            self.add_vacant_node(&mut free_node);
            self.free_node = free_node;
        }
        self.occupy_vacant_node(node_ix, N::default());
    }

    #[cfg(feature = "serde-1")]
    /// Fix up node and edge links after deserialization
    fn link_edges(&mut self) -> Result<(), NodeIndex<Ix>> {
        // set up free node list
        self.node_count = 0;
        self.edge_count = 0;
        let mut free_node = NodeIndex::end();
        for node_index in 0..self.g.node_count() {
            let node = &mut self.g.nodes[node_index];
            if node.weight.is_some() {
                self.node_count += 1;
            } else {
                // free node
                node.next = [free_node._into_edge(), EdgeIndex::end()];
                if free_node != NodeIndex::end() {
                    self.g.nodes[free_node.index()].next[1] = EdgeIndex::new(node_index);
                }
                free_node = NodeIndex::new(node_index);
            }
        }
        self.free_node = free_node;

        let mut free_edge = EdgeIndex::end();
        for (edge_index, edge) in self.g.edges.iter_mut().enumerate() {
            if edge.weight.is_none() {
                // free edge
                edge.next = [free_edge, EdgeIndex::end()];
                free_edge = EdgeIndex::new(edge_index);
                continue;
            }
            let a = edge.source();
            let b = edge.target();
            let edge_idx = EdgeIndex::new(edge_index);
            match index_twice(&mut self.g.nodes, a.index(), b.index()) {
                Pair::None => return Err(if a > b { a } else { b }),
                Pair::One(an) => {
                    edge.next = an.next;
                    an.next[0] = edge_idx;
                    an.next[1] = edge_idx;
                }
                Pair::Both(an, bn) => {
                    // a and b are different indices
                    edge.next = [an.next[0], bn.next[1]];
                    an.next[0] = edge_idx;
                    bn.next[1] = edge_idx;
                }
            }
            self.edge_count += 1;
        }
        self.free_edge = free_edge;
        Ok(())
    }

    #[cfg(not(debug_assertions))]
    fn check_free_lists(&self) {}
    #[cfg(debug_assertions)]
    // internal method to debug check the free lists (linked lists)
    // For the nodes, also check the backpointers of the doubly linked list.
    fn check_free_lists(&self) {
        let mut free_node = self.free_node;
        let mut prev_free_node = NodeIndex::end();
        let mut free_node_len = 0;
        while free_node != NodeIndex::end() {
            if let Some(n) = self.g.nodes.get(free_node.index()) {
                if n.weight.is_none() {
                    debug_assert_eq!(n.next[1]._into_node(), prev_free_node);
                    prev_free_node = free_node;
                    free_node = n.next[0]._into_node();
                    free_node_len += 1;
                    continue;
                }
                debug_assert!(
                    false,
                    "Corrupt free list: pointing to existing {:?}",
                    free_node.index()
                );
            }
            debug_assert!(false, "Corrupt free list: missing {:?}", free_node.index());
        }
        debug_assert_eq!(self.node_count(), self.raw_nodes().len() - free_node_len);

        let mut free_edge_len = 0;
        let mut free_edge = self.free_edge;
        while free_edge != EdgeIndex::end() {
            if let Some(n) = self.g.edges.get(free_edge.index()) {
                if n.weight.is_none() {
                    free_edge = n.next[0];
                    free_edge_len += 1;
                    continue;
                }
                debug_assert!(
                    false,
                    "Corrupt free list: pointing to existing {:?}",
                    free_node.index()
                );
            }
            debug_assert!(false, "Corrupt free list: missing {:?}", free_edge.index());
        }
        debug_assert_eq!(self.edge_count(), self.raw_edges().len() - free_edge_len);
    }
}

/// The resulting cloned graph has the same graph indices as `self`.
impl<N, E, Ty, Ix> Clone for StableGraph<N, E, Ty, Ix>
where
    N: Clone,
    E: Clone,
    Ix: Copy,
{
    fn clone(&self) -> Self {
        StableGraph {
            g: self.g.clone(),
            node_count: self.node_count,
            edge_count: self.edge_count,
            free_node: self.free_node,
            free_edge: self.free_edge,
        }
    }

    fn clone_from(&mut self, rhs: &Self) {
        self.g.clone_from(&rhs.g);
        self.node_count = rhs.node_count;
        self.edge_count = rhs.edge_count;
        self.free_node = rhs.free_node;
        self.free_edge = rhs.free_edge;
    }
}

/// Index the `StableGraph` by `NodeIndex` to access node weights.
///
/// **Panics** if the node doesn't exist.
impl<N, E, Ty, Ix> Index<NodeIndex<Ix>> for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type Output = N;
    fn index(&self, index: NodeIndex<Ix>) -> &N {
        self.node_weight(index).unwrap()
    }
}

/// Index the `StableGraph` by `NodeIndex` to access node weights.
///
/// **Panics** if the node doesn't exist.
impl<N, E, Ty, Ix> IndexMut<NodeIndex<Ix>> for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    fn index_mut(&mut self, index: NodeIndex<Ix>) -> &mut N {
        self.node_weight_mut(index).unwrap()
    }
}

/// Index the `StableGraph` by `EdgeIndex` to access edge weights.
///
/// **Panics** if the edge doesn't exist.
impl<N, E, Ty, Ix> Index<EdgeIndex<Ix>> for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type Output = E;
    fn index(&self, index: EdgeIndex<Ix>) -> &E {
        self.edge_weight(index).unwrap()
    }
}

/// Index the `StableGraph` by `EdgeIndex` to access edge weights.
///
/// **Panics** if the edge doesn't exist.
impl<N, E, Ty, Ix> IndexMut<EdgeIndex<Ix>> for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    fn index_mut(&mut self, index: EdgeIndex<Ix>) -> &mut E {
        self.edge_weight_mut(index).unwrap()
    }
}

/// Create a new empty `StableGraph`.
impl<N, E, Ty, Ix> Default for StableGraph<N, E, Ty, Ix>
where
    Ix: IndexType,
{
    fn default() -> Self {
        Self::with_capacity(0, 0)
    }
}

/// Convert a `Graph` into a `StableGraph`
///
/// Computes in **O(|V| + |E|)** time where V is the set of nodes and E is the set of edges.
///
/// The resulting graph has the same node and edge indices as
/// the original graph.
impl<N, E, Ty, Ix> From<Graph<N, E, Ty, Ix>> for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    fn from(g: Graph<N, E, Ty, Ix>) -> Self {
        let nodes = g.nodes.into_iter().map(|e| Node {
            weight: Some(e.weight),
            next: e.next,
        });
        let edges = g.edges.into_iter().map(|e| Edge {
            weight: Some(e.weight),
            node: e.node,
            next: e.next,
        });
        StableGraph {
            node_count: nodes.len(),
            edge_count: edges.len(),
            g: Graph {
                edges: edges.collect(),
                nodes: nodes.collect(),
                ty: g.ty,
            },
            free_node: NodeIndex::end(),
            free_edge: EdgeIndex::end(),
        }
    }
}

/// Convert a `StableGraph` into a `Graph`
///
/// Computes in **O(|V| + |E|)** time where V is the set of nodes and E is the set of edges.
///
/// This translates the stable graph into a graph with node and edge indices in
/// a compact interval without holes (like `Graph`s always are).
///
/// Only if the stable graph had no vacancies after deletions (if node bound was
/// equal to node count, and the same for edges), would the resulting graph have
/// the same node and edge indices as the input.
impl<N, E, Ty, Ix> From<StableGraph<N, E, Ty, Ix>> for Graph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    fn from(graph: StableGraph<N, E, Ty, Ix>) -> Self {
        let mut result_g = Graph::with_capacity(graph.node_count(), graph.edge_count());
        // mapping from old node index to new node index
        let mut node_index_map = vec![NodeIndex::end(); graph.node_bound()];

        for (i, node) in graph.g.nodes.into_iter().enumerate() {
            if let Some(nw) = node.weight {
                node_index_map[i] = result_g.add_node(nw);
            }
        }
        for edge in graph.g.edges {
            let source_index = edge.source().index();
            let target_index = edge.target().index();
            if let Some(ew) = edge.weight {
                let source = node_index_map[source_index];
                let target = node_index_map[target_index];
                debug_assert!(source != NodeIndex::end());
                debug_assert!(target != NodeIndex::end());
                result_g.add_edge(source, target, ew);
            }
        }
        result_g
    }
}

/// Iterator over all nodes of a graph.
#[derive(Debug, Clone)]
pub struct NodeReferences<'a, N: 'a, Ix: IndexType = DefaultIx> {
    iter: iter::Enumerate<slice::Iter<'a, Node<Option<N>, Ix>>>,
}

impl<'a, N, Ix> Iterator for NodeReferences<'a, N, Ix>
where
    Ix: IndexType,
{
    type Item = (NodeIndex<Ix>, &'a N);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .ex_find_map(|(i, node)| node.weight.as_ref().map(move |w| (node_index(i), w)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, hi) = self.iter.size_hint();
        (0, hi)
    }
}

impl<N, Ix> DoubleEndedIterator for NodeReferences<'_, N, Ix>
where
    Ix: IndexType,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .ex_rfind_map(|(i, node)| node.weight.as_ref().map(move |w| (node_index(i), w)))
    }
}

/// Reference to a `StableGraph` edge.
#[derive(Debug)]
pub struct EdgeReference<'a, E: 'a, Ix = DefaultIx> {
    index: EdgeIndex<Ix>,
    node: [NodeIndex<Ix>; 2],
    weight: &'a E,
}

impl<E, Ix: IndexType> Clone for EdgeReference<'_, E, Ix> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<E, Ix: IndexType> Copy for EdgeReference<'_, E, Ix> {}

impl<E, Ix: IndexType> PartialEq for EdgeReference<'_, E, Ix>
where
    E: PartialEq,
{
    fn eq(&self, rhs: &Self) -> bool {
        self.index == rhs.index && self.weight == rhs.weight
    }
}

impl<'a, Ix, E> EdgeReference<'a, E, Ix>
where
    Ix: IndexType,
{
    /// Access the edge’s weight.
    ///
    /// **NOTE** that this method offers a longer lifetime
    /// than the trait (unfortunately they don't match yet).
    pub fn weight(&self) -> &'a E {
        self.weight
    }
}

/// Iterator over the edges of from or to a node
#[derive(Debug, Clone)]
pub struct Edges<'a, E: 'a, Ty, Ix: 'a = DefaultIx>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    /// starting node to skip over
    skip_start: NodeIndex<Ix>,
    edges: &'a [Edge<Option<E>, Ix>],

    /// Next edge to visit.
    next: [EdgeIndex<Ix>; 2],

    /// For directed graphs: the direction to iterate in
    /// For undirected graphs: the direction of edges
    direction: Direction,
    ty: PhantomData<Ty>,
}

impl<'a, E, Ty, Ix> Iterator for Edges<'a, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type Item = EdgeReference<'a, E, Ix>;

    fn next(&mut self) -> Option<Self::Item> {
        //      type        direction    |    iterate over    reverse
        //                               |
        //    Directed      Outgoing     |      outgoing        no
        //    Directed      Incoming     |      incoming        no
        //   Undirected     Outgoing     |        both       incoming
        //   Undirected     Incoming     |        both       outgoing

        // For iterate_over, "both" is represented as None.
        // For reverse, "no" is represented as None.
        let (iterate_over, reverse) = if Ty::is_directed() {
            (Some(self.direction), None)
        } else {
            (None, Some(self.direction.opposite()))
        };

        if iterate_over.unwrap_or(Outgoing) == Outgoing {
            let i = self.next[0].index();
            if let Some(Edge {
                node,
                weight: Some(weight),
                next,
            }) = self.edges.get(i)
            {
                self.next[0] = next[0];
                return Some(EdgeReference {
                    index: edge_index(i),
                    node: if reverse == Some(Outgoing) {
                        swap_pair(*node)
                    } else {
                        *node
                    },
                    weight,
                });
            }
        }

        if iterate_over.unwrap_or(Incoming) == Incoming {
            while let Some(Edge { node, weight, next }) = self.edges.get(self.next[1].index()) {
                debug_assert!(weight.is_some());
                let edge_index = self.next[1];
                self.next[1] = next[1];
                // In any of the "both" situations, self-loops would be iterated over twice.
                // Skip them here.
                if iterate_over.is_none() && node[0] == self.skip_start {
                    continue;
                }

                return Some(EdgeReference {
                    index: edge_index,
                    node: if reverse == Some(Incoming) {
                        swap_pair(*node)
                    } else {
                        *node
                    },
                    weight: weight.as_ref().unwrap(),
                });
            }
        }

        None
    }
}

/// Iterator over the multiple directed edges connecting a source node to a target node
#[derive(Debug, Clone)]
pub struct EdgesConnecting<'a, E: 'a, Ty, Ix: 'a = DefaultIx>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    target_node: NodeIndex<Ix>,
    edges: Edges<'a, E, Ty, Ix>,
    ty: PhantomData<Ty>,
}

impl<'a, E, Ty, Ix> Iterator for EdgesConnecting<'a, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type Item = EdgeReference<'a, E, Ix>;

    fn next(&mut self) -> Option<EdgeReference<'a, E, Ix>> {
        let target_node = self.target_node;
        self.edges
            .by_ref()
            .find(|&edge| edge.node[1] == target_node)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.edges.size_hint();
        (0, upper)
    }
}

fn swap_pair<T>(mut x: [T; 2]) -> [T; 2] {
    x.swap(0, 1);
    x
}

/// Iterator over all edges of a graph.
#[derive(Debug, Clone)]
pub struct EdgeReferences<'a, E: 'a, Ix: 'a = DefaultIx> {
    iter: iter::Enumerate<slice::Iter<'a, Edge<Option<E>, Ix>>>,
}

impl<'a, E, Ix> Iterator for EdgeReferences<'a, E, Ix>
where
    Ix: IndexType,
{
    type Item = EdgeReference<'a, E, Ix>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.ex_find_map(|(i, edge)| {
            edge.weight.as_ref().map(move |weight| EdgeReference {
                index: edge_index(i),
                node: edge.node,
                weight,
            })
        })
    }
}

impl<E, Ix> DoubleEndedIterator for EdgeReferences<'_, E, Ix>
where
    Ix: IndexType,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.ex_rfind_map(|(i, edge)| {
            edge.weight.as_ref().map(move |weight| EdgeReference {
                index: edge_index(i),
                node: edge.node,
                weight,
            })
        })
    }
}

/// An iterator over either the nodes without edges to them or from them.
#[derive(Debug, Clone)]
pub struct Externals<'a, N: 'a, Ty, Ix: IndexType = DefaultIx> {
    iter: iter::Enumerate<slice::Iter<'a, Node<Option<N>, Ix>>>,
    dir: Direction,
    ty: PhantomData<Ty>,
}

impl<'a, N: 'a, Ty, Ix> Iterator for Externals<'a, N, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type Item = NodeIndex<Ix>;
    fn next(&mut self) -> Option<NodeIndex<Ix>> {
        let k = self.dir.index();
        loop {
            match self.iter.next() {
                None => return None,
                Some((index, node)) => {
                    if node.weight.is_some()
                        && node.next[k] == EdgeIndex::end()
                        && (Ty::is_directed() || node.next[1 - k] == EdgeIndex::end())
                    {
                        return Some(NodeIndex::new(index));
                    } else {
                        continue;
                    }
                }
            }
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.iter.size_hint();
        (0, upper)
    }
}

/// Iterator over the neighbors of a node.
///
/// Iterator element type is `NodeIndex`.
#[derive(Debug, Clone)]
pub struct Neighbors<'a, E: 'a, Ix: 'a = DefaultIx> {
    /// starting node to skip over
    skip_start: NodeIndex<Ix>,
    edges: &'a [Edge<Option<E>, Ix>],
    next: [EdgeIndex<Ix>; 2],
}

impl<E, Ix> Neighbors<'_, E, Ix>
where
    Ix: IndexType,
{
    /// Return a “walker” object that can be used to step through the
    /// neighbors and edges from the origin node.
    ///
    /// Note: The walker does not borrow from the graph, this is to allow mixing
    /// edge walking with mutating the graph's weights.
    pub fn detach(&self) -> WalkNeighbors<Ix> {
        WalkNeighbors {
            inner: super::WalkNeighbors {
                skip_start: self.skip_start,
                next: self.next,
            },
        }
    }
}

impl<E, Ix> Iterator for Neighbors<'_, E, Ix>
where
    Ix: IndexType,
{
    type Item = NodeIndex<Ix>;

    fn next(&mut self) -> Option<NodeIndex<Ix>> {
        // First any outgoing edges
        match self.edges.get(self.next[0].index()) {
            None => {}
            Some(edge) => {
                debug_assert!(edge.weight.is_some());
                self.next[0] = edge.next[0];
                return Some(edge.node[1]);
            }
        }
        // Then incoming edges
        // For an "undirected" iterator (traverse both incoming
        // and outgoing edge lists), make sure we don't double
        // count selfloops by skipping them in the incoming list.
        while let Some(edge) = self.edges.get(self.next[1].index()) {
            debug_assert!(edge.weight.is_some());
            self.next[1] = edge.next[1];
            if edge.node[0] != self.skip_start {
                return Some(edge.node[0]);
            }
        }
        None
    }
}

/// A “walker” object that can be used to step through the edge list of a node.
///
/// See [*.detach()*](struct.Neighbors.html#method.detach) for more information.
///
/// The walker does not borrow from the graph, so it lets you step through
/// neighbors or incident edges while also mutating graph weights, as
/// in the following example:
///
/// ```
/// use petgraph::visit::Dfs;
/// use petgraph::Incoming;
/// use petgraph::stable_graph::StableGraph;
///
/// let mut gr = StableGraph::new();
/// let a = gr.add_node(0.);
/// let b = gr.add_node(0.);
/// let c = gr.add_node(0.);
/// gr.add_edge(a, b, 3.);
/// gr.add_edge(b, c, 2.);
/// gr.add_edge(c, b, 1.);
///
/// // step through the graph and sum incoming edges into the node weight
/// let mut dfs = Dfs::new(&gr, a);
/// while let Some(node) = dfs.next(&gr) {
///     // use a detached neighbors walker
///     let mut edges = gr.neighbors_directed(node, Incoming).detach();
///     while let Some(edge) = edges.next_edge(&gr) {
///         gr[node] += gr[edge];
///     }
/// }
///
/// // check the result
/// assert_eq!(gr[a], 0.);
/// assert_eq!(gr[b], 4.);
/// assert_eq!(gr[c], 2.);
/// ```
pub struct WalkNeighbors<Ix> {
    inner: super::WalkNeighbors<Ix>,
}

impl<Ix: IndexType> Clone for WalkNeighbors<Ix> {
    clone_fields!(WalkNeighbors, inner);
}

impl<Ix: IndexType> WalkNeighbors<Ix> {
    /// Step to the next edge and its endpoint node in the walk for graph `g`.
    ///
    /// The next node indices are always the others than the starting point
    /// where the `WalkNeighbors` value was created.
    /// For an `Outgoing` walk, the target nodes,
    /// for an `Incoming` walk, the source nodes of the edge.
    pub fn next<N, E, Ty: EdgeType>(
        &mut self,
        g: &StableGraph<N, E, Ty, Ix>,
    ) -> Option<(EdgeIndex<Ix>, NodeIndex<Ix>)> {
        self.inner.next(&g.g)
    }

    pub fn next_node<N, E, Ty: EdgeType>(
        &mut self,
        g: &StableGraph<N, E, Ty, Ix>,
    ) -> Option<NodeIndex<Ix>> {
        self.next(g).map(|t| t.1)
    }

    pub fn next_edge<N, E, Ty: EdgeType>(
        &mut self,
        g: &StableGraph<N, E, Ty, Ix>,
    ) -> Option<EdgeIndex<Ix>> {
        self.next(g).map(|t| t.0)
    }
}

/// Iterator over the node indices of a graph.
#[derive(Debug, Clone)]
pub struct NodeIndices<'a, N: 'a, Ix: 'a = DefaultIx> {
    iter: iter::Enumerate<slice::Iter<'a, Node<Option<N>, Ix>>>,
}

impl<N, Ix: IndexType> Iterator for NodeIndices<'_, N, Ix> {
    type Item = NodeIndex<Ix>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.ex_find_map(|(i, node)| {
            if node.weight.is_some() {
                Some(node_index(i))
            } else {
                None
            }
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.iter.size_hint();
        (0, upper)
    }
}

impl<N, Ix: IndexType> DoubleEndedIterator for NodeIndices<'_, N, Ix> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.ex_rfind_map(|(i, node)| {
            if node.weight.is_some() {
                Some(node_index(i))
            } else {
                None
            }
        })
    }
}

/// Iterator over the edge indices of a graph.
///
/// Note: `EdgeIndices` borrows a graph.
#[derive(Debug, Clone)]
pub struct EdgeIndices<'a, E: 'a, Ix: 'a = DefaultIx> {
    iter: iter::Enumerate<slice::Iter<'a, Edge<Option<E>, Ix>>>,
}

impl<E, Ix: IndexType> Iterator for EdgeIndices<'_, E, Ix> {
    type Item = EdgeIndex<Ix>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.ex_find_map(|(i, node)| {
            if node.weight.is_some() {
                Some(edge_index(i))
            } else {
                None
            }
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.iter.size_hint();
        (0, upper)
    }
}

impl<E, Ix: IndexType> DoubleEndedIterator for EdgeIndices<'_, E, Ix> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.ex_rfind_map(|(i, node)| {
            if node.weight.is_some() {
                Some(edge_index(i))
            } else {
                None
            }
        })
    }
}

impl<N, E, Ty, Ix> visit::GraphBase for StableGraph<N, E, Ty, Ix>
where
    Ix: IndexType,
{
    type NodeId = NodeIndex<Ix>;
    type EdgeId = EdgeIndex<Ix>;
}

impl<N, E, Ty, Ix> visit::Visitable for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type Map = FixedBitSet;
    fn visit_map(&self) -> FixedBitSet {
        FixedBitSet::with_capacity(self.node_bound())
    }
    fn reset_map(&self, map: &mut Self::Map) {
        map.clear();
        map.grow(self.node_bound());
    }
}

impl<N, E, Ty, Ix> visit::Data for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type NodeWeight = N;
    type EdgeWeight = E;
}

impl<N, E, Ty, Ix> visit::GraphProp for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type EdgeType = Ty;
}

impl<'a, N, E: 'a, Ty, Ix> visit::IntoNodeIdentifiers for &'a StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type NodeIdentifiers = NodeIndices<'a, N, Ix>;
    fn node_identifiers(self) -> Self::NodeIdentifiers {
        StableGraph::node_indices(self)
    }
}

impl<N, E, Ty, Ix> visit::NodeCount for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    fn node_count(&self) -> usize {
        self.node_count()
    }
}

impl<'a, N, E, Ty, Ix> visit::IntoNodeReferences for &'a StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type NodeRef = (NodeIndex<Ix>, &'a N);
    type NodeReferences = NodeReferences<'a, N, Ix>;
    fn node_references(self) -> Self::NodeReferences {
        NodeReferences {
            iter: self.raw_nodes().iter().enumerate(),
        }
    }
}

impl<N, E, Ty, Ix> visit::NodeIndexable for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    /// Return an upper bound of the node indices in the graph
    fn node_bound(&self) -> usize {
        self.node_indices().next_back().map_or(0, |i| i.index() + 1)
    }
    fn to_index(&self, ix: NodeIndex<Ix>) -> usize {
        ix.index()
    }
    fn from_index(&self, ix: usize) -> Self::NodeId {
        NodeIndex::new(ix)
    }
}

impl<'a, N, E: 'a, Ty, Ix> visit::IntoNeighbors for &'a StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type Neighbors = Neighbors<'a, E, Ix>;
    fn neighbors(self, n: Self::NodeId) -> Self::Neighbors {
        (*self).neighbors(n)
    }
}

impl<'a, N, E: 'a, Ty, Ix> visit::IntoNeighborsDirected for &'a StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type NeighborsDirected = Neighbors<'a, E, Ix>;
    fn neighbors_directed(self, n: NodeIndex<Ix>, d: Direction) -> Self::NeighborsDirected {
        StableGraph::neighbors_directed(self, n, d)
    }
}

impl<'a, N, E, Ty, Ix> visit::IntoEdges for &'a StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type Edges = Edges<'a, E, Ty, Ix>;
    fn edges(self, a: Self::NodeId) -> Self::Edges {
        self.edges(a)
    }
}

impl<Ix, E> visit::EdgeRef for EdgeReference<'_, E, Ix>
where
    Ix: IndexType,
{
    type NodeId = NodeIndex<Ix>;
    type EdgeId = EdgeIndex<Ix>;
    type Weight = E;

    fn source(&self) -> Self::NodeId {
        self.node[0]
    }
    fn target(&self) -> Self::NodeId {
        self.node[1]
    }
    fn weight(&self) -> &E {
        self.weight
    }
    fn id(&self) -> Self::EdgeId {
        self.index
    }
}

impl<N, E, Ty, Ix> visit::EdgeIndexable for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    fn edge_bound(&self) -> usize {
        self.edge_references()
            .next_back()
            .map_or(0, |edge| edge.id().index() + 1)
    }

    fn to_index(&self, ix: EdgeIndex<Ix>) -> usize {
        ix.index()
    }

    fn from_index(&self, ix: usize) -> Self::EdgeId {
        EdgeIndex::new(ix)
    }
}

impl<'a, N, E, Ty, Ix> visit::IntoEdgesDirected for &'a StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type EdgesDirected = Edges<'a, E, Ty, Ix>;
    fn edges_directed(self, a: Self::NodeId, dir: Direction) -> Self::EdgesDirected {
        self.edges_directed(a, dir)
    }
}

impl<'a, N: 'a, E: 'a, Ty, Ix> visit::IntoEdgeReferences for &'a StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    type EdgeRef = EdgeReference<'a, E, Ix>;
    type EdgeReferences = EdgeReferences<'a, E, Ix>;

    /// Create an iterator over all edges in the graph, in indexed order.
    ///
    /// Iterator element type is `EdgeReference<E, Ix>`.
    fn edge_references(self) -> Self::EdgeReferences {
        EdgeReferences {
            iter: self.g.edges.iter().enumerate(),
        }
    }
}

impl<N, E, Ty, Ix> visit::EdgeCount for StableGraph<N, E, Ty, Ix>
where
    Ty: EdgeType,
    Ix: IndexType,
{
    #[inline]
    fn edge_count(&self) -> usize {
        self.edge_count()
    }
}

#[test]
fn stable_graph() {
    use std::println;

    let mut gr = StableGraph::<_, _>::with_capacity(0, 0);
    let a = gr.add_node(0);
    let b = gr.add_node(1);
    let c = gr.add_node(2);
    let _ed = gr.add_edge(a, b, 1);
    println!("{gr:?}");
    gr.remove_node(b);
    println!("{gr:?}");
    let d = gr.add_node(3);
    println!("{gr:?}");
    gr.check_free_lists();
    gr.remove_node(a);
    gr.check_free_lists();
    gr.remove_node(c);
    gr.check_free_lists();
    println!("{gr:?}");
    gr.add_edge(d, d, 2);
    println!("{gr:?}");

    let e = gr.add_node(4);
    gr.add_edge(d, e, 3);
    println!("{gr:?}");
    for neigh in gr.neighbors(d) {
        println!("edge {d:?} -> {neigh:?}");
    }
    gr.check_free_lists();
}

#[test]
fn dfs() {
    use std::println;

    use crate::visit::Dfs;

    let mut gr = StableGraph::<_, _>::with_capacity(0, 0);
    let a = gr.add_node("a");
    let b = gr.add_node("b");
    let c = gr.add_node("c");
    let d = gr.add_node("d");
    gr.add_edge(a, b, 1);
    gr.add_edge(a, c, 2);
    gr.add_edge(b, c, 3);
    gr.add_edge(b, d, 4);
    gr.add_edge(c, d, 5);
    gr.add_edge(d, b, 6);
    gr.add_edge(c, b, 7);
    println!("{gr:?}");

    let mut dfs = Dfs::new(&gr, a);
    while let Some(next) = dfs.next(&gr) {
        println!("dfs visit => {:?}, weight={:?}", next, &gr[next]);
    }
}

#[test]
fn test_retain_nodes() {
    let mut gr = StableGraph::<_, _>::with_capacity(6, 6);
    let a = gr.add_node("a");
    let f = gr.add_node("f");
    let b = gr.add_node("b");
    let c = gr.add_node("c");
    let d = gr.add_node("d");
    let e = gr.add_node("e");
    gr.add_edge(a, b, 1);
    gr.add_edge(a, c, 2);
    gr.add_edge(b, c, 3);
    gr.add_edge(b, d, 4);
    gr.add_edge(c, d, 5);
    gr.add_edge(d, b, 6);
    gr.add_edge(c, b, 7);
    gr.add_edge(d, e, 8);
    gr.remove_node(f);

    assert_eq!(gr.node_count(), 5);
    assert_eq!(gr.edge_count(), 8);
    gr.retain_nodes(|frozen_gr, ix| frozen_gr[ix] >= "c");
    assert_eq!(gr.node_count(), 3);
    assert_eq!(gr.edge_count(), 2);

    gr.check_free_lists();
}

#[test]
fn extend_with_edges() {
    let mut gr = StableGraph::<_, _>::default();
    let a = gr.add_node("a");
    let b = gr.add_node("b");
    let c = gr.add_node("c");
    let _d = gr.add_node("d");
    gr.remove_node(a);
    gr.remove_node(b);
    gr.remove_node(c);

    gr.extend_with_edges(vec![(0, 1, ())]);
    assert_eq!(gr.node_count(), 3);
    assert_eq!(gr.edge_count(), 1);
    gr.check_free_lists();

    gr.extend_with_edges(vec![(5, 1, ())]);
    assert_eq!(gr.node_count(), 4);
    assert_eq!(gr.edge_count(), 2);
    gr.check_free_lists();
}

#[test]
fn test_reverse() {
    let mut gr = StableGraph::<_, _>::default();
    let a = gr.add_node("a");
    let b = gr.add_node("b");

    gr.add_edge(a, b, 0);

    let mut reversed_gr = gr.clone();
    reversed_gr.reverse();

    for i in gr.node_indices() {
        itertools::assert_equal(gr.edges_directed(i, Incoming), reversed_gr.edges(i));
    }
}

#[test]
fn test_reverse_after_remove_nodes() {
    let mut gr = StableGraph::<_, _>::default();
    let a = gr.add_node("a");
    let b = gr.add_node("b");
    let c = gr.add_node("c");

    gr.add_edge(a, a, 0);

    gr.remove_node(b);
    gr.remove_node(c);

    gr.check_free_lists();

    gr.reverse();

    gr.check_free_lists();
}

#[test]
fn test_reverse_after_remove_edges() {
    let mut gr = StableGraph::<_, _>::default();
    let a = gr.add_node("a");
    let b = gr.add_node("b");
    let c = gr.add_node("c");

    let e_ab = gr.add_edge(a, b, 0);
    let e_bc = gr.add_edge(b, c, 0);

    gr.remove_edge(e_ab);
    gr.remove_edge(e_bc);

    gr.check_free_lists();

    gr.reverse();

    gr.check_free_lists();
}
