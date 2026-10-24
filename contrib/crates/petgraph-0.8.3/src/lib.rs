/*!
`petgraph` is a graph data structure library.

Graphs are collections of nodes, and edges between nodes. `petgraph`
provides several [graph types](index.html#graph-types) (each differing in the
tradeoffs taken in their internal representation),
[algorithms](./algo/index.html) on those graphs, and functionality to
[output graphs](./dot/struct.Dot.html) in
[`Graphviz`](https://www.graphviz.org/) format. Both nodes and edges
can have arbitrary associated data, and edges may be either directed or undirected.

# Overview

Here is a simple example showing off some features of `petgraph`:
```
use petgraph::graph::UnGraph;
use petgraph::algo::{dijkstra, min_spanning_tree};
use petgraph::data::FromElements;
use petgraph::dot::{Dot, Config};
use petgraph::visit::NodeIndexable;

// Create an undirected graph with associated data
// of type `i32` for the nodes and `()` for the edges.
let g = UnGraph::<i32, ()>::from_edges(&[
    (0, 1), (1, 2), (2, 3), (0, 3)
]);

// The graph looks like this:
// 0 -- 1
// |    |
// 3 -- 2

// Find the shortest path from `0` to `2` using `1` as the cost for every edge.
let node_map = dijkstra(&g, 0.into(), Some(2.into()), |_| 1);
assert_eq!(&2i32, node_map.get(&g.from_index(2)).unwrap());

// Get the minimum spanning tree of the graph as a new graph, and check that
// one edge was trimmed.
let mst = UnGraph::<_, _>::from_elements(min_spanning_tree(&g));
assert_eq!(g.raw_edges().len() - 1, mst.raw_edges().len());

// Output the tree to `graphviz` `DOT` format
println!("{:?}", Dot::with_config(&mst, &[Config::EdgeNoLabel]));
// graph {
//     0 [ label = "0" ]
//     1 [ label = "0" ]
//     2 [ label = "0" ]
//     3 [ label = "0" ]
//     0 -- 1 [ ]
//     2 -- 3 [ ]
//     1 -- 2 [ ]
// }
```

`petgraph` provides several concrete graph types — [`Graph`](./graph/struct.Graph.html),
[`StableGraph`](./stable_graph/struct.StableGraph.html), [`GraphMap`](./graphmap/struct.GraphMap.html),
[`MatrixGraph`](./matrix_graph/struct.MatrixGraph.html), and [`Csr`](./csr/struct.Csr.html)
— each optimized for different trade-offs in memory layout, index stability, and lookup speed.
Some types (e.g., [`Graph`](./graph/struct.Graph.html)) expose the fullest set of methods
and algorithm support, while others (like [`StableGraph`](./stable_graph/struct.StableGraph.html)
or [`Csr`](./csr/struct.Csr.html)) are more recent and may not yet implement the full feature set,
see [Graph Types](#graph-types) for more details.

With these types as building blocks, you can insert or remove nodes and edges, attach arbitrary data to them,
explore neighbors, and apply standard graph algorithms. The [`algo`] module implements routines such as
shortest‐path searches and minimum spanning trees for any compatible graph, and the [`dot`] module exports
functionality to convert graphs to DOT format so you can visualize or analyze them with
[`Graphviz`](https://www.graphviz.org/).

The remainder of this documentation is organized as follows:

* [Usage](#usage) shows how to add `petgraph` to your project.

* [Graph types](#graph-types) explains each implementation’s internal structure and feature set.

    * [Generic parameters](#generic-parameters) clarifies what N, E, Ty, and Ix signify and any trait bounds they impose.

    * [Shorthand types](#shorthand-types) lists commonly used aliases (for example, [`DiGraph<_, _>`](./graph/type.DiGraph.html) for [`Graph<_, _, Directed>`](./graph/struct.Graph.html).

* [Examples](#examples) walks through common tasks such as basic graph construction, index behavior, running algorithms, weight transformations, and DOT export.

* [Crate features](#crate-features) covers (optional) Cargo flags (e.g. serde or rayon support).

* Finally, each submodule page (e.g., [`algo`], [`graph`], [`graphmap`], etc.) provides detailed API documentation and design notes.

# Usage

`petgraph` is available on [crates.io](https://crates.io/crates/petgraph) and can be added to your
project by adding `petgraph` to your `Cargo.toml`. Or more simply, by running `cargo add petgraph`.

Here is an example that creates a new Rust project, adds a dependency on `petgraph`, and runs
a simple program that creates an undirected graph.

First, create a new Rust project in a new directory:
```bash
cargo new petgraph_example
cd petgraph_example
```

Second, add a dependency on `petgraph`:
```bash
cargo add petgraph
```

Third, replace the contents of your main function in `src/main.rs` with the following code:
```text
use petgraph::graph::UnGraph;

fn main() {
    let g = UnGraph::<(), ()>::from_edges(&[(0, 1), (1, 2), (2, 3), (0, 3)]);

    println!("Graph: {:?}", g);
}
```

Finally, run the program with `cargo run`:
```bash
Graph { Ty: "Undirected", node_count: 4, edge_count: 4, edges: (0, 1), (1, 2), (2, 3), (0, 3) }
```

# Graph types

* [`Graph`](./graph/struct.Graph.html) -
  An adjacency list graph with arbitrary associated data.
* [`StableGraph`](./stable_graph/struct.StableGraph.html) -
  Similar to `Graph`, but it keeps indices stable across removals.
* [`GraphMap`](./graphmap/struct.GraphMap.html) -
  An adjacency list graph backed by a hash table. The node identifiers are the keys
  into the table.
* [`MatrixGraph`](./matrix_graph/struct.MatrixGraph.html) -
  An adjacency matrix graph.
* [`CSR`](./csr/struct.Csr.html) -
  A sparse adjacency matrix graph with arbitrary associated data.

### Generic parameters

Each graph type is generic over a handful of parameters. All graphs share 3 common
parameters, `N`, `E`, and `Ty`. This is a broad overview of what those are. Each
graph type's documentation will have finer detail on these parameters.

`N` & `E` are called *weights* in this implementation, and are associated with
nodes and edges respectively. They can generally be of arbitrary type, and don't have to
be what you might conventionally consider weight-like. For example, using `&str` for `N`
will work. Many algorithms that require costs let you provide a cost function that
translates your `N` and `E` weights into costs appropriate to the algorithm. Some graph
types and choices do impose bounds on `N` or `E`.
[`min_spanning_tree`](./algo/fn.min_spanning_tree.html) for example requires edge weights that
implement [`PartialOrd`](https://doc.rust-lang.org/stable/core/cmp/trait.PartialOrd.html).
[`GraphMap`](./graphmap/struct.GraphMap.html) requires node weights that can serve as hash
map keys, since that graph type does not create standalone node indices.

`Ty` controls whether edges are [`Directed`](./enum.Directed.html) or
[`Undirected`](./enum.Undirected.html).

`Ix` appears on graph types that use indices. It is exposed so you can control
the size of node and edge indices, and therefore the memory footprint of your graphs.
Allowed values are `u8`, `u16`, `u32`, and `usize`, with `u32` being the default.

### Shorthand types

Each graph type vends a few shorthand type definitions that name some specific
generic choices. For example, [`DiGraph<_, _>`](./graph/type.DiGraph.html) is shorthand
for [`Graph<_, _, Directed>`](graph/struct.Graph.html).
[`UnMatrix<_, _>`](./matrix_graph/type.UnMatrix.html) is shorthand for
[`MatrixGraph<_, _, Undirected>`](./matrix_graph/struct.MatrixGraph.html). Each graph type's
module documentation lists the available shorthand types.

# Examples

* [Creating an undirected graph and manipulating nodes and edges](#creating-an-undirected-graph-and-manipulating-nodes-and-edges)
* [Differences of stable and non-stable graphs in index management](#differences-of-stable-and-non-stable-graphs-in-index-management)
* [Using algorithms on graphs](#using-algorithms-on-graphs)
* [Associating data with nodes and edges and transmuting the type of the data](#associating-data-with-nodes-and-edges-and-transmuting-the-type-of-the-data)
* [Exporting graphs to DOT format](#exporting-graphs-to-dot-format)

### Creating an undirected graph and manipulating nodes and edges

```
use petgraph::graph::UnGraph;
use petgraph::visit::NodeIndexable;

// Create an undirected graph with associated data of type `i32` for nodes and `()` for edges.
let mut g = UnGraph::<i32, ()>::from_edges(&[(0, 1), (1, 2), (2, 3), (0, 3)]);

// The graph looks like this:
// 0 -- 1
// |    |
// 3 -- 2

// Add two more edges between nodes 0 and 2, and 1 and 3
g.extend_with_edges(&[(0, 2), (1, 3)]);

// Add another node with a weight of 5
let node = g.add_node(5);

// Connect the new node to node 2.
// We can access the recently added node via the returned `NodeIndex`.
g.add_edge(node, 2.into(), ());

// The graph now looks like this:
// 0 -- 1
// | \/ |
// | /\ |
// 3 -- 2
//        \
//         4

// We can also access existing nodes by creating a `NodeIndex` using the from_index
// method on g. Indexes are zero-based, so the first node is at index 0.
let node_0 = g.from_index(0);

// And then change the weight of node 0 to 10.
let node_0_weight = g.node_weight_mut(node_0).unwrap();
*node_0_weight = 10;
assert_eq!(g.node_weight(node_0), Some(&10));
```

Note that when creating the graph, we only specified the edges, and the nodes were created
automatically. Since we did not specify the node weights, they default to `i32::default()`, which
is `0`.

### Differences of stable and non-stable graphs in index management

This example shows how to remove a node from a graph and how this might change node indices.
Removing a node also automatically takes care of removing all edges connected to the removed node.

When removing a node from a non-stable graph, the node indices might change depending on two cases.
If the node had the highest index, the other nodes' indices don't change. If the removed
node did not have the highest index, the node with the highest index will take the index of the
removed node and all other indices stay the same.

[Stable graphs](./stable_graph/struct.StableGraph.html) address this by keeping the indices of nodes
and edges stable, even after removals. Currently, this comes at the cost of possible additional memory
usage and lack of some features that other graph types provide. For all the graph types, and their
internal structure and feature set, please refer to [Graph Types](#graph-types).

```
#[cfg(feature = "stable_graph")]
{
use petgraph::graph::UnGraph;
use petgraph::stable_graph::StableUnGraph;
use petgraph::visit::IntoNodeIdentifiers;

// Create an stable and non-stable undirected graph.
let mut g_non_stable = UnGraph::<i32, ()>::from_edges(&[(0, 1), (1, 2), (2, 3), (0, 3)]);
let mut g_stable = StableUnGraph::<i32, ()>::from_edges(&[(0, 1), (1, 2), (2, 3), (0, 3)]);

// The graphs look like this:
// 0 -- 1
// |    |
// 3 -- 2

// Remove node 1 and see how the node indexes change.
g_non_stable.remove_node(1.into());
g_stable.remove_node(1.into());

println!("Node Indices (Non-Stable): {:?}", g_non_stable.node_identifiers().collect::<Vec<_>>());
// Output: Node Indices (Non-Stable): [NodeIndex(0), NodeIndex(1), NodeIndex(2)]
println!("Node Indices (Stable): {:?}", g_stable.node_identifiers().collect::<Vec<_>>());
// Output: Node Indices (Stable):     [NodeIndex(0), NodeIndex(1), NodeIndex(3)]

// The non-stable graph now looks like this:
// 0
// |
// 1 -- 2
// The node which previously had index 1 has been removed, and the node which previously had
// index 2 has now taken index 1. The other nodes' indices remain unchanged.

// The stable graph now looks like this:
// 0
// |
// 2 -- 3
// The node indices have remained stable and the node with index 1 has been removed.
}
```

### Using algorithms on graphs

Petgraph provides not only data structures for modeling graphs, but also a wide range of algorithms
that can be applied to them. For example, given a graph, one can compute shortest paths,
minimum spanning trees, or even compute the maximal cliques of a graph.

Generally, algorithms are found in the [`algo`] module, except for algorithms like
depth-/breadth-first-search, which can be found in the [`visit`] module. All of them should include
an example of how to use them. For example, to compute the minimum spanning tree of a graph, one can use the
[`min_spanning_tree`](algo/min_spanning_tree/fn.min_spanning_tree.html) function.

```
use petgraph::algo::min_spanning_tree;
use petgraph::data::FromElements;
use petgraph::graph::UnGraph;

// Create a graph to compute the minimum spanning tree of.
let g = UnGraph::<i32, ()>::from_edges(&[(0, 1), (1, 2), (2, 3), (0, 3)]);

// The graphs look like this:
// 0 -- 1
// |    |
// 3 -- 2

// Compute a minimum spanning tree of the graph and collect it into a new graph.
let mst = UnGraph::<_, _>::from_elements(min_spanning_tree(&g));

// Check that the minimum spanning tree has one edge less than the original graph.
assert_eq!(g.raw_edges().len() - 1, mst.raw_edges().len());
```

### Associating data with nodes and edges and transmuting the type of the data

In many cases, it is useful to associate data with nodes and/or edges in a graph.
For example, associating an integer with each edge to represent its usage cost in a network.

Petgraph allows you to associate arbitrary data with both nodes and edges in a graph.
Not only that, but it also exposes functionality to easily work with your associated
data and transform it into a different type using the `map` methods.

Associated data might also be referred to as *weights* in the documentation.

```
use petgraph::graph::UnGraph;

// Create an undirected graph with city names as node data and their distances as edge data.
let mut g = UnGraph::<String, u32>::new_undirected();

let ber = g.add_node("Berlin".to_owned());
let del = g.add_node("New Delhi".to_owned());
let mex = g.add_node("Mexico City".to_owned());
let syd = g.add_node("Sydney".to_owned());

// Add distances in kilometers as edge data.
g.extend_with_edges(&[
    (ber, del, 6_000),
    (ber, mex, 10_000),
    (ber, syd, 16_000),
    (del, mex, 14_000),
    (del, syd, 12_000),
    (mex, syd, 15_000),
]);

// We might now want to change up the distances to be in miles instead and to be strings.
// We can do this using the `map` method, which takes two closures for the node and edge data,
// respectively, and returns a new graph with the transformed data.
let g_miles: UnGraph<String, String> = g.map(
    |_, city| city.to_owned(),
    |_, distance| format!("{} miles", (*distance as f64 * 0.621371).round() as i32),
);
```

### Exporting graphs to DOT format

Petgraph provides functionality to export graphs to the [DOT](https://www.graphviz.org/doc/info/lang.html)
format, which can be used with the [Graphviz](https://www.graphviz.org/) suite of tools for
visualization and analysis of graphs. The [`dot`] module provides the necessary functionality to
convert graphs into DOT format.

Let's try exporting the graph we created in the previous example to DOT format.

```
use petgraph::dot::{Config, Dot};
use petgraph::graph::UnGraph;
use petgraph::visit::EdgeRef;

// Create an undirected graph with city names as node data and their distances as edge data.
let mut g = UnGraph::<String, u32>::new_undirected();

let ber = g.add_node("Berlin".to_owned());
let del = g.add_node("New Delhi".to_owned());
let mex = g.add_node("Mexico City".to_owned());
let syd = g.add_node("Sydney".to_owned());

// Add distances in kilometers as edge data.
g.extend_with_edges(&[
    (ber, del, 6_000),
    (ber, mex, 10_000),
    (ber, syd, 16_000),
    (del, mex, 14_000),
    (del, syd, 12_000),
    (mex, syd, 15_000),
]);

// Basic DOT export with automatic labels
let basic_dot = Dot::new(&g);
println!("Basic DOT format:\n{:?}\n", basic_dot);
// Output:
// Basic DOT format:
// graph {
//     0 [ label = "\"Berlin\"" ]
//     1 [ label = "\"New Delhi\"" ]
//     2 [ label = "\"Mexico City\"" ]
//     3 [ label = "\"Sydney\"" ]
//     0 -- 1 [ label = "6000" ]
//     0 -- 2 [ label = "10000" ]
//     0 -- 3 [ label = "16000" ]
//     1 -- 2 [ label = "14000" ]
//     1 -- 3 [ label = "12000" ]
//     2 -- 3 [ label = "15000" ]
// }

// Enhanced DOT export with custom attributes
let fancy_dot = Dot::with_attr_getters(
    &g,
    // Global graph attributes
    &[],
    // Edge attribute getter
    &|graph_reference, edge_reference| {
        // Style edges depending on distance
        if graph_reference.edge_weight(edge_reference.id()).unwrap() > &12_500 {
            "style=dashed, penwidth=3".to_owned()
        } else {
            "style=solid".to_owned()
        }
    },
    // Node attribute getter; We don't change any node attributes
    &|_, (_, _)| String::new(),
);

println!("Enhanced DOT format:\n{:?}", fancy_dot);
// Output:
// Enhanced DOT format:
// graph {
//     0 [ label = "\"Berlin\"" ]
//     1 [ label = "\"New Delhi\"" ]
//     2 [ label = "\"Mexico City\"" ]
//     3 [ label = "\"Sydney\"" ]
//     0 -- 1 [ label = "6000" style=solid]
//     0 -- 2 [ label = "10000" style=solid]
//     0 -- 3 [ label = "16000" style=dashed, penwidth=3]
//     1 -- 2 [ label = "14000" style=dashed, penwidth=3]
//     1 -- 3 [ label = "12000" style=solid]
//     2 -- 3 [ label = "15000" style=dashed, penwidth=3]
// }

// This would typically be written to a file:
// std::fs::write("flight_network.dot", format!("{:?}", fancy_dot)).unwrap();
```

# Crate features

`petgraph` is built with these features enabled by default:

* **graphmap** -
  Enables [`GraphMap`](./graphmap/struct.GraphMap.html).
* **stable_graph** -
  Enables [`StableGraph`](./stable_graph/struct.StableGraph.html).
* **matrix_graph** -
  Enables [`MatrixGraph`](./matrix_graph/struct.MatrixGraph.html).
* **std** -
  Enables the Rust Standard Library. Disabling the `std` feature makes it possible to use `petgraph` in `no_std` contexts.

Optionally, the following features can be enabled:

* **serde-1** -
  Enables serialization for ``Graph, StableGraph, GraphMap`` using
  [`serde 1.0`](https://crates.io/crates/serde). May require a more recent version
  of Rust than petgraph alone.
* **rayon** -
  Enables parallel versions of iterators and algorithms using
  [`rayon`](https://docs.rs/rayon/latest/rayon/) crate. Requires the `std` feature.
* **dot_parser** -
  Enables building [`Graph`](./graph/struct.Graph.html) and [`StableGraph`](./stable_graph/struct.StableGraph.html) from [DOT/Graphviz](https://www.graphviz.org/doc/info/lang.html) descriptions. Imports can be made statically or dynamically (i.e. at compile time or at runtime).
* **unstable** -
  Enables unstable crate features (currently only `generate`).
* **generate** -
  Enables graph generators. The API of functionality behind this flag is subject to change at any time.
*/
#![doc(html_root_url = "https://docs.rs/petgraph/0.4/")]
#![no_std]

extern crate alloc;

#[cfg(any(feature = "std", test))]
extern crate std;

extern crate fixedbitset;
#[cfg(feature = "graphmap")]
extern crate indexmap;

#[cfg(feature = "serde-1")]
extern crate serde;
#[cfg(feature = "serde-1")]
#[macro_use]
extern crate serde_derive;

#[cfg(all(feature = "serde-1", test))]
extern crate itertools;

#[doc(no_inline)]
pub use crate::graph::Graph;

pub use crate::Direction::{Incoming, Outgoing};

#[macro_use]
mod macros;
mod scored;

// these modules define trait-implementing macros
#[macro_use]
pub mod visit;
#[macro_use]
pub mod data;

pub mod acyclic;
pub mod adj;
pub mod algo;
pub mod csr;
pub mod dot;
#[cfg(feature = "generate")]
pub mod generate;
pub mod graph6;
mod graph_impl;
#[cfg(feature = "graphmap")]
pub mod graphmap;
mod iter_format;
mod iter_utils;
#[cfg(feature = "matrix_graph")]
pub mod matrix_graph;
#[cfg(feature = "quickcheck")]
mod quickcheck;
#[cfg(feature = "serde-1")]
mod serde_utils;
mod traits_graph;
pub mod unionfind;

pub mod operator;
pub mod prelude;

/// `Graph<N, E, Ty, Ix>` is a graph datastructure using an adjacency list representation.
pub mod graph {
    pub use crate::graph_impl::{
        edge_index, node_index, DefaultIx, DiGraph, Edge, EdgeIndex, EdgeIndices, EdgeReference,
        EdgeReferences, EdgeWeightsMut, Edges, EdgesConnecting, Externals, Frozen, Graph,
        GraphError, GraphIndex, IndexType, Neighbors, Node, NodeIndex, NodeIndices, NodeReferences,
        NodeWeightsMut, UnGraph, WalkNeighbors,
    };
}

#[cfg(feature = "stable_graph")]
pub use crate::graph_impl::stable_graph;

// Index into the NodeIndex and EdgeIndex arrays
/// Edge direction.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
#[repr(usize)]
#[cfg_attr(
    feature = "serde-1",
    derive(serde_derive::Serialize, serde_derive::Deserialize)
)]
pub enum Direction {
    /// An `Outgoing` edge is an outward edge *from* the current node.
    Outgoing = 0,
    /// An `Incoming` edge is an inbound edge *to* the current node.
    Incoming = 1,
}

impl Direction {
    /// Return the opposite `Direction`.
    #[inline]
    pub fn opposite(self) -> Direction {
        match self {
            Outgoing => Incoming,
            Incoming => Outgoing,
        }
    }

    /// Return `0` for `Outgoing` and `1` for `Incoming`.
    #[inline]
    pub fn index(self) -> usize {
        (self as usize) & 0x1
    }
}

#[doc(hidden)]
pub use crate::Direction as EdgeDirection;

/// Marker type for a directed graph.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(
    feature = "serde-1",
    derive(serde_derive::Serialize, serde_derive::Deserialize)
)]
pub enum Directed {}

/// Marker type for an undirected graph.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(
    feature = "serde-1",
    derive(serde_derive::Serialize, serde_derive::Deserialize)
)]
pub enum Undirected {}

/// A graph's edge type determines whether it has directed edges or not.
pub trait EdgeType {
    fn is_directed() -> bool;
}

impl EdgeType for Directed {
    #[inline]
    fn is_directed() -> bool {
        true
    }
}

impl EdgeType for Undirected {
    #[inline]
    fn is_directed() -> bool {
        false
    }
}

/// Convert an element like `(i, j)` or `(i, j, w)` into
/// a triple of source, target, edge weight.
///
/// For `Graph::from_edges` and `GraphMap::from_edges`.
pub trait IntoWeightedEdge<E> {
    type NodeId;
    fn into_weighted_edge(self) -> (Self::NodeId, Self::NodeId, E);
}

impl<Ix, E> IntoWeightedEdge<E> for (Ix, Ix)
where
    E: Default,
{
    type NodeId = Ix;

    fn into_weighted_edge(self) -> (Ix, Ix, E) {
        let (s, t) = self;
        (s, t, E::default())
    }
}

impl<Ix, E> IntoWeightedEdge<E> for (Ix, Ix, E) {
    type NodeId = Ix;
    fn into_weighted_edge(self) -> (Ix, Ix, E) {
        self
    }
}

impl<Ix, E> IntoWeightedEdge<E> for (Ix, Ix, &E)
where
    E: Clone,
{
    type NodeId = Ix;
    fn into_weighted_edge(self) -> (Ix, Ix, E) {
        let (a, b, c) = self;
        (a, b, c.clone())
    }
}

impl<Ix, E> IntoWeightedEdge<E> for &(Ix, Ix)
where
    Ix: Copy,
    E: Default,
{
    type NodeId = Ix;
    fn into_weighted_edge(self) -> (Ix, Ix, E) {
        let (s, t) = *self;
        (s, t, E::default())
    }
}

impl<Ix, E> IntoWeightedEdge<E> for &(Ix, Ix, E)
where
    Ix: Copy,
    E: Clone,
{
    type NodeId = Ix;
    fn into_weighted_edge(self) -> (Ix, Ix, E) {
        self.clone()
    }
}
