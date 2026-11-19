# Algorithm documentation template

## Template

```markdown
Title or summary

Main description (optional)
...
...

# Arguments
...

# Returns
...

# Complexity
* Time complexity: **O(..)**.
* Auxiliary space: **O(..)**.


# Examples
Rust code...
```

## Template description
The documentation line of the algorithm should consist of:

1. Title or summary that captures the algorithm’s purpose or name.
2. An optional text section with a detailed description, if additional explanation is needed.
3. `Arguments` - a list and explanation of the input parameters.
4. `Returns` - a description of the output produced by the algorithm.
5. `Complexity` - time complexity and the amount of auxiliary memory.
6. `Examples` - sample usage to illustrate the algorithm and help users get started quickly.

The description of the algorithm should also include a reference to the literature. It can be either a Wikipedia page or a link to a journal article, etc.

## Example
The `bellman_ford` algorithm documentation is given below as an example:
```markdown
/// Compute shortest paths from node `source` to all other.
///
/// Using the [Bellman–Ford algorithm][bf]; negative edge costs are
/// permitted, but the graph must not have a cycle of negative weights
/// (in that case it will return an error).
///
/// # Arguments
/// * `g`: graph with no negative cycle.
/// * `source`: the source node.
///
/// # Returns
/// * `Ok`: (if graph contains no negative cycle) a struct [`Paths`] containing distances and
///   predecessors along each shortest path. The vectors in [`Paths`] are indexed by the graph's node indices.
/// * `Err`: if graph contains negative cycle.
///
/// # Complexity
/// * Time complexity: **O(|V||E|)**.
/// * Auxiliary space: **O(|V|)**.
///
/// where **|V|** is the number of nodes and **|E|** is the number of edges.
///
/// [bf]: https://en.wikipedia.org/wiki/Bellman%E2%80%93Ford_algorithm
```

```rust
use petgraph::Graph;
use petgraph::algo::bellman_ford;
use petgraph::prelude::*;

let mut g = Graph::new();
let a = g.add_node(()); // node with no weight
let b = g.add_node(());
let c = g.add_node(());
let d = g.add_node(());
let e = g.add_node(());
let f = g.add_node(());
g.extend_with_edges(&[
    (0, 1, 2.0),
    (0, 3, 4.0),
    (1, 2, 1.0),
    (1, 5, 7.0),
    (2, 4, 5.0),
    (4, 5, 1.0),
    (3, 4, 1.0),
]);
// Graph represented with the weight of each edge
//     2       1
// a ----- b ----- c
// | 4     | 7     |
// d       f       | 5
// | 1     | 1     |
// \------ e ------/
let path = bellman_ford(&g, a);
assert!(path.is_ok());
let path = path.unwrap();
assert_eq!(path.distances, vec![    0.0,     2.0,    3.0,    4.0,     5.0,     6.0]);
assert_eq!(path.predecessors, vec![None, Some(a),Some(b),Some(a), Some(d), Some(e)]);
// Node f (indice 5) can be reach from a with a path costing 6.
// Predecessor of f is Some(e) which predecessor is Some(d) which predecessor is Some(a).
// Thus the path from a to f is a <-> d <-> e <-> f
let graph_with_neg_cycle = Graph::<(), f32, Undirected>::from_edges(&[
        (0, 1, -2.0),
        (0, 3, -4.0),
        (1, 2, -1.0),
        (1, 5, -25.0),
        (2, 4, -5.0),
        (4, 5, -25.0),
        (3, 4, -1.0),
]);

assert!(bellman_ford(&graph_with_neg_cycle, NodeIndex::new(0)).is_err());
```

