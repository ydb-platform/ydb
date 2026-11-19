//! Collection of algorithms for the [Maximum Flow Problem][max_flow].
//!
//! Both algorithms solve the maximum flow problem and compute the same
//! maximum flow value, although they may differ in how much flow is
//! assigned to each edge in the resulting flow.
//!
//! [dinics] and [ford_fulkerson] have different time complexities, and
//! their performance can vary significantly depending on the input graph.
//! In general, [dinics] is faster, especially on dense graphs, graphs with
//! unit capacities, and bipartite graphs.
//! [ford_fulkerson] may be a better choice when working with small or
//! sparse graphs.
//!
//! For more information about each algorithm and their detailed time
//! complexity, check their respective documentation.
//!
//! [max_flow]: https://en.wikipedia.org/wiki/Maximum_flow_problem

mod dinics;
mod ford_fulkerson;

pub use dinics::dinics;
pub use ford_fulkerson::ford_fulkerson;
