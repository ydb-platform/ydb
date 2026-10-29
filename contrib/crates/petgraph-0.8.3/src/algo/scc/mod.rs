pub mod kosaraju_scc;
pub mod tarjan_scc;

#[allow(deprecated)]
pub use kosaraju_scc::{kosaraju_scc, scc};
pub use tarjan_scc::{tarjan_scc, TarjanScc};
