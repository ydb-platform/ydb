//! Simple graphviz dot file format output.

use alloc::string::String;
use core::fmt::{self, Display, Write};

use crate::visit::{
    EdgeRef, GraphProp, IntoEdgeReferences, IntoNodeReferences, NodeIndexable, NodeRef,
};

/// `Dot` implements output to graphviz .dot format for a graph.
///
/// Formatting and options are rather simple, this is mostly intended
/// for debugging. Exact output may change.
///
/// # Examples
///
/// ```
/// use petgraph::Graph;
/// use petgraph::dot::{Dot, Config};
///
/// let mut graph = Graph::<_, ()>::new();
/// graph.add_node("A");
/// graph.add_node("B");
/// graph.add_node("C");
/// graph.add_node("D");
/// graph.extend_with_edges(&[
///     (0, 1), (0, 2), (0, 3),
///     (1, 2), (1, 3),
///     (2, 3),
/// ]);
///
/// println!("{:?}", Dot::with_config(&graph, &[Config::EdgeNoLabel]));
///
/// // In this case the output looks like this:
/// //
/// // digraph {
/// //     0 [label="\"A\""]
/// //     1 [label="\"B\""]
/// //     2 [label="\"C\""]
/// //     3 [label="\"D\""]
/// //     0 -> 1 [ ]
/// //     0 -> 2 [ ]
/// //     0 -> 3 [ ]
/// //     1 -> 2 [ ]
/// //     1 -> 3 [ ]
/// //     2 -> 3 [ ]
/// // }
///
/// // If you need multiple config options, just list them all in the slice.
/// ```
pub struct Dot<'a, G>
where
    G: IntoEdgeReferences + IntoNodeReferences,
{
    graph: G,
    get_edge_attributes: &'a dyn Fn(G, G::EdgeRef) -> String,
    get_node_attributes: &'a dyn Fn(G, G::NodeRef) -> String,
    config: Configs,
}

static TYPE: [&str; 2] = ["graph", "digraph"];
static EDGE: [&str; 2] = ["--", "->"];
static INDENT: &str = "    ";

impl<'a, G> Dot<'a, G>
where
    G: IntoNodeReferences + IntoEdgeReferences,
{
    /// Create a `Dot` formatting wrapper with default configuration.
    #[inline]
    pub fn new(graph: G) -> Self {
        Self::with_config(graph, &[])
    }

    /// Create a `Dot` formatting wrapper with custom configuration.
    #[inline]
    pub fn with_config(graph: G, config: &'a [Config]) -> Self {
        Self::with_attr_getters(graph, config, &|_, _| String::new(), &|_, _| String::new())
    }

    /// Create a `Dot` that uses the given functions to generate edge and node attributes.
    ///
    /// NOTE: `Config::EdgeNoLabel` and `Config::NodeNoLabel` should be set if you intend to
    /// generate your own `label` attributes.
    /// The getter functions should return an attribute list as a String. For example, if you
    /// want to calculate a `label` for a node, then return `"label = \"your label here\""`.
    /// Each function should take as arguments the graph and that graph's `EdgeRef` or `NodeRef`, respectively.
    /// Check the documentation for the graph type to see how it implements `IntoNodeReferences`.
    /// The [Graphviz docs] list the available attributes.
    ///
    /// Note that some attribute values, such as labels, should be strings and must be quoted. These can be
    /// written using escapes (`"label = \"foo\""`) or [raw strings] (`r#"label = "foo""#`).
    ///
    /// For example, using a `Graph<&str, &str>` where we want the node labels to be the nodes' weights
    /// shortened to 4 characters, and all the edges are colored blue with no labels:
    /// ```
    /// use petgraph::Graph;
    /// use petgraph::dot::{Config, Dot};
    ///
    /// let mut deps = Graph::<&str, &str>::new();
    /// let pg = deps.add_node("petgraph");
    /// let fb = deps.add_node("fixedbitset");
    /// let qc = deps.add_node("quickcheck");
    /// let rand = deps.add_node("rand");
    /// let libc = deps.add_node("libc");
    /// deps.extend_with_edges(&[(pg, fb), (pg, qc), (qc, rand), (rand, libc), (qc, libc)]);
    ///
    /// println!(
    ///     "{:?}",
    ///     Dot::with_attr_getters(
    ///         &deps,
    ///         &[Config::EdgeNoLabel, Config::NodeNoLabel],
    ///         &|_, _| "color = blue".to_string(),
    ///         &|_, (_, s)| format!(r#"label = "{}""#, s.chars().take(4).collect::<String>()),
    ///     )
    /// );
    /// // This outputs:
    /// // digraph {
    /// //     0 [ label = "petg"]
    /// //     1 [ label = "fixe"]
    /// //     2 [ label = "quic"]
    /// //     3 [ label = "rand"]
    /// //     4 [ label = "libc"]
    /// //     0 -> 1 [ color = blue]
    /// //     0 -> 2 [ color = blue]
    /// //     2 -> 3 [ color = blue]
    /// //     3 -> 4 [ color = blue]
    /// //     2 -> 4 [ color = blue]
    /// // }
    /// ```
    ///
    /// [Graphviz docs]: https://graphviz.org/doc/info/attrs.html
    /// [raw strings]: https://doc.rust-lang.org/rust-by-example/std/str.html#literals-and-escapes
    #[inline]
    pub fn with_attr_getters(
        graph: G,
        config: &'a [Config],
        get_edge_attributes: &'a dyn Fn(G, G::EdgeRef) -> String,
        get_node_attributes: &'a dyn Fn(G, G::NodeRef) -> String,
    ) -> Self {
        let config = Configs::extract(config);
        Dot {
            graph,
            get_edge_attributes,
            get_node_attributes,
            config,
        }
    }
}

/// Direction of graph layout.
///
/// <https://graphviz.org/docs/attrs/rankdir/>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankDir {
    /// Top to bottom
    TB,
    /// Bottom to top
    BT,
    /// Left to right
    LR,
    /// Right to left
    RL,
}

/// `Dot` configuration.
///
/// This enum does not have an exhaustive definition (will be expanded)
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum Config {
    /// Use indices for node labels.
    NodeIndexLabel,
    /// Use indices for edge labels.
    EdgeIndexLabel,
    /// Do not generate `label` attributes for edges.
    EdgeNoLabel,
    /// Do not generate `label` attributes for nodes.
    NodeNoLabel,
    /// Do not print the graph/digraph string.
    GraphContentOnly,
    /// Sets direction of graph layout.
    RankDir(RankDir),
}
macro_rules! make_config_struct {
    ($($variant:ident,)*) => {
        #[allow(non_snake_case)]
        #[derive(Default)]
        struct Configs {
            $($variant: bool,)*
            RankDir: Option<RankDir>,
        }
        impl Configs {
            #[inline]
            fn extract(configs: &[Config]) -> Self {
                let mut conf = Self::default();
                for c in configs {
                    match c {
                        $(Config::$variant => conf.$variant = true,)*
                        Config::RankDir(dir) => conf.RankDir = Some(*dir),
                    }
                }
                conf
            }
        }
    }
}
make_config_struct!(
    NodeIndexLabel,
    EdgeIndexLabel,
    EdgeNoLabel,
    NodeNoLabel,
    GraphContentOnly,
);

/// A low-level function allows specifying fmt functions for nodes and edges separately.
impl<G> Dot<'_, G>
where
    G: IntoNodeReferences + IntoEdgeReferences + NodeIndexable + GraphProp,
{
    pub fn graph_fmt<NF, EF>(
        &self,
        f: &mut fmt::Formatter,
        node_fmt: NF,
        edge_fmt: EF,
    ) -> fmt::Result
    where
        NF: Fn(&G::NodeWeight, &mut fmt::Formatter) -> fmt::Result,
        EF: Fn(&G::EdgeWeight, &mut fmt::Formatter) -> fmt::Result,
    {
        let g = self.graph;
        if !self.config.GraphContentOnly {
            writeln!(f, "{} {{", TYPE[g.is_directed() as usize])?;
        }

        if let Some(rank_dir) = &self.config.RankDir {
            let value = match rank_dir {
                RankDir::TB => "TB",
                RankDir::BT => "BT",
                RankDir::LR => "LR",
                RankDir::RL => "RL",
            };
            writeln!(f, "{INDENT}rankdir=\"{value}\"")?;
        }

        // output all labels
        for node in g.node_references() {
            write!(f, "{}{} [ ", INDENT, g.to_index(node.id()),)?;
            if !self.config.NodeNoLabel {
                write!(f, "label = \"")?;
                if self.config.NodeIndexLabel {
                    write!(f, "{}", g.to_index(node.id()))?;
                } else {
                    Escaped(FnFmt(node.weight(), &node_fmt)).fmt(f)?;
                }
                write!(f, "\" ")?;
            }
            writeln!(f, "{}]", (self.get_node_attributes)(g, node))?;
        }
        // output all edges
        for (i, edge) in g.edge_references().enumerate() {
            write!(
                f,
                "{}{} {} {} [ ",
                INDENT,
                g.to_index(edge.source()),
                EDGE[g.is_directed() as usize],
                g.to_index(edge.target()),
            )?;
            if !self.config.EdgeNoLabel {
                write!(f, "label = \"")?;
                if self.config.EdgeIndexLabel {
                    write!(f, "{i}")?;
                } else {
                    Escaped(FnFmt(edge.weight(), &edge_fmt)).fmt(f)?;
                }
                write!(f, "\" ")?;
            }
            writeln!(f, "{}]", (self.get_edge_attributes)(g, edge))?;
        }

        if !self.config.GraphContentOnly {
            writeln!(f, "}}")?;
        }
        Ok(())
    }
}

impl<G> fmt::Display for Dot<'_, G>
where
    G: IntoEdgeReferences + IntoNodeReferences + NodeIndexable + GraphProp,
    G::EdgeWeight: fmt::Display,
    G::NodeWeight: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.graph_fmt(f, fmt::Display::fmt, fmt::Display::fmt)
    }
}

impl<G> fmt::LowerHex for Dot<'_, G>
where
    G: IntoEdgeReferences + IntoNodeReferences + NodeIndexable + GraphProp,
    G::EdgeWeight: fmt::LowerHex,
    G::NodeWeight: fmt::LowerHex,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.graph_fmt(f, fmt::LowerHex::fmt, fmt::LowerHex::fmt)
    }
}

impl<G> fmt::UpperHex for Dot<'_, G>
where
    G: IntoEdgeReferences + IntoNodeReferences + NodeIndexable + GraphProp,
    G::EdgeWeight: fmt::UpperHex,
    G::NodeWeight: fmt::UpperHex,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.graph_fmt(f, fmt::UpperHex::fmt, fmt::UpperHex::fmt)
    }
}

impl<G> fmt::Debug for Dot<'_, G>
where
    G: IntoEdgeReferences + IntoNodeReferences + NodeIndexable + GraphProp,
    G::EdgeWeight: fmt::Debug,
    G::NodeWeight: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.graph_fmt(f, fmt::Debug::fmt, fmt::Debug::fmt)
    }
}

/// Escape for Graphviz
struct Escaper<W>(W);

impl<W> fmt::Write for Escaper<W>
where
    W: fmt::Write,
{
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for c in s.chars() {
            self.write_char(c)?;
        }
        Ok(())
    }

    fn write_char(&mut self, c: char) -> fmt::Result {
        match c {
            '"' | '\\' => self.0.write_char('\\')?,
            // \l is for left justified linebreak
            '\n' => return self.0.write_str("\\l"),
            _ => {}
        }
        self.0.write_char(c)
    }
}

/// Pass Display formatting through a simple escaping filter
struct Escaped<T>(T);

impl<T> fmt::Display for Escaped<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.alternate() {
            writeln!(&mut Escaper(f), "{:#}", &self.0)
        } else {
            write!(&mut Escaper(f), "{}", &self.0)
        }
    }
}

/// Format data using a specific format function
struct FnFmt<'a, T, F>(&'a T, F);

impl<'a, T, F> fmt::Display for FnFmt<'a, T, F>
where
    F: Fn(&'a T, &mut fmt::Formatter<'_>) -> fmt::Result,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.1(self.0, f)
    }
}

#[cfg(feature = "dot_parser")]
#[macro_use]
pub mod dot_parser;

#[cfg(test)]
mod test {
    use alloc::{format, string::String};
    use core::fmt::Write;

    use super::{Config, Dot, Escaper, RankDir};
    use crate::prelude::Graph;
    use crate::visit::NodeRef;

    #[test]
    fn test_escape() {
        let mut buff = String::new();
        {
            let mut e = Escaper(&mut buff);
            let _ = e.write_str("\" \\ \n");
        }
        assert_eq!(buff, "\\\" \\\\ \\l");
    }

    fn simple_graph() -> Graph<&'static str, &'static str> {
        let mut graph = Graph::<&str, &str>::new();
        let a = graph.add_node("A");
        let b = graph.add_node("B");
        graph.add_edge(a, b, "edge_label");
        graph
    }

    #[test]
    fn test_nodeindexlable_option() {
        let graph = simple_graph();
        let dot = format!("{:?}", Dot::with_config(&graph, &[Config::NodeIndexLabel]));
        assert_eq!(dot, "digraph {\n    0 [ label = \"0\" ]\n    1 [ label = \"1\" ]\n    0 -> 1 [ label = \"\\\"edge_label\\\"\" ]\n}\n");
    }

    #[test]
    fn test_edgeindexlable_option() {
        let graph = simple_graph();
        let dot = format!("{:?}", Dot::with_config(&graph, &[Config::EdgeIndexLabel]));
        assert_eq!(dot, "digraph {\n    0 [ label = \"\\\"A\\\"\" ]\n    1 [ label = \"\\\"B\\\"\" ]\n    0 -> 1 [ label = \"0\" ]\n}\n");
    }

    #[test]
    fn test_edgenolable_option() {
        let graph = simple_graph();
        let dot = format!("{:?}", Dot::with_config(&graph, &[Config::EdgeNoLabel]));
        assert_eq!(dot, "digraph {\n    0 [ label = \"\\\"A\\\"\" ]\n    1 [ label = \"\\\"B\\\"\" ]\n    0 -> 1 [ ]\n}\n");
    }

    #[test]
    fn test_nodenolable_option() {
        let graph = simple_graph();
        let dot = format!("{:?}", Dot::with_config(&graph, &[Config::NodeNoLabel]));
        assert_eq!(
            dot,
            "digraph {\n    0 [ ]\n    1 [ ]\n    0 -> 1 [ label = \"\\\"edge_label\\\"\" ]\n}\n"
        );
    }

    #[test]
    fn test_rankdir_bt_option() {
        let graph = simple_graph();
        let dot = format!(
            "{:?}",
            Dot::with_config(&graph, &[Config::RankDir(RankDir::TB)])
        );
        assert_eq!(
            dot,
            "digraph {\n    rankdir=\"TB\"\n    0 [ label = \"\\\"A\\\"\" ]\n    \
            1 [ label = \"\\\"B\\\"\" ]\n    0 -> 1 [ label = \"\\\"edge_label\\\"\" ]\n}\n"
        );
    }

    #[test]
    fn test_rankdir_tb_option() {
        let graph = simple_graph();
        let dot = format!(
            "{:?}",
            Dot::with_config(&graph, &[Config::RankDir(RankDir::BT)])
        );
        assert_eq!(
            dot,
            "digraph {\n    rankdir=\"BT\"\n    0 [ label = \"\\\"A\\\"\" ]\n    \
            1 [ label = \"\\\"B\\\"\" ]\n    0 -> 1 [ label = \"\\\"edge_label\\\"\" ]\n}\n"
        );
    }

    #[test]
    fn test_rankdir_lr_option() {
        let graph = simple_graph();
        let dot = format!(
            "{:?}",
            Dot::with_config(&graph, &[Config::RankDir(RankDir::LR)])
        );
        assert_eq!(
            dot,
            "digraph {\n    rankdir=\"LR\"\n    0 [ label = \"\\\"A\\\"\" ]\n    \
            1 [ label = \"\\\"B\\\"\" ]\n    0 -> 1 [ label = \"\\\"edge_label\\\"\" ]\n}\n"
        );
    }

    #[test]
    fn test_rankdir_rl_option() {
        let graph = simple_graph();
        let dot = format!(
            "{:?}",
            Dot::with_config(&graph, &[Config::RankDir(RankDir::RL)])
        );
        assert_eq!(
            dot,
            "digraph {\n    rankdir=\"RL\"\n    0 [ label = \"\\\"A\\\"\" ]\n    \
            1 [ label = \"\\\"B\\\"\" ]\n    0 -> 1 [ label = \"\\\"edge_label\\\"\" ]\n}\n"
        );
    }

    #[test]
    fn test_with_attr_getters() {
        let graph = simple_graph();
        let dot = format!(
            "{:?}",
            Dot::with_attr_getters(
                &graph,
                &[Config::NodeNoLabel, Config::EdgeNoLabel],
                &|_, er| format!("label = \"{}\"", er.weight().to_uppercase()),
                &|_, nr| format!("label = \"{}\"", nr.weight().to_lowercase()),
            ),
        );
        assert_eq!(dot, "digraph {\n    0 [ label = \"a\"]\n    1 [ label = \"b\"]\n    0 -> 1 [ label = \"EDGE_LABEL\"]\n}\n");
    }
}
