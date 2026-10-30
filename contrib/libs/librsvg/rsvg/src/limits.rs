//! Processing limits to mitigate malicious SVGs.

/// Maximum number of times that elements can be referenced through URL fragments.
///
/// This is a mitigation for the security-related bugs:
/// <https://gitlab.gnome.org/GNOME/librsvg/issues/323>
/// <https://gitlab.gnome.org/GNOME/librsvg/issues/515>
///
/// Imagine the XML [billion laughs attack], but done in SVG's terms:
///
/// - #323 above creates deeply nested groups of `<use>` elements.
///   The first one references the second one ten times, the second one
///   references the third one ten times, and so on.  In the file given,
///   this causes 10^17 objects to be rendered.  While this does not
///   exhaust memory, it would take a really long time.
///
/// - #515 has deeply nested references of `<pattern>` elements.  Each
///   object inside each pattern has an attribute
///   fill="url(#next_pattern)", so the number of final rendered objects
///   grows exponentially.
///
/// We deal with both cases by placing a limit on how many references
/// will be resolved during the SVG rendering process, that is,
/// how many `url(#foo)` will be resolved.
///
/// [billion laughs attack]: https://bitbucket.org/tiran/defusedxml
pub const MAX_REFERENCED_ELEMENTS: usize = 500_000;

/// Maximum number of elements loadable per document.
///
/// This is a mitigation for SVG files which create millions of elements
/// in an attempt to exhaust memory.  We don't allow loading more than
/// this number of elements during the initial streaming load process.
pub const MAX_LOADED_ELEMENTS: usize = 1_000_000;

/// Maximum number of attributes per XML element.
///
/// A malicious file could have `<foo attr1="blah" attr2="blah" attr3="blah"/>`,
/// etc.  Librsvg puts this limit on how many attributes an element can have.
pub const MAX_LOADED_ATTRIBUTES: usize = u16::MAX as usize;

/// Maximum level of nesting for XInclude (XML Include) files.
///
/// See <https://gitlab.gnome.org/GNOME/librsvg/-/issues/942>.  With
/// the use of XML like `<xi:include parse="xml" href="foo.xml"/>`, an
/// SVG document can recursively include other XML files.  This value
/// defines a maximum level of nesting for XInclude, to prevent cases
/// where the base document is included within itself, or when two
/// documents recursively include each other.
pub const MAX_XINCLUDE_DEPTH: usize = 20;

/// Maximum level of nesting for layers during rendering.
///
/// This is a mitigation for malicious SVGs which try to have layers that are nested
/// extremely deep, as this could cause stack exhaustion.
pub const MAX_LAYER_NESTING_DEPTH: u16 = 50;
