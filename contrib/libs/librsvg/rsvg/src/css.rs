//! Representation of CSS types, and the CSS parsing and matching engine.
//!
//! # Terminology
//!
//! Consider a CSS **stylesheet** like this:
//!
//! ```css
//! @import url("another.css");
//!
//! foo, .bar {
//!         fill: red;
//!         stroke: green;
//! }
//!
//! #baz { stroke-width: 42; }
//! ```
//! The example contains three **rules**, the first one is an **at-rule*,
//! the other two are **qualified rules**.
//!
//! Each rule is made of two parts, a **prelude** and an optional **block**
//! The prelude is the part until the first `{` or until `;`, depending on
//! whether a block is present.  The block is the part between curly braces.
//!
//! Let's look at each rule:
//!
//! `@import` is an **at-rule**.  This rule has a prelude, but no block.
//! There are other at-rules like `@media` and some of them may have a block,
//! but librsvg doesn't support those yet.
//!
//! The prelude of the following rule is `foo, .bar`.
//! It is a **selector list** with two **selectors**, one for
//! `foo` elements and one for elements that have the `bar` class.
//!
//! The content of the block between `{}` for a qualified rule is a
//! **declaration list**.  The block of the first qualified rule contains two
//! **declarations**, one for the `fill` **property** and one for the
//! `stroke` property.
//!
//! After the first qualified rule, we have a second qualified rule with
//! a single selector for the `#baz` id, with a single declaration for the
//! `stroke-width` property.
//!
//! # Helper crates we use
//!
//! * `cssparser` crate as a CSS tokenizer, and some utilities to
//!   parse CSS rules and declarations.
//!
//! * `selectors` crate for the representation of selectors and
//!   selector lists, and for the matching engine.
//!
//! Both crates provide very generic implementations of their concepts,
//! and expect the caller to provide implementations of various traits,
//! and to provide types that represent certain things.
//!
//! For example, `cssparser` expects one to provide representations of
//! the following types:
//!
//! * A parsed CSS rule.  For `fill: blue;` we have
//!   `ParsedProperty::Fill(...)`.
//!
//! * A parsed selector list; we use `SelectorList` from the
//!   `selectors` crate.
//!
//! In turn, the `selectors` crate needs a way to navigate and examine
//! one's implementation of an element tree.  We provide `impl
//! selectors::Element for RsvgElement` for this.  This implementation
//! has methods like "does this element have the id `#foo`", or "give
//! me the next sibling element".
//!
//! Finally, the matching engine ties all of this together with
//! `matches_selector()`.  This takes an opaque representation of an
//! element, plus a selector, and returns a bool.  We iterate through
//! the rules in the stylesheets and gather the matches; then sort the
//! matches by specificity and apply the result to each element.

use cssparser::{
    self, AtRuleParser, BasicParseErrorKind, CowRcStr, DeclarationParser, Parser, ParserInput,
    ParserState, QualifiedRuleParser, RuleBodyItemParser, RuleBodyParser, SourceLocation,
    StyleSheetParser, ToCss, match_ignore_ascii_case, parse_important,
};
use language_tags::LanguageTag;
use markup5ever::{self, Namespace, QualName, ns};
use precomputed_hash::PrecomputedHash;
use selectors::attr::{AttrSelectorOperation, CaseSensitivity, NamespaceConstraint};
use selectors::bloom::BloomFilter;
use selectors::context::SelectorCaches;
use selectors::matching::{
    ElementSelectorFlags, MatchingContext, MatchingForInvalidation, MatchingMode,
    NeedsSelectorFlags, QuirksMode,
};
use selectors::parser::ParseRelative;
use selectors::{OpaqueElement, SelectorImpl, SelectorList};
use std::cmp::Ordering;
use std::fmt;
use std::str;
use std::str::FromStr;

use crate::element::Element;
use crate::error::*;
use crate::io;
use crate::node::{Node, NodeBorrow, NodeCascade};
use crate::properties::{ComputedValues, ParseAs, ParsedProperty, parse_value};
use crate::rsvg_log;
use crate::session::Session;
use crate::url_resolver::{AllowedUrl, UrlResolver};

/// A parsed CSS declaration
///
/// For example, in the declaration `fill: green !important`, the
/// `prop_name` would be `fill`, the `property` would be
/// `ParsedProperty::Fill(...)` with the green value, and `important`
/// would be `true`.
pub struct Declaration {
    pub prop_name: QualName,
    pub property: ParsedProperty,
    pub important: bool,
}

/// This enum represents the fact that a rule body can be either a
/// declaration or a nested rule.
pub enum RuleBodyItem {
    Decl(Declaration),
    #[allow(dead_code)] // We don't support nested rules yet
    Rule(Rule),
}

/// Dummy struct required to use `cssparser::DeclarationListParser`
///
/// It implements `cssparser::DeclarationParser`, which knows how to parse
/// the property/value pairs from a CSS declaration.
pub struct DeclParser;

impl<'i> DeclarationParser<'i> for DeclParser {
    type Declaration = RuleBodyItem;
    type Error = ValueErrorKind;

    /// Parses a CSS declaration like `name: input_value [!important]`
    fn parse_value<'t>(
        &mut self,
        name: CowRcStr<'i>,
        input: &mut Parser<'i, 't>,
        _declaration_start: &ParserState,
    ) -> Result<RuleBodyItem, cssparser::ParseError<'i, Self::Error>> {
        let prop_name = QualName::new(None, ns!(), markup5ever::LocalName::from(name.as_ref()));
        let property = parse_value(&prop_name, input, ParseAs::Property)?;

        let important = input.try_parse(parse_important).is_ok();

        Ok(RuleBodyItem::Decl(Declaration {
            prop_name,
            property,
            important,
        }))
    }
}

// cssparser's DeclarationListParser requires this; we just use the dummy
// implementations from cssparser itself.  We may want to provide a real
// implementation in the future, although this may require keeping track of the
// CSS parsing state like Servo does.
impl<'i> AtRuleParser<'i> for DeclParser {
    type Prelude = ();
    type AtRule = RuleBodyItem;
    type Error = ValueErrorKind;
}

/// We need this dummy implementation as well.
impl<'i> QualifiedRuleParser<'i> for DeclParser {
    type Prelude = ();
    type QualifiedRule = RuleBodyItem;
    type Error = ValueErrorKind;
}

impl<'i> RuleBodyItemParser<'i, RuleBodyItem, ValueErrorKind> for DeclParser {
    /// We want to parse declarations.
    fn parse_declarations(&self) -> bool {
        true
    }

    /// We don't wanto parse qualified rules though.
    fn parse_qualified(&self) -> bool {
        false
    }
}

/// Struct to implement cssparser::QualifiedRuleParser and cssparser::AtRuleParser
pub struct RuleParser {
    session: Session,
}

/// Errors from the CSS parsing process
#[allow(dead_code)] // looks like we are not actually using this yet?
#[derive(Debug)]
pub enum ParseErrorKind<'i> {
    Selector(selectors::parser::SelectorParseErrorKind<'i>),
}

impl<'i> From<selectors::parser::SelectorParseErrorKind<'i>> for ParseErrorKind<'i> {
    fn from(e: selectors::parser::SelectorParseErrorKind<'_>) -> ParseErrorKind<'_> {
        ParseErrorKind::Selector(e)
    }
}

/// A CSS qualified rule (or ruleset)
pub struct QualifiedRule {
    selectors: SelectorList<Selector>,
    declarations: Vec<Declaration>,
}

/// Prelude of at-rule used in the AtRuleParser.
pub enum AtRulePrelude {
    Import(String),
}

/// A CSS at-rule (or ruleset)
pub enum AtRule {
    Import(String),
}

/// A CSS rule (or ruleset)
pub enum Rule {
    AtRule(AtRule),
    QualifiedRule(QualifiedRule),
}

// Required to implement the `Prelude` associated type in `cssparser::QualifiedRuleParser`
impl<'i> selectors::Parser<'i> for RuleParser {
    type Impl = Selector;
    type Error = ParseErrorKind<'i>;

    fn default_namespace(&self) -> Option<<Self::Impl as SelectorImpl>::NamespaceUrl> {
        Some(ns!(svg))
    }

    fn namespace_for_prefix(
        &self,
        _prefix: &<Self::Impl as SelectorImpl>::NamespacePrefix,
    ) -> Option<<Self::Impl as SelectorImpl>::NamespaceUrl> {
        // FIXME: Do we need to keep a lookup table extracted from libxml2's
        // XML namespaces?
        //
        // Or are CSS namespaces completely different, declared elsewhere?
        None
    }
    fn parse_non_ts_pseudo_class(
        &self,
        location: SourceLocation,
        name: CowRcStr<'i>,
    ) -> Result<NonTSPseudoClass, cssparser::ParseError<'i, Self::Error>> {
        match &*name {
            "link" => Ok(NonTSPseudoClass::Link),
            "visited" => Ok(NonTSPseudoClass::Visited),
            _ => Err(location.new_custom_error(
                selectors::parser::SelectorParseErrorKind::UnsupportedPseudoClassOrElement(name),
            )),
        }
    }
    fn parse_non_ts_functional_pseudo_class(
        &self,
        name: CowRcStr<'i>,
        arguments: &mut Parser<'i, '_>,
        _after_part: bool,
    ) -> Result<NonTSPseudoClass, cssparser::ParseError<'i, Self::Error>> {
        match &*name {
            "lang" => {
                // Comma-separated lists of languages are a Selectors 4 feature,
                // but a pretty stable one that hasn't changed in a long time.
                let tags = arguments.parse_comma_separated(|arg| {
                    let language_tag = arg.expect_ident_or_string()?.clone();
                    LanguageTag::from_str(&language_tag).map_err(|_| {
                        arg.new_custom_error(selectors::parser::SelectorParseErrorKind::UnsupportedPseudoClassOrElement(language_tag))
                    })
                })?;
                arguments.expect_exhausted()?;
                Ok(NonTSPseudoClass::Lang(tags))
            }
            _ => Err(arguments.new_custom_error(
                selectors::parser::SelectorParseErrorKind::UnsupportedPseudoClassOrElement(name),
            )),
        }
    }
}

// `cssparser::StyleSheetParser` is a struct which requires that we provide a type that
// implements `cssparser::QualifiedRuleParser` and `cssparser::AtRuleParser`.
//
// In turn, `cssparser::QualifiedRuleParser` requires that we
// implement a way to parse the `Prelude` of a ruleset or rule.  For
// example, in this ruleset:
//
// ```css
// foo, .bar { fill: red; stroke: green; }
// ```
//
// The prelude is the selector list with the `foo` and `.bar` selectors.
//
// The `parse_prelude` method just uses `selectors::SelectorList`.  This
// is what requires the `impl selectors::Parser for RuleParser`.
//
// Next, the `parse_block` method takes an already-parsed prelude (a selector list),
// and tries to parse the block between braces.  It creates a `Rule` out of
// the selector list and the declaration list.
impl<'i> QualifiedRuleParser<'i> for RuleParser {
    type Prelude = SelectorList<Selector>;
    type QualifiedRule = Rule;
    type Error = ValueErrorKind;

    fn parse_prelude<'t>(
        &mut self,
        input: &mut Parser<'i, 't>,
    ) -> Result<Self::Prelude, cssparser::ParseError<'i, Self::Error>> {
        SelectorList::parse(self, input, ParseRelative::No).map_err(|e| ParseError {
            kind: cssparser::ParseErrorKind::Custom(ValueErrorKind::parse_error(
                "Could not parse selector",
            )),
            location: e.location,
        })
    }

    fn parse_block<'t>(
        &mut self,
        prelude: Self::Prelude,
        _start: &ParserState,
        input: &mut Parser<'i, 't>,
    ) -> Result<Self::QualifiedRule, cssparser::ParseError<'i, Self::Error>> {
        let declarations = RuleBodyParser::<_, _, Self::Error>::new(input, &mut DeclParser)
            .filter_map(|r| match r {
                Ok(RuleBodyItem::Decl(decl)) => Some(decl),
                Ok(RuleBodyItem::Rule(_)) => None,
                Err(e) => {
                    rsvg_log!(self.session, "Invalid declaration; ignoring: {:?}", e);
                    None
                }
            })
            .collect();

        Ok(Rule::QualifiedRule(QualifiedRule {
            selectors: prelude,
            declarations,
        }))
    }
}

// Required by `cssparser::StyleSheetParser`.
//
// This only handles the `@import` at-rule.
impl<'i> AtRuleParser<'i> for RuleParser {
    type Prelude = AtRulePrelude;
    type AtRule = Rule;
    type Error = ValueErrorKind;

    #[allow(clippy::type_complexity)]
    fn parse_prelude<'t>(
        &mut self,
        name: CowRcStr<'i>,
        input: &mut Parser<'i, 't>,
    ) -> Result<Self::Prelude, cssparser::ParseError<'i, Self::Error>> {
        match_ignore_ascii_case! {
            &name,

            // FIXME: at the moment we ignore media queries

            "import" => {
                let url = input.expect_url_or_string()?.as_ref().to_owned();
                Ok(AtRulePrelude::Import(url))
            },

            _ => Err(input.new_error(BasicParseErrorKind::AtRuleInvalid(name))),
        }
    }

    fn rule_without_block(
        &mut self,
        prelude: Self::Prelude,
        _start: &ParserState,
    ) -> Result<Self::AtRule, ()> {
        let AtRulePrelude::Import(url) = prelude;
        Ok(Rule::AtRule(AtRule::Import(url)))
    }

    // When we implement at-rules with blocks, implement the trait's parse_block() method here.
}

/// Dummy type required by the SelectorImpl trait.
#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NonTSPseudoClass {
    Link,
    Visited,
    Lang(Vec<LanguageTag>),
}

impl ToCss for NonTSPseudoClass {
    fn to_css<W>(&self, dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        match self {
            NonTSPseudoClass::Link => write!(dest, "link"),
            NonTSPseudoClass::Visited => write!(dest, "visited"),
            NonTSPseudoClass::Lang(lang) => write!(
                dest,
                "lang(\"{}\")",
                lang.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("\",\"")
            ),
        }
    }
}

impl selectors::parser::NonTSPseudoClass for NonTSPseudoClass {
    type Impl = Selector;

    fn is_active_or_hover(&self) -> bool {
        false
    }

    fn is_user_action_state(&self) -> bool {
        false
    }
}

/// Dummy type required by the SelectorImpl trait
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PseudoElement;

impl ToCss for PseudoElement {
    fn to_css<W>(&self, _dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        Ok(())
    }
}

impl selectors::parser::PseudoElement for PseudoElement {
    type Impl = Selector;
}

/// Holds all the types for the SelectorImpl trait
#[derive(Debug, Clone)]
pub struct Selector;

/// Wrapper for attribute values.
///
/// We use a newtype because the associated type Selector::AttrValue
/// must implement `From<&str>` and `ToCss`, which are foreign traits.
///
/// The `derive` requirements come from the `selectors` crate.
#[derive(Clone, PartialEq, Eq)]
pub struct AttributeValue(String);

impl From<&str> for AttributeValue {
    fn from(s: &str) -> AttributeValue {
        AttributeValue(s.to_owned())
    }
}

impl ToCss for AttributeValue {
    fn to_css<W>(&self, dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        use std::fmt::Write;

        write!(cssparser::CssStringWriter::new(dest), "{}", &self.0)
    }
}

impl AsRef<str> for AttributeValue {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

/// Wrapper for identifier values.
///
/// Used to implement `ToCss` on the `LocalName` foreign type.
#[derive(Clone, PartialEq, Eq)]
pub struct Identifier(markup5ever::LocalName);

impl From<&str> for Identifier {
    fn from(s: &str) -> Identifier {
        Identifier(markup5ever::LocalName::from(s))
    }
}

impl ToCss for Identifier {
    fn to_css<W>(&self, dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        cssparser::serialize_identifier(&self.0, dest)
    }
}

impl PrecomputedHash for Identifier {
    fn precomputed_hash(&self) -> u32 {
        self.0.precomputed_hash()
    }
}

/// Wrapper for local names.
///
/// Used to implement `ToCss` on the `LocalName` foreign type.
#[derive(Clone, PartialEq, Eq)]
pub struct LocalName(markup5ever::LocalName);

impl From<&str> for LocalName {
    fn from(s: &str) -> LocalName {
        LocalName(markup5ever::LocalName::from(s))
    }
}

impl ToCss for LocalName {
    fn to_css<W>(&self, dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        cssparser::serialize_identifier(&self.0, dest)
    }
}

impl PrecomputedHash for LocalName {
    fn precomputed_hash(&self) -> u32 {
        self.0.precomputed_hash()
    }
}

/// Wrapper for namespace prefixes.
///
/// Used to implement `ToCss` on the `markup5ever::Prefix` foreign type.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct NamespacePrefix(markup5ever::Prefix);

impl From<&str> for NamespacePrefix {
    fn from(s: &str) -> NamespacePrefix {
        NamespacePrefix(markup5ever::Prefix::from(s))
    }
}

impl ToCss for NamespacePrefix {
    fn to_css<W>(&self, dest: &mut W) -> fmt::Result
    where
        W: fmt::Write,
    {
        cssparser::serialize_identifier(&self.0, dest)
    }
}

impl SelectorImpl for Selector {
    type ExtraMatchingData<'a> = ();
    type AttrValue = AttributeValue;
    type Identifier = Identifier;
    type LocalName = LocalName;
    type NamespaceUrl = Namespace;
    type NamespacePrefix = NamespacePrefix;
    type BorrowedNamespaceUrl = Namespace;
    type BorrowedLocalName = LocalName;
    type NonTSPseudoClass = NonTSPseudoClass;
    type PseudoElement = PseudoElement;
}

/// Newtype wrapper around `Node` so we can implement [`selectors::Element`] for it.
///
/// `Node` is an alias for [`rctree::Node`], so we can't implement
/// `selectors::Element` directly on it.  We implement it on the
/// `RsvgElement` wrapper instead.
#[derive(Clone, PartialEq)]
pub struct RsvgElement(Node);

impl From<Node> for RsvgElement {
    fn from(n: Node) -> RsvgElement {
        RsvgElement(n)
    }
}

impl fmt::Debug for RsvgElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.borrow())
    }
}

// The selectors crate uses this to examine our tree of elements.
impl selectors::Element for RsvgElement {
    type Impl = Selector;

    /// Converts self into an opaque representation.
    fn opaque(&self) -> OpaqueElement {
        // The `selectors` crate uses this value just for pointer comparisons, to answer
        // the question, "is this element the same as that one?".  So, we'll give it a
        // reference to our actual node's data, i.e. skip over whatever wrappers there
        // are in rctree.
        //
        // We use an explicit type here to make it clear what the type is; otherwise you
        // may be fooled by the fact that borrow_element() returns a Ref<Element>, not a
        // plain reference: &Ref<T> is transient and would get dropped at the end of this
        // function, but we want something long-lived.
        let element: &Element = &self.0.borrow_element();
        OpaqueElement::new::<Element>(element)
    }

    fn parent_element(&self) -> Option<Self> {
        self.0.parent().map(|n| n.into())
    }

    /// Whether the parent node of this element is a shadow root.
    fn parent_node_is_shadow_root(&self) -> bool {
        // unsupported
        false
    }

    /// The host of the containing shadow root, if any.
    fn containing_shadow_host(&self) -> Option<Self> {
        // unsupported
        None
    }

    /// Whether we're matching on a pseudo-element.
    fn is_pseudo_element(&self) -> bool {
        // unsupported
        false
    }

    /// Skips non-element nodes
    fn prev_sibling_element(&self) -> Option<Self> {
        let mut sibling = self.0.previous_sibling();

        while let Some(ref sib) = sibling {
            if sib.is_element() {
                return sibling.map(|n| n.into());
            }

            sibling = sib.previous_sibling();
        }

        None
    }

    /// Skips non-element nodes
    fn next_sibling_element(&self) -> Option<Self> {
        let mut sibling = self.0.next_sibling();

        while let Some(ref sib) = sibling {
            if sib.is_element() {
                return sibling.map(|n| n.into());
            }

            sibling = sib.next_sibling();
        }

        None
    }

    fn is_html_element_in_html_document(&self) -> bool {
        false
    }

    fn has_local_name(&self, local_name: &LocalName) -> bool {
        self.0.borrow_element().element_name().local == local_name.0
    }

    /// Empty string for no namespace
    fn has_namespace(&self, ns: &Namespace) -> bool {
        self.0.borrow_element().element_name().ns == *ns
    }

    /// Whether this element and the `other` element have the same local name and namespace.
    fn is_same_type(&self, other: &Self) -> bool {
        self.0.borrow_element().element_name() == other.0.borrow_element().element_name()
    }

    fn attr_matches(
        &self,
        ns: &NamespaceConstraint<&Namespace>,
        local_name: &LocalName,
        operation: &AttrSelectorOperation<&AttributeValue>,
    ) -> bool {
        self.0
            .borrow_element()
            .get_attributes()
            .iter()
            .find(|(attr, _)| {
                // do we have an attribute that matches the namespace and local_name?
                match *ns {
                    NamespaceConstraint::Any => local_name.0 == attr.local,
                    NamespaceConstraint::Specific(ns) => {
                        QualName::new(None, ns.clone(), local_name.0.clone()) == *attr
                    }
                }
            })
            .map(|(_, value)| {
                // we have one; does the attribute's value match the expected operation?
                operation.eval_str(value)
            })
            .unwrap_or(false)
    }

    fn match_non_ts_pseudo_class(
        &self,
        pc: &<Self::Impl as SelectorImpl>::NonTSPseudoClass,
        _context: &mut MatchingContext<'_, Self::Impl>,
    ) -> bool
where {
        match pc {
            NonTSPseudoClass::Link => self.is_link(),
            NonTSPseudoClass::Visited => false,
            NonTSPseudoClass::Lang(css_lang) => self
                .0
                .borrow_element()
                .get_computed_values()
                .xml_lang()
                .0
                .as_ref()
                .is_some_and(|e_lang| {
                    css_lang
                        .iter()
                        .any(|l| l.is_language_range() && l.matches(e_lang))
                }),
        }
    }

    fn match_pseudo_element(
        &self,
        _pe: &<Self::Impl as SelectorImpl>::PseudoElement,
        _context: &mut MatchingContext<'_, Self::Impl>,
    ) -> bool {
        // unsupported
        false
    }

    /// Whether this element is a `link`.
    fn is_link(&self) -> bool {
        // Style as link only if href is specified at all.
        //
        // The SVG and CSS specifications do not seem to clearly
        // say what happens when you have an `<svg:a>` tag with no
        // `(xlink:|svg:)href` attribute. However, both Firefox and Chromium
        // consider a bare `<svg:a>` element with no href to be NOT
        // a link, so to avoid nasty surprises, we do the same.
        // Empty href's, however, ARE considered links.
        self.0.is_element()
            && match *self.0.borrow_element_data() {
                crate::element::ElementData::Link(ref link) => link.link.is_some(),
                _ => false,
            }
    }

    /// Returns whether the element is an HTML `<slot>` element.
    fn is_html_slot_element(&self) -> bool {
        false
    }

    fn has_id(&self, id: &Identifier, case_sensitivity: CaseSensitivity) -> bool {
        self.0
            .borrow_element()
            .get_id()
            .map(|self_id| case_sensitivity.eq(self_id.as_bytes(), id.0.as_bytes()))
            .unwrap_or(false)
    }

    fn has_class(&self, name: &Identifier, case_sensitivity: CaseSensitivity) -> bool {
        self.0
            .borrow_element()
            .get_class()
            .map(|classes| {
                classes
                    .split_whitespace()
                    .any(|class| case_sensitivity.eq(class.as_bytes(), name.0.as_bytes()))
            })
            .unwrap_or(false)
    }

    fn has_custom_state(&self, _name: &<Self::Impl as SelectorImpl>::Identifier) -> bool {
        false
    }

    fn imported_part(&self, _name: &Identifier) -> Option<Identifier> {
        // unsupported
        None
    }

    fn is_part(&self, _name: &Identifier) -> bool {
        // unsupported
        false
    }

    /// Returns whether this element matches `:empty`.
    ///
    /// That is, whether it does not contain any child element or any non-zero-length text node.
    /// See <http://dev.w3.org/csswg/selectors-3/#empty-pseudo>.
    fn is_empty(&self) -> bool {
        // .all() returns true for the empty iterator
        self.0
            .children()
            .all(|child| child.is_chars() && child.borrow_chars().is_empty())
    }

    /// Returns whether this element matches `:root`,
    /// i.e. whether it is the root element of a document.
    ///
    /// Note: this can be false even if `.parent_element()` is `None`
    /// if the parent node is a `DocumentFragment`.
    fn is_root(&self) -> bool {
        self.0.parent().is_none()
    }

    fn add_element_unique_hashes(&self, _filter: &mut BloomFilter) -> bool {
        false
    }

    /// Returns the first child element of this element.
    fn first_element_child(&self) -> Option<Self> {
        self.0
            .children()
            .find(|child| child.is_element())
            .map(|n| n.into())
    }

    /// Applies the given selector flags to this element.
    fn apply_selector_flags(&self, _: ElementSelectorFlags) {
        todo!()
    }
}

/// Origin for a stylesheet, per CSS 2.2.
///
/// This is used when sorting selector matches according to their origin and specificity.
///
/// CSS2.2: <https://www.w3.org/TR/CSS22/cascade.html#cascading-order>
#[derive(Copy, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub enum Origin {
    UserAgent,
    User,
    Author,
}

/// A parsed CSS stylesheet.
pub struct Stylesheet {
    origin: Origin,
    qualified_rules: Vec<QualifiedRule>,
}

/// A match during the selector matching process
///
/// This struct comes from [`Stylesheet::get_matches`], and represents
/// that a certain node matched a CSS rule which has a selector with a
/// certain `specificity`.  The stylesheet's `origin` is also given here.
///
/// This type implements [`Ord`] so a list of `Match` can be sorted.
/// That implementation does ordering based on origin and specificity
/// as per <https://www.w3.org/TR/CSS22/cascade.html#cascading-order>.
struct Match<'a> {
    specificity: u32,
    origin: Origin,
    declaration: &'a Declaration,
}

impl<'a> Ord for Match<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.origin.cmp(&other.origin) {
            Ordering::Equal => self.specificity.cmp(&other.specificity),
            o => o,
        }
    }
}

impl<'a> PartialOrd for Match<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialEq for Match<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.origin == other.origin && self.specificity == other.specificity
    }
}

impl<'a> Eq for Match<'a> {}

impl Stylesheet {
    fn empty(origin: Origin) -> Stylesheet {
        Stylesheet {
            origin,
            qualified_rules: Vec::new(),
        }
    }

    /// Parses a new stylesheet from CSS data in a string.
    ///
    /// The `url_resolver_url` is required for `@import` rules, so that librsvg can determine if
    /// the requested path is allowed.
    pub fn from_data(
        buf: &str,
        url_resolver: &UrlResolver,
        origin: Origin,
        session: Session,
    ) -> Result<Self, LoadingError> {
        let mut stylesheet = Stylesheet::empty(origin);
        stylesheet.add_rules_from_string(buf, url_resolver, session)?;
        Ok(stylesheet)
    }

    /// Parses a new stylesheet by loading CSS data from a URL.
    pub fn from_href(
        aurl: &AllowedUrl,
        origin: Origin,
        session: Session,
    ) -> Result<Self, LoadingError> {
        let mut stylesheet = Stylesheet::empty(origin);
        stylesheet.load(aurl, session)?;
        Ok(stylesheet)
    }

    /// Parses the CSS rules in `buf` and appends them to the stylesheet.
    ///
    /// The `url_resolver_url` is required for `@import` rules, so that librsvg can determine if
    /// the requested path is allowed.
    ///
    /// If there is an `@import` rule, its rules will be recursively added into the
    /// stylesheet, in the order in which they appear.
    fn add_rules_from_string(
        &mut self,
        buf: &str,
        url_resolver: &UrlResolver,
        session: Session,
    ) -> Result<(), LoadingError> {
        let mut input = ParserInput::new(buf);
        let mut parser = Parser::new(&mut input);
        let mut rule_parser = RuleParser {
            session: session.clone(),
        };

        StyleSheetParser::new(&mut parser, &mut rule_parser)
            .filter_map(|r| match r {
                Ok(rule) => Some(rule),
                Err(e) => {
                    rsvg_log!(session, "Invalid rule; ignoring: {:?}", e);
                    None
                }
            })
            .for_each(|rule| match rule {
                Rule::AtRule(AtRule::Import(url)) => match url_resolver.resolve_href(&url) {
                    Ok(aurl) => {
                        // ignore invalid imports
                        let _ = self.load(&aurl, session.clone());
                    }

                    Err(e) => {
                        rsvg_log!(session, "Not loading stylesheet from \"{}\": {}", url, e);
                    }
                },

                Rule::QualifiedRule(qr) => self.qualified_rules.push(qr),
            });

        Ok(())
    }

    /// Parses a stylesheet referenced by an URL
    fn load(&mut self, aurl: &AllowedUrl, session: Session) -> Result<(), LoadingError> {
        io::acquire_data(aurl, None)
            .map_err(LoadingError::from)
            .and_then(|data| {
                String::from_utf8(data.data).map_err(|_| {
                    rsvg_log!(
                        session,
                        "\"{}\" does not contain valid UTF-8 CSS data; ignoring",
                        aurl
                    );
                    LoadingError::BadCss
                })
            })
            .and_then(|utf8| {
                let url = (**aurl).clone();
                self.add_rules_from_string(&utf8, &UrlResolver::new(Some(url)), session)
            })
    }

    /// Appends the style declarations that match a specified node to a given vector
    fn get_matches<'a>(
        &'a self,
        node: &Node,
        match_ctx: &mut MatchingContext<'_, Selector>,
        acc: &mut Vec<Match<'a>>,
    ) {
        for rule in &self.qualified_rules {
            for selector in rule.selectors.slice() {
                // This magic call is stolen from selectors::matching::matches_selector_list()
                let matches = selectors::matching::matches_selector(
                    selector,
                    0,
                    None,
                    &RsvgElement(node.clone()),
                    match_ctx,
                );

                if matches {
                    for decl in rule.declarations.iter() {
                        acc.push(Match {
                            declaration: decl,
                            specificity: selector.specificity(),
                            origin: self.origin,
                        });
                    }
                }
            }
        }
    }
}

/// Runs the CSS cascade on the specified tree from all the stylesheets
pub fn cascade(
    root: &mut Node,
    ua_stylesheets: &[Stylesheet],
    author_stylesheets: &[Stylesheet],
    user_stylesheets: &[Stylesheet],
    session: &Session,
) {
    for mut node in root.descendants().filter(|n| n.is_element()) {
        let mut matches = Vec::new();

        // xml:lang needs to be inherited before selector matching, so it
        // can't be done in the usual SpecifiedValues::to_computed_values,
        // which is called by cascade() and runs after matching.
        let parent = node.parent().clone();
        node.borrow_element_mut().inherit_xml_lang(parent);

        let mut caches = SelectorCaches::default();
        let mut match_ctx = MatchingContext::new(
            MatchingMode::Normal,
            // FIXME: how the fuck does one set up a bloom filter here?
            None,
            &mut caches,
            QuirksMode::NoQuirks,
            NeedsSelectorFlags::No,
            MatchingForInvalidation::No,
        );

        for s in ua_stylesheets
            .iter()
            .chain(author_stylesheets)
            .chain(user_stylesheets)
        {
            s.get_matches(&node, &mut match_ctx, &mut matches);
        }

        matches.as_mut_slice().sort();

        let mut element = node.borrow_element_mut();

        for m in matches {
            element.apply_style_declaration(m.declaration, m.origin);
        }

        element.set_style_attribute(session);
    }

    let values = ComputedValues::default();
    root.cascade(&values);
}

#[cfg(test)]
mod tests {
    use super::*;
    use selectors::Element;

    use crate::document::Document;
    use crate::is_element_of_type;

    #[test]
    fn xml_lang() {
        let document = Document::load_from_bytes(
            br#"<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" xml:lang="zh">
  <text id="a" x="10" y="10" width="30" height="30"></text>
  <text id="b" x="10" y="20" width="30" height="30" xml:lang="en"></text>
</svg>
"#,
        );
        let a = document.lookup_internal_node("a").unwrap();
        assert_eq!(
            a.borrow_element()
                .get_computed_values()
                .xml_lang()
                .0
                .unwrap()
                .as_str(),
            "zh"
        );
        let b = document.lookup_internal_node("b").unwrap();
        assert_eq!(
            b.borrow_element()
                .get_computed_values()
                .xml_lang()
                .0
                .unwrap()
                .as_str(),
            "en"
        );
    }

    #[test]
    fn impl_element() {
        let document = Document::load_from_bytes(
            br#"<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" id="a">
  <rect id="b" x="10" y="10" width="30" height="30"/>
  <circle id="c" cx="10" cy="10" r="10"/>
  <rect id="d" class="foo bar"/>
</svg>
"#,
        );

        let a = document.lookup_internal_node("a").unwrap();
        let b = document.lookup_internal_node("b").unwrap();
        let c = document.lookup_internal_node("c").unwrap();
        let d = document.lookup_internal_node("d").unwrap();

        // Node types
        assert!(is_element_of_type!(a, Svg));
        assert!(is_element_of_type!(b, Rect));
        assert!(is_element_of_type!(c, Circle));
        assert!(is_element_of_type!(d, Rect));

        let a = RsvgElement(a);
        let b = RsvgElement(b);
        let c = RsvgElement(c);
        let d = RsvgElement(d);

        // Tree navigation

        assert_eq!(a.parent_element(), None);
        assert_eq!(b.parent_element(), Some(a.clone()));
        assert_eq!(c.parent_element(), Some(a.clone()));
        assert_eq!(d.parent_element(), Some(a.clone()));

        assert_eq!(b.next_sibling_element(), Some(c.clone()));
        assert_eq!(c.next_sibling_element(), Some(d.clone()));
        assert_eq!(d.next_sibling_element(), None);

        assert_eq!(b.prev_sibling_element(), None);
        assert_eq!(c.prev_sibling_element(), Some(b.clone()));
        assert_eq!(d.prev_sibling_element(), Some(c.clone()));

        // Other operations

        assert!(a.has_local_name(&LocalName::from("svg")));

        assert!(a.has_namespace(&ns!(svg)));

        assert!(!a.is_same_type(&b));
        assert!(b.is_same_type(&d));

        assert!(a.has_id(
            &Identifier::from("a"),
            CaseSensitivity::AsciiCaseInsensitive
        ));
        assert!(!b.has_id(
            &Identifier::from("foo"),
            CaseSensitivity::AsciiCaseInsensitive
        ));

        assert!(d.has_class(
            &Identifier::from("foo"),
            CaseSensitivity::AsciiCaseInsensitive
        ));
        assert!(d.has_class(
            &Identifier::from("bar"),
            CaseSensitivity::AsciiCaseInsensitive
        ));

        assert!(!a.has_class(
            &Identifier::from("foo"),
            CaseSensitivity::AsciiCaseInsensitive
        ));

        assert!(d.is_empty());
        assert!(!a.is_empty());
    }
}
