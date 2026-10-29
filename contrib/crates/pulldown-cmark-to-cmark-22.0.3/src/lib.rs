//! Convert `pulldown-cmark` `Event`s back to the string they were parsed from.
//!
//! This crate provides functions to serialize markdown events back into markdown text format.
//!
//! # Examples
//!
//! ```rust
//! use pulldown_cmark::Parser;
//! use pulldown_cmark_to_cmark::cmark;
//!
//! let input_markdown = "# Hello\n\nWorld!";
//! let events = Parser::new(input_markdown);
//! let mut output_markdown = String::new();
//! cmark(events, &mut output_markdown).unwrap();
//! assert_eq!(output_markdown, input_markdown);
//! ```

#![deny(rust_2018_idioms)]
#![deny(missing_docs)]

use std::{
    borrow::{Borrow, Cow},
    collections::HashSet,
    fmt,
    ops::Range,
};

use pulldown_cmark::{Alignment as TableAlignment, BlockQuoteKind, Event, LinkType, MetadataBlockKind, Tag, TagEnd};

mod source_range;
mod text_modifications;

pub use source_range::{
    cmark_resume_with_source_range, cmark_resume_with_source_range_and_options, cmark_with_source_range,
    cmark_with_source_range_and_options,
};
use text_modifications::*;

/// Similar to [Pulldown-Cmark-Alignment][Alignment], but with required
/// traits for comparison to allow testing.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Alignment {
    /// No alignment specified
    None,
    /// Left-aligned
    Left,
    /// Center-aligned
    Center,
    /// Right-aligned
    Right,
}

impl<'a> From<&'a TableAlignment> for Alignment {
    fn from(s: &'a TableAlignment) -> Self {
        match *s {
            TableAlignment::None => Self::None,
            TableAlignment::Left => Self::Left,
            TableAlignment::Center => Self::Center,
            TableAlignment::Right => Self::Right,
        }
    }
}

/// The kind of code block being serialized.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CodeBlockKind {
    /// An indented code block (4 spaces or 1 tab)
    Indented,
    /// A fenced code block (delimited by backticks or tildes)
    Fenced,
}

/// How a closed list item's content ended.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ItemTail {
    /// The item ended with inline text. An unindented next line would continue it.
    ///
    /// For example:
    ///
    /// ```md
    /// 1. item
    ///    * a
    ///    * b
    ///    para
    /// ```
    ///
    /// Here, `* b` ends in `b`, so `para` is attached to the nested item.
    OpenParagraph,
    /// The item ended without any content.
    ///
    /// If the next line is indented to the column where content would go, it
    /// would become the item's content.
    ///
    /// For example:
    ///
    /// ```md
    /// 1. item
    ///    * a
    ///    *
    ///      code
    /// ```
    ///
    /// The second `*` is empty, so the indented `code` is attached to the
    /// nested item.
    Empty,
    /// The item ended in a closed block (e.g. fenced code, HTML). Nothing can
    /// continue it.
    ///
    /// For example:
    ///
    /// ```md
    /// 1. item
    ///    * a
    ///      ```
    ///      x
    ///      ```
    ///    para
    /// ```
    ///
    /// Here, `para` is never attached to the nested list.
    ClosedBlock,
}

/// Information about the previous event that the following event cares about.
#[derive(Copy, Clone, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum LastEvent {
    /// Any event not covered by another variant.
    #[default]
    Other,
    /// Text or other inline content.
    InlineContent,
    /// `Event::Start(Tag::Item)`.
    ItemStart,
    /// `Event::End(TagEnd::Item)`.
    ItemEnd(ItemTail),
    /// `Event::End(TagEnd::List(_))` for a list nested in another list.
    NestedListEnd(ItemTail),
}

/// What the serializer knows so far about whether a list is tight.
///
/// If an item directly contains inline content, it is always tight.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ListSpacing {
    /// The list might be either tight or loose.
    Unknown,
    /// The list is known to be tight.
    Tight,
}

/// A tag that has started but not yet ended.
///
/// Part of [`State::open_tags`], which maintains a stack of such tags.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum OpenTag {
    /// `Tag::List`.
    List(ListSpacing),
    /// `Tag::Item`.
    Item,
    /// Some other tag.
    Other,
}

/// The state of the [`cmark_resume()`] and [`cmark_resume_with_options()`] functions.
/// This does not only allow introspection, but enables the user
/// to halt the serialization at any time, and resume it later.
#[derive(Clone, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct State<'a> {
    /// The amount of newlines to insert after `Event::Start(...)`
    pub newlines_before_start: usize,
    /// The lists and their types for which we have seen a `Event::Start(List(...))` tag
    pub list_stack: Vec<Option<u64>>,
    /// The computed padding and prefix to print after each newline.
    /// This changes with the level of `BlockQuote` and `List` events.
    pub padding: Vec<Cow<'a, str>>,
    /// Keeps the current table alignments, if we are currently serializing a table.
    pub table_alignments: Vec<Alignment>,
    /// Keeps the current table headers, if we are currently serializing a table.
    pub table_headers: Vec<String>,
    /// The last seen text when serializing a header
    pub text_for_header: Option<String>,
    /// Is set while we are handling text in a code block
    pub code_block: Option<CodeBlockKind>,
    /// True if the last event was text and the text does not have trailing newline. Used to inject additional newlines before code block end fence.
    pub last_was_text_without_trailing_newline: bool,
    /// True if the preceding HTML block already wrote a newline with the current padding.
    pub html_block_has_trailing_newline: bool,
    /// True if the last event was a paragraph start. Used to escape spaces at start of line (prevent spurrious indented code).
    pub last_was_paragraph_start: bool,
    /// True if the next event is a link, image, or footnote.
    pub next_is_link_like: bool,
    /// Currently open links
    pub link_stack: Vec<LinkCategory<'a>>,
    /// Currently open images
    pub image_stack: Vec<ImageLink<'a>>,
    /// Keeps track of the last seen heading's id, classes, and attributes
    pub current_heading: Option<Heading<'a>>,
    /// True whenever between `Start(TableCell)` and `End(TableCell)`
    pub in_table_cell: bool,

    /// Keeps track of the last seen shortcut/link
    pub current_shortcut_text: Option<String>,
    /// A list of shortcuts seen so far for later emission
    pub shortcuts: Vec<(String, String, String)>,
    /// Index into the `source` bytes of the end of the range corresponding to the last event.
    ///
    /// It's used to see if the current event didn't capture some bytes because of a
    /// skipped-over backslash.
    pub last_event_end_index: usize,
    /// Information about the previous event.
    //
    // It is possible to fold last_was_paragraph_start and
    // last_was_text_without_trailing_newline into this field -- this should be
    // done the next time the crate has a breaking change.
    pub last_event: LastEvent,
    /// A stack of tags that have started but not yet ended.
    pub open_tags: Vec<OpenTag>,
    /// A `>` line to write before the next child of a tight list item, to
    /// close a block quote that just ended.
    ///
    /// Consider this tight list:
    ///
    /// ```md
    /// * item
    ///   * a
    ///      > q
    ///      >
    ///   text
    /// ```
    ///
    /// The empty `>` line is required. Without it, we would write:
    ///
    /// ```md
    /// * item
    ///   * a
    ///      > q
    ///   text
    /// ```
    ///
    /// i.e., `text` would be a lazy continuation of `q`, and end up inside the
    /// block quote.
    ///
    /// But the `>` line isn't always needed. Here, it isn't, because a new
    /// list item can't continue `q`:
    ///
    /// ```md
    /// * item
    ///   * a
    ///      > q
    ///   * c
    /// ```
    ///
    /// The two lists only differ in the line after the block quote, so when the
    /// block quote ends, we can't tell which one we're in. We don't write the `>`
    /// line yet. Instead, we store it here: in both examples, this is set to
    /// `Some("     >")`. Then, we decide on seeing the next child:
    ///
    /// * `text` isn't a list item, so it could continue `q`. We write this line
    ///   before it.
    /// * `* c` is a list item, so it can't. We don't write this line.
    ///
    /// Either way, this goes back to `None`.
    ///
    /// We store the whole line, including indentation, because by the time
    /// `text` arrives, `a` has ended, and its indentation is gone from
    /// [`State::padding`].
    ///
    /// This is also `None`:
    ///
    /// * when a block quote ends outside a list item, e.g. `> q` at the top
    ///   level. The `>` line is only ever needed inside a tight list item.
    /// * after the outermost list item ends.
    /// * after a definition ends and writes a newline outside the quote.
    pub pending_block_quote_end_line: Option<String>,
    /// A definition marker waiting for its first child to determine whether it needs a blank line.
    pub pending_definition_list_marker: bool,
}

/// The category of link being serialized.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LinkCategory<'a> {
    /// An autolink (e.g., `<http://example.com>`)
    AngleBracketed,
    /// A reference link with an explicit label (e.g., `[text][label]`)
    Reference {
        /// The destination URI
        uri: Cow<'a, str>,
        /// The link title
        title: Cow<'a, str>,
        /// The reference identifier
        id: Cow<'a, str>,
    },
    /// A collapsed reference link (e.g., `[text][]`)
    Collapsed {
        /// The destination URI
        uri: Cow<'a, str>,
        /// The link title
        title: Cow<'a, str>,
    },
    /// A shortcut reference link (e.g., `[text]`)
    Shortcut {
        /// The destination URI
        uri: Cow<'a, str>,
        /// The link title
        title: Cow<'a, str>,
    },
    /// An inline link or other link type
    Other {
        /// The destination URI
        uri: Cow<'a, str>,
        /// The link title
        title: Cow<'a, str>,
    },
}

/// The category of image link being serialized.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ImageLink<'a> {
    /// A reference image with an explicit label (e.g., `![alt][label]`)
    Reference {
        /// The destination URI
        uri: Cow<'a, str>,
        /// The image title
        title: Cow<'a, str>,
        /// The reference identifier
        id: Cow<'a, str>,
    },
    /// A collapsed reference image (e.g., `![alt][]`)
    Collapsed {
        /// The destination URI
        uri: Cow<'a, str>,
        /// The image title
        title: Cow<'a, str>,
    },
    /// A shortcut reference image (e.g., `![alt]`)
    Shortcut {
        /// The destination URI
        uri: Cow<'a, str>,
        /// The image title
        title: Cow<'a, str>,
    },
    /// An inline image or other image type
    Other {
        /// The destination URI
        uri: Cow<'a, str>,
        /// The image title
        title: Cow<'a, str>,
    },
}

/// Information about a heading's attributes (id, classes, and other attributes).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Heading<'a> {
    /// The heading's id attribute, or `None` if no id is specified
    id: Option<Cow<'a, str>>,
    /// The heading's CSS class attributes; empty if no classes are specified
    classes: Vec<Cow<'a, str>>,
    /// Other attributes as key-value pairs in the form (attribute_name, optional_value)
    attributes: Vec<(Cow<'a, str>, Option<Cow<'a, str>>)>,
}

/// Thea mount of code-block tokens one needs to produce a valid fenced code-block.
pub const DEFAULT_CODE_BLOCK_TOKEN_COUNT: usize = 3;

/// Configuration for the [`cmark_with_options()`] and [`cmark_resume_with_options()`] functions.
/// The defaults should provide decent spacing and most importantly, will
/// provide a faithful rendering of your markdown document particularly when
/// rendering it to HTML.
///
/// It's best used with its `Options::default()` implementation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Options<'a> {
    /// The number of newlines to insert after a headline
    pub newlines_after_headline: usize,
    /// The number of newlines to insert after a paragraph
    pub newlines_after_paragraph: usize,
    /// The number of newlines to insert after a code block
    pub newlines_after_codeblock: usize,
    /// The number of newlines to insert after an HTML block
    pub newlines_after_htmlblock: usize,
    /// The number of newlines to insert after a table
    pub newlines_after_table: usize,
    /// The number of newlines to insert after a horizontal rule
    pub newlines_after_rule: usize,
    /// The number of newlines to insert after a list
    pub newlines_after_list: usize,
    /// The number of newlines to insert after a block quote
    pub newlines_after_blockquote: usize,
    /// The number of newlines to insert after other elements
    pub newlines_after_rest: usize,
    /// The amount of newlines placed after TOML or YAML metadata blocks at the beginning of a document.
    pub newlines_after_metadata: usize,
    /// Token count for fenced code block. An appropriate value of this field can be decided by
    /// [`calculate_code_block_token_count()`].
    /// Note that the default value is `4` which allows for one level of nested code-blocks,
    /// which is typically a safe value for common kinds of markdown documents.
    pub code_block_token_count: usize,
    /// The character to use for code block fences (backtick or tilde)
    pub code_block_token: char,
    /// The character to use for unordered list items
    pub list_token: char,
    /// The character to use after ordered list numbers (e.g., '.' for `1.`)
    pub ordered_list_token: char,
    /// Whether to increment the number for each ordered list item
    pub increment_ordered_list_bullets: bool,
    /// The character to use for emphasis (italic)
    pub emphasis_token: char,
    /// The string to use for strong emphasis (bold)
    pub strong_token: &'a str,
    /// If `true` (default) then use HTML tags `<sup>` and `<sub>`.
    /// If `false`, use the Markdown symbols `^` and `~` instead.
    ///
    /// If you use [`ENABLE_SUPERSCRIPT`](pulldown_cmark::Options::ENABLE_SUPERSCRIPT) and
    /// [`ENABLE_SUBSCRIPT`](pulldown_cmark::Options::ENABLE_SUBSCRIPT) when parsing, then
    /// you might need this in order to round-trip Markdown byte-for-byte, with knowledge
    /// of whether the parsed documents use `<sub>`/`<sup>` or `^`/`~` instead.
    pub use_html_for_super_sub_script: bool,
}

const DEFAULT_OPTIONS: Options<'_> = Options {
    newlines_after_headline: 2,
    newlines_after_paragraph: 2,
    newlines_after_codeblock: 2,
    newlines_after_htmlblock: 1,
    newlines_after_table: 2,
    newlines_after_rule: 2,
    newlines_after_list: 2,
    newlines_after_blockquote: 2,
    newlines_after_rest: 1,
    newlines_after_metadata: 1,
    code_block_token_count: 4,
    code_block_token: '`',
    list_token: '*',
    ordered_list_token: '.',
    increment_ordered_list_bullets: false,
    emphasis_token: '*',
    strong_token: "**",
    use_html_for_super_sub_script: true,
};

impl Default for Options<'_> {
    fn default() -> Self {
        DEFAULT_OPTIONS
    }
}

impl Options<'_> {
    /// Returns the set of special characters that need escaping based on the current options.
    pub fn special_characters(&self) -> Cow<'static, str> {
        // These always need to be escaped, even if reconfigured.
        const BASE: &str = "#\\_*<>`|[]";
        if DEFAULT_OPTIONS.code_block_token == self.code_block_token
            && DEFAULT_OPTIONS.list_token == self.list_token
            && DEFAULT_OPTIONS.emphasis_token == self.emphasis_token
            && DEFAULT_OPTIONS.strong_token == self.strong_token
        {
            BASE.into()
        } else {
            let mut s = String::from(BASE);
            s.push(self.code_block_token);
            s.push(self.list_token);
            s.push(self.emphasis_token);
            s.push_str(self.strong_token);
            s.into()
        }
    }
}

/// The error returned by [`cmark_resume_with_options()`] and
/// [`cmark_resume_with_source_range_and_options()`].
#[derive(Debug)]
pub enum Error {
    /// Formatting to the output writer failed
    FormatFailed(fmt::Error),
    /// An event was encountered that cannot be produced by valid markdown
    UnexpectedEvent,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FormatFailed(e) => e.fmt(f),
            Self::UnexpectedEvent => f.write_str("Unexpected event while reconstructing Markdown"),
        }
    }
}

impl std::error::Error for Error {}

impl From<fmt::Error> for Error {
    fn from(e: fmt::Error) -> Self {
        Self::FormatFailed(e)
    }
}

/// As [`cmark_with_options()`], but with default [`Options`].
pub fn cmark<'a, I, E, F>(events: I, mut formatter: F) -> Result<State<'a>, Error>
where
    I: Iterator<Item = E>,
    E: Borrow<Event<'a>>,
    F: fmt::Write,
{
    cmark_with_options(events, &mut formatter, Default::default())
}

/// As [`cmark_resume_with_options()`], but with default [`Options`].
pub fn cmark_resume<'a, I, E, F>(events: I, formatter: F, state: Option<State<'a>>) -> Result<State<'a>, Error>
where
    I: Iterator<Item = E>,
    E: Borrow<Event<'a>>,
    F: fmt::Write,
{
    cmark_resume_with_options(events, formatter, state, Options::default())
}

/// As [`cmark_resume_with_options()`], but with the [`State`] finalized.
pub fn cmark_with_options<'a, I, E, F>(events: I, mut formatter: F, options: Options<'_>) -> Result<State<'a>, Error>
where
    I: Iterator<Item = E>,
    E: Borrow<Event<'a>>,
    F: fmt::Write,
{
    let state = cmark_resume_with_options(events, &mut formatter, Default::default(), options)?;
    state.finalize(formatter)
}

/// Serialize a stream of [pulldown-cmark-Events][Event] into a string-backed buffer.
///
/// 1. **events**
///    * An iterator over [`Events`][Event], for example as returned by the [`Parser`][pulldown_cmark::Parser]
/// 1. **formatter**
///    * A format writer, can be a `String`.
/// 1. **state**
///    * The optional initial state of the serialization.
/// 1. **options**
///    * Customize the appearance of the serialization. All otherwise magic values are contained
///      here.
///
/// *Returns* the [`State`] of the serialization on success. You can use it as initial state in the
/// next call if you are halting event serialization.
///
/// *Errors* if the underlying buffer fails (which is unlikely) or if the [`Event`] stream
/// cannot ever be produced by deserializing valid Markdown. Each failure mode corresponds to one
/// of [`Error`]'s variants.
pub fn cmark_resume_with_options<'a, I, E, F>(
    events: I,
    mut formatter: F,
    state: Option<State<'a>>,
    options: Options<'_>,
) -> Result<State<'a>, Error>
where
    I: Iterator<Item = E>,
    E: Borrow<Event<'a>>,
    F: fmt::Write,
{
    let mut state = state.unwrap_or_default();
    let mut events = events.peekable();
    while let Some(event) = events.next() {
        state.next_is_link_like = matches!(
            events.peek().map(Borrow::borrow),
            Some(
                Event::Start(Tag::Link { .. } | Tag::Image { .. } | Tag::FootnoteDefinition(..))
                    | Event::FootnoteReference(..)
            )
        );
        cmark_resume_one_event(event, &mut formatter, &mut state, &options)?;
    }
    Ok(state)
}

fn cmark_resume_one_event<'a, E, F>(
    event: E,
    formatter: &mut F,
    state: &mut State<'a>,
    options: &Options<'_>,
) -> Result<(), Error>
where
    E: Borrow<Event<'a>>,
    F: fmt::Write,
{
    use pulldown_cmark::{Event::*, Tag::*};

    if state.pending_definition_list_marker {
        if matches!(event.borrow(), Start(Paragraph)) {
            // Paragraph wrappers distinguish loose definitions from tight ones.
            write_padded_newline(formatter, state)?;
        }
        state.write_pending_definition_list_marker(formatter)?;
    }

    let last_was_text_without_trailing_newline = state.last_was_text_without_trailing_newline;
    state.last_was_text_without_trailing_newline = false;
    let last_was_paragraph_start = state.last_was_paragraph_start;
    state.last_was_paragraph_start = false;
    let last_event = std::mem::take(&mut state.last_event);
    state.last_event = if is_inline_content(event.borrow()) {
        LastEvent::InlineContent
    } else {
        LastEvent::Other
    };
    let child_kind = ChildKind::of(event.borrow());
    fit_newlines_to_tight_list(child_kind, formatter, state)?;
    let html_block_has_trailing_newline = state.html_block_has_trailing_newline;
    state.html_block_has_trailing_newline = match event.borrow() {
        Html(text) if !text.is_empty() => text.ends_with('\n'),
        Html(_) | End(TagEnd::HtmlBlock) => html_block_has_trailing_newline,
        // Other events can change the padding or write content on this line.
        _ => false,
    };
    match event.borrow() {
        Start(List(_)) => state.open_tags.push(OpenTag::List(ListSpacing::Unknown)),
        Start(Item) => state.open_tags.push(OpenTag::Item),
        Start(_) => state.open_tags.push(OpenTag::Other),
        End(_) => {
            state.open_tags.pop();
        }
        Code(_) | Text(_) | InlineHtml(_) | InlineMath(_) | DisplayMath(_) | FootnoteReference(_)
        | TaskListMarker(_) => {
            // For leaf inline events, write pending newlines here. For Start and
            // Rule, we write them in their own arms, possibly after changing the
            // number of newlines to be written.
            consume_newlines(formatter, state)?;
        }
        Rule | Html(_) | SoftBreak | HardBreak => {}
    }

    let needs_line_break = match last_event {
        // Consider this standard list:
        //
        // * item
        //
        //   # heading
        //
        // Here, `item` is wrapped in a paragraph, and when the paragraph ends,
        // we write a blank line before the heading.
        //
        // Now, consider this tight list:
        //
        // * item
        //   # heading
        //
        // Here, `item` isn't wrapped in a paragraph, so nothing starts the
        // heading on a new line. If we don't add a line break ourselves, we
        // would write:
        //
        // * item# heading
        //
        // which parses as a single item with the text `item# heading`.
        //
        // Definition lists have the same problem:
        //
        // term
        // : definition
        //   # heading
        //
        // If the last event was inline content and this starts a block, add a
        // line break. In Markdown, a block can only begin at the start of a
        // line, so this is always correct.
        LastEvent::InlineContent => match child_kind {
            Some(ChildKind::ListItem | ChildKind::Block) => true,
            Some(ChildKind::Inline) | None => false,
        },
        LastEvent::Other | LastEvent::ItemStart | LastEvent::ItemEnd(_) | LastEvent::NestedListEnd(_) => false,
    };
    if needs_line_break {
        // Ensure that exactly one newline is set. With for example 2 newlines
        // (a blank line in between), the tight list above would be written
        // as:
        //
        // * item
        //
        //   # heading
        //
        // i.e., a loose list, which isn't correct. Definition lists behave the
        // same way.
        state.set_minimum_newlines_before_start(1);
    }

    let res = match event.borrow() {
        Rule => {
            let rule = match last_event {
                // Consider this tight list:
                //
                // * item
                //   ***
                //
                // Producing `---` would be incorrect, because it would be read
                // as a setext heading underline:
                //
                // * item
                //   ---
                //
                // i.e., `item` would become a level 2 heading. Switch to `***`
                // instead, which is unambiguously not a setext heading.
                LastEvent::InlineContent => "***",
                // Continue using "---" elsewhere since it is the more commonly
                // understood horizontal rule marker.
                LastEvent::Other | LastEvent::ItemStart | LastEvent::ItemEnd(_) | LastEvent::NestedListEnd(_) => "---",
            };
            consume_newlines(formatter, state)?;
            state.set_minimum_newlines_before_start(options.newlines_after_rule);
            formatter.write_str(rule)
        }
        Code(text) => {
            if let Some(shortcut_text) = state.current_shortcut_text.as_mut() {
                shortcut_text.push('`');
                shortcut_text.push_str(text);
                shortcut_text.push('`');
            }
            if let Some(text_for_header) = state.text_for_header.as_mut() {
                text_for_header.push('`');
                text_for_header.push_str(text);
                text_for_header.push('`');
            }

            // (re)-escape `|` when it appears as part of inline code in the
            // body of a table.
            //
            // NOTE: This does not do *general* escaped-character handling
            // because the only character which *requires* this handling in this
            // spot in earlier versions of `pulldown-cmark` is a pipe character
            // in inline code in a table. Other escaping is handled when `Text`
            // events are emitted.
            let text = if state.in_table_cell {
                Cow::Owned(text.replace('|', "\\|"))
            } else {
                Cow::Borrowed(text.as_ref())
            };

            // When inline code has leading and trailing ' ' characters, additional space is needed
            // to escape it, unless all characters are space.
            if text.chars().all(|ch| ch == ' ') {
                write!(formatter, "`{text}`")
            } else {
                // More backticks are needed to delimit the inline code than the maximum number of
                // backticks in a consecutive run.
                let backticks = Repeated('`', max_consecutive_chars(&text, '`') + 1);
                let space = match text.as_bytes() {
                    &[b'`', ..] | &[.., b'`'] => " ", // Space needed to separate backtick.
                    &[b' ', .., b' '] => " ",         // Space needed to escape inner space.
                    _ => "",                          // No space needed.
                };
                write!(formatter, "{backticks}{space}{text}{space}{backticks}")
            }
        }
        Start(tag) => {
            if let List(list_type) = tag {
                state.list_stack.push(*list_type);
                if state.list_stack.len() > 1 {
                    state.set_minimum_newlines_before_start(options.newlines_after_rest);
                }
            }
            let needs_blank_line = match last_event {
                LastEvent::NestedListEnd(tail) => needs_blank_line_after_nested_list(tag, tail),
                // In all of these cases, a blank line is either unnecessary or
                // actively wrong.
                LastEvent::Other | LastEvent::InlineContent | LastEvent::ItemStart | LastEvent::ItemEnd(_) => false,
            };
            if needs_blank_line {
                state.set_minimum_newlines_before_start(options.newlines_after_list);
            }
            let consumed_newlines = state.newlines_before_start != 0 || html_block_has_trailing_newline;
            consume_newlines(formatter, state)?;
            match tag {
                Item => {
                    // lazy lists act like paragraphs with no event
                    state.last_was_paragraph_start = true;
                    state.last_event = LastEvent::ItemStart;
                    match state.list_stack.last_mut() {
                        Some(inner) => {
                            state.padding.push(list_item_padding_of(*inner));
                            match inner {
                                Some(n) => {
                                    let bullet_number = *n;
                                    if options.increment_ordered_list_bullets {
                                        *n += 1;
                                    }
                                    write!(formatter, "{}{} ", bullet_number, options.ordered_list_token)
                                }
                                None => write!(formatter, "{} ", options.list_token),
                            }
                        }
                        None => Ok(()),
                    }
                }
                Table(alignments) => {
                    state.table_alignments = alignments.iter().map(From::from).collect();
                    Ok(())
                }
                TableHead => Ok(()),
                TableRow => Ok(()),
                TableCell => {
                    state.text_for_header = Some(String::new());
                    state.in_table_cell = true;
                    formatter.write_char('|')
                }
                Link {
                    link_type,
                    dest_url,
                    title,
                    id,
                } => {
                    state.link_stack.push(match link_type {
                        LinkType::Autolink | LinkType::Email => {
                            formatter.write_char('<')?;
                            LinkCategory::AngleBracketed
                        }
                        LinkType::Reference => {
                            formatter.write_char('[')?;
                            LinkCategory::Reference {
                                uri: dest_url.clone().into(),
                                title: title.clone().into(),
                                id: id.clone().into(),
                            }
                        }
                        LinkType::Collapsed => {
                            state.current_shortcut_text = Some(String::new());
                            formatter.write_char('[')?;
                            LinkCategory::Collapsed {
                                uri: dest_url.clone().into(),
                                title: title.clone().into(),
                            }
                        }
                        LinkType::Shortcut => {
                            state.current_shortcut_text = Some(String::new());
                            formatter.write_char('[')?;
                            LinkCategory::Shortcut {
                                uri: dest_url.clone().into(),
                                title: title.clone().into(),
                            }
                        }
                        _ => {
                            formatter.write_char('[')?;
                            LinkCategory::Other {
                                uri: dest_url.clone().into(),
                                title: title.clone().into(),
                            }
                        }
                    });
                    Ok(())
                }
                Image {
                    link_type,
                    dest_url,
                    title,
                    id,
                } => {
                    state.image_stack.push(match link_type {
                        LinkType::Reference => ImageLink::Reference {
                            uri: dest_url.clone().into(),
                            title: title.clone().into(),
                            id: id.clone().into(),
                        },
                        LinkType::Collapsed => {
                            state.current_shortcut_text = Some(String::new());
                            ImageLink::Collapsed {
                                uri: dest_url.clone().into(),
                                title: title.clone().into(),
                            }
                        }
                        LinkType::Shortcut => {
                            state.current_shortcut_text = Some(String::new());
                            ImageLink::Shortcut {
                                uri: dest_url.clone().into(),
                                title: title.clone().into(),
                            }
                        }
                        _ => ImageLink::Other {
                            uri: dest_url.clone().into(),
                            title: title.clone().into(),
                        },
                    });
                    formatter.write_str("![")
                }
                Emphasis => formatter.write_char(options.emphasis_token),
                Strong => formatter.write_str(options.strong_token),
                FootnoteDefinition(name) => {
                    state.padding.push("    ".into());
                    write!(formatter, "[^{name}]: ")
                }
                Paragraph => {
                    state.last_was_paragraph_start = true;
                    Ok(())
                }
                Heading {
                    level,
                    id,
                    classes,
                    attrs,
                } => {
                    if state.current_heading.is_some() {
                        return Err(Error::UnexpectedEvent);
                    }
                    state.current_heading = Some(self::Heading {
                        id: id.as_ref().map(|id| id.clone().into()),
                        classes: classes.iter().map(|class| class.clone().into()).collect(),
                        attributes: attrs
                            .iter()
                            .map(|(k, v)| (k.clone().into(), v.as_ref().map(|val| val.clone().into())))
                            .collect(),
                    });
                    // Write '#', '##', '###', etc. based on the heading level.
                    write!(formatter, "{} ", Repeated('#', *level as usize))
                }
                BlockQuote(kind) => {
                    let every_line_padding = " > ";
                    let first_line_padding = kind
                        .map(|kind| match kind {
                            BlockQuoteKind::Note => " > [!NOTE]",
                            BlockQuoteKind::Tip => " > [!TIP]",
                            BlockQuoteKind::Important => " > [!IMPORTANT]",
                            BlockQuoteKind::Warning => " > [!WARNING]",
                            BlockQuoteKind::Caution => " > [!CAUTION]",
                        })
                        .unwrap_or(every_line_padding);
                    state.newlines_before_start = 1;

                    // if we consumed some newlines, we know that we can just write out the next
                    // level in our blockquote. This should work regardless if we have other
                    // padding or if we're in a list
                    if !consumed_newlines {
                        write_padded_newline(formatter, state)?;
                    }
                    formatter.write_str(first_line_padding)?;
                    state.padding.push(every_line_padding.into());
                    Ok(())
                }
                CodeBlock(pulldown_cmark::CodeBlockKind::Indented) => {
                    state.code_block = Some(CodeBlockKind::Indented);
                    state.padding.push("    ".into());
                    if consumed_newlines {
                        formatter.write_str("    ")
                    } else {
                        write_padded_newline(formatter, &state)
                    }
                }
                CodeBlock(pulldown_cmark::CodeBlockKind::Fenced(info)) => {
                    state.code_block = Some(CodeBlockKind::Fenced);
                    if !consumed_newlines {
                        write_padded_newline(formatter, &state)?;
                    }

                    let fence = Repeated(options.code_block_token, options.code_block_token_count);
                    write!(formatter, "{fence}{info}")?;
                    write_padded_newline(formatter, &state)
                }
                HtmlBlock => Ok(()),
                MetadataBlock(MetadataBlockKind::YamlStyle) => formatter.write_str("---\n"),
                MetadataBlock(MetadataBlockKind::PlusesStyle) => formatter.write_str("+++\n"),
                List(_) => Ok(()),
                Strikethrough => formatter.write_str("~~"),
                DefinitionList => Ok(()),
                DefinitionListTitle => {
                    state.set_minimum_newlines_before_start(options.newlines_after_rest);
                    Ok(())
                }
                DefinitionListDefinition => {
                    padding(formatter, &state.padding)?;
                    state.pending_definition_list_marker = true;
                    Ok(())
                }
                Superscript => formatter.write_str(if options.use_html_for_super_sub_script {
                    "<sup>"
                } else {
                    "^"
                }),
                Subscript => formatter.write_str(if options.use_html_for_super_sub_script {
                    "<sub>"
                } else {
                    "~"
                }),
            }
        }
        End(tag) => match tag {
            TagEnd::Link => match if let Some(link_cat) = state.link_stack.pop() {
                link_cat
            } else {
                return Err(Error::UnexpectedEvent);
            } {
                LinkCategory::AngleBracketed => formatter.write_char('>'),
                LinkCategory::Reference { uri, title, id } => {
                    state
                        .shortcuts
                        .push((id.to_string(), uri.to_string(), title.to_string()));
                    formatter.write_str("][")?;
                    formatter.write_str(&id)?;
                    formatter.write_char(']')
                }
                LinkCategory::Collapsed { uri, title } => {
                    if let Some(shortcut_text) = state.current_shortcut_text.take() {
                        state
                            .shortcuts
                            .push((EscapeLinkLabel(&shortcut_text).to_string(), uri.into(), title.into()));
                    }
                    formatter.write_str("][]")
                }
                LinkCategory::Shortcut { uri, title } => {
                    if let Some(shortcut_text) = state.current_shortcut_text.take() {
                        state
                            .shortcuts
                            .push((EscapeLinkLabel(&shortcut_text).to_string(), uri.into(), title.into()));
                    }
                    formatter.write_char(']')
                }
                LinkCategory::Other { uri, title } => close_link(&uri, &title, formatter, LinkType::Inline),
            },
            TagEnd::Image => match if let Some(img_link) = state.image_stack.pop() {
                img_link
            } else {
                return Err(Error::UnexpectedEvent);
            } {
                ImageLink::Reference { uri, title, id } => {
                    state
                        .shortcuts
                        .push((id.to_string(), uri.to_string(), title.to_string()));
                    formatter.write_str("][")?;
                    formatter.write_str(&id)?;
                    formatter.write_char(']')
                }
                ImageLink::Collapsed { uri, title } => {
                    if let Some(shortcut_text) = state.current_shortcut_text.take() {
                        state
                            .shortcuts
                            .push((EscapeLinkLabel(&shortcut_text).to_string(), uri.into(), title.into()));
                    }
                    formatter.write_str("][]")
                }
                ImageLink::Shortcut { uri, title } => {
                    if let Some(shortcut_text) = state.current_shortcut_text.take() {
                        state
                            .shortcuts
                            .push((EscapeLinkLabel(&shortcut_text).to_string(), uri.into(), title.into()));
                    }
                    formatter.write_char(']')
                }
                ImageLink::Other { uri, title } => {
                    close_link(uri.as_ref(), title.as_ref(), formatter, LinkType::Inline)
                }
            },
            TagEnd::Emphasis => formatter.write_char(options.emphasis_token),
            TagEnd::Strong => formatter.write_str(options.strong_token),
            TagEnd::Heading(_) => {
                let Some(self::Heading {
                    id,
                    classes,
                    attributes,
                }) = state.current_heading.take()
                else {
                    return Err(Error::UnexpectedEvent);
                };
                let emit_braces = id.is_some() || !classes.is_empty() || !attributes.is_empty();
                if emit_braces {
                    formatter.write_str(" {")?;
                }
                if let Some(id_str) = id {
                    formatter.write_char(' ')?;
                    formatter.write_char('#')?;
                    formatter.write_str(&id_str)?;
                }
                for class in &classes {
                    formatter.write_char(' ')?;
                    formatter.write_char('.')?;
                    formatter.write_str(class)?;
                }
                for (key, val) in &attributes {
                    formatter.write_char(' ')?;
                    formatter.write_str(key)?;
                    if let Some(val) = val {
                        formatter.write_char('=')?;
                        formatter.write_str(val)?;
                    }
                }
                if emit_braces {
                    formatter.write_char(' ')?;
                    formatter.write_char('}')?;
                }
                state.set_minimum_newlines_before_start(options.newlines_after_headline);
                Ok(())
            }
            TagEnd::Paragraph => {
                state.set_minimum_newlines_before_start(options.newlines_after_paragraph);
                Ok(())
            }
            TagEnd::CodeBlock => {
                state.set_minimum_newlines_before_start(options.newlines_after_codeblock);
                if last_was_text_without_trailing_newline {
                    write_padded_newline(formatter, &state)?;
                }
                match state.code_block {
                    Some(CodeBlockKind::Fenced) => {
                        let fence = Repeated(options.code_block_token, options.code_block_token_count);
                        write!(formatter, "{fence}")?;
                    }
                    Some(CodeBlockKind::Indented) => {
                        state.padding.pop();
                    }
                    None => {}
                }
                state.code_block = None;
                Ok(())
            }
            TagEnd::HtmlBlock => {
                state.set_minimum_newlines_before_start(options.newlines_after_htmlblock);
                Ok(())
            }
            TagEnd::MetadataBlock(MetadataBlockKind::PlusesStyle) => {
                state.set_minimum_newlines_before_start(options.newlines_after_metadata);
                formatter.write_str("+++\n")
            }
            TagEnd::MetadataBlock(MetadataBlockKind::YamlStyle) => {
                state.set_minimum_newlines_before_start(options.newlines_after_metadata);
                formatter.write_str("---\n")
            }
            TagEnd::Table => {
                state.set_minimum_newlines_before_start(options.newlines_after_table);
                state.table_alignments.clear();
                state.table_headers.clear();
                Ok(())
            }
            TagEnd::TableCell => {
                state
                    .table_headers
                    .push(state.text_for_header.take().unwrap_or_default());
                state.in_table_cell = false;
                Ok(())
            }
            t @ (TagEnd::TableRow | TagEnd::TableHead) => {
                state.set_minimum_newlines_before_start(options.newlines_after_rest);
                formatter.write_char('|')?;

                if let TagEnd::TableHead = t {
                    write_padded_newline(formatter, &state)?;
                    for (alignment, name) in state.table_alignments.iter().zip(state.table_headers.iter()) {
                        formatter.write_char('|')?;
                        // NOTE: For perfect counting, count grapheme clusters.
                        // The reason this is not done is to avoid the dependency.

                        // The minimum width of the column so that we can represent its alignment.
                        let min_width = match alignment {
                            // Must at least represent `-`.
                            Alignment::None => 1,
                            // Must at least represent `:-` or `-:`
                            Alignment::Left | Alignment::Right => 2,
                            // Must at least represent `:-:`
                            Alignment::Center => 3,
                        };
                        let length = name.chars().count().max(min_width);
                        let last_minus_one = length.saturating_sub(1);
                        for c in 0..length {
                            formatter.write_char(
                                if (c == 0 && (alignment == &Alignment::Center || alignment == &Alignment::Left))
                                    || (c == last_minus_one
                                        && (alignment == &Alignment::Center || alignment == &Alignment::Right))
                                {
                                    ':'
                                } else {
                                    '-'
                                },
                            )?;
                        }
                    }
                    formatter.write_char('|')?;
                }
                Ok(())
            }
            TagEnd::Item => {
                state.padding.pop();
                state.set_minimum_newlines_before_start(options.newlines_after_rest);
                // The pending block quote end line is only written inside a
                // list item, so drop it once the outermost item ends.
                if !state.open_tags.contains(&OpenTag::Item) {
                    state.pending_block_quote_end_line = None;
                }
                let tail = match last_event {
                    LastEvent::InlineContent => ItemTail::OpenParagraph,
                    LastEvent::ItemStart => ItemTail::Empty,
                    // Inherit the tail from the nested list's last item.
                    //
                    // For example, consider:
                    //
                    // 1. one
                    //    * two
                    //      - three
                    //
                    //    para
                    //
                    // The events end with:
                    //
                    // Text("three")  End(Item)  End(List)  End(Item)  End(List)  Start(Paragraph) …
                    //                ^ three    ^ - list   ^ two      ^ * list
                    //
                    // We must insert a blank line here -- without it, `para`
                    // would be a lazy continuation of `three`, which is two
                    // levels down. So we continue to keep track of the tail for
                    // that purpose instead of setting it to
                    // `ItemTail::ClosedBlock`.
                    LastEvent::NestedListEnd(tail) => tail,
                    LastEvent::Other | LastEvent::ItemEnd(_) => ItemTail::ClosedBlock,
                };
                state.last_event = LastEvent::ItemEnd(tail);
                Ok(())
            }
            TagEnd::List(_) => {
                state.list_stack.pop();
                if state.list_stack.is_empty() {
                    state.set_minimum_newlines_before_start(options.newlines_after_list);
                } else {
                    // Putting a blank line directly here would turn a tight
                    // parent list into a loose one. Instead, record how the
                    // nested list ended, which allows the next block to insert
                    // a blank line if needed.
                    let tail = match last_event {
                        LastEvent::ItemEnd(tail) => tail,
                        // Parsers always emit End(Item) before End(List), so
                        // these cases should never be hit in normal use. But a
                        // hand-written event stream could be malformed -- fall
                        // back to ItemTail::ClosedBlock.
                        LastEvent::Other
                        | LastEvent::InlineContent
                        | LastEvent::ItemStart
                        | LastEvent::NestedListEnd(_) => ItemTail::ClosedBlock,
                    };
                    state.last_event = LastEvent::NestedListEnd(tail);
                }
                Ok(())
            }
            TagEnd::BlockQuote(_) => {
                state.padding.pop();

                state.set_minimum_newlines_before_start(options.newlines_after_blockquote);
                state.pending_block_quote_end_line = if state.open_tags.contains(&OpenTag::Item) {
                    let mut pending = state.padding.concat();
                    pending.push_str(" >");
                    Some(pending)
                } else {
                    // The pending block quote end line is only ever written
                    // inside a tight list item -- there's no need to track it
                    // if the stack of open tags doesn't contain any Item
                    // instances in it.
                    None
                };

                Ok(())
            }
            TagEnd::FootnoteDefinition => {
                state.padding.pop();
                Ok(())
            }
            TagEnd::Strikethrough => formatter.write_str("~~"),
            TagEnd::DefinitionList => {
                state.set_minimum_newlines_before_start(options.newlines_after_list);
                Ok(())
            }
            TagEnd::DefinitionListTitle => formatter.write_char('\n'),
            TagEnd::DefinitionListDefinition => {
                state.padding.pop();
                // This newline leaves the quote; a later `>` would start a new one.
                state.pending_block_quote_end_line = None;
                write_padded_newline(formatter, &state)
            }
            TagEnd::Superscript => formatter.write_str(if options.use_html_for_super_sub_script {
                "</sup>"
            } else {
                "^"
            }),
            TagEnd::Subscript => formatter.write_str(if options.use_html_for_super_sub_script {
                "</sub>"
            } else {
                "~"
            }),
        },
        HardBreak => formatter.write_str("  ").and(write_padded_newline(formatter, &state)),
        SoftBreak => write_padded_newline(formatter, &state),
        Text(text) => {
            let mut text = &text[..];
            if let Some(shortcut_text) = state.current_shortcut_text.as_mut() {
                shortcut_text.push_str(text);
            }
            if let Some(text_for_header) = state.text_for_header.as_mut() {
                text_for_header.push_str(text);
            }
            if last_was_paragraph_start {
                if text.starts_with('\t') {
                    formatter.write_str("&#9;")?;
                    text = &text[1..];
                } else if text.starts_with(' ') {
                    formatter.write_str("&#32;")?;
                    text = &text[1..];
                }
            }
            state.last_was_text_without_trailing_newline = !text.ends_with('\n');
            let escaped_text = escape_special_characters(text, state, options);
            print_text_without_trailing_newline(&escaped_text, formatter, &state)
        }
        InlineHtml(text) => print_text_without_trailing_newline(text, formatter, &state),
        Html(text) => {
            let mut lines = text.split('\n');
            if let Some(line) = lines.next() {
                formatter.write_str(line)?;
            }
            for line in lines {
                write_padded_newline(formatter, &state)?;
                formatter.write_str(line)?;
            }
            Ok(())
        }
        FootnoteReference(name) => write!(formatter, "[^{name}]"),
        TaskListMarker(checked) => {
            let check = if *checked { "x" } else { " " };
            write!(formatter, "[{check}] ")
        }
        InlineMath(text) => write!(formatter, "${text}$"),
        DisplayMath(text) => write!(formatter, "$${text}$$"),
    };

    Ok(res?)
}

/// What an event adds to the innermost open tag.
///
/// For example, in this list:
///
/// ```md
/// * item
///   # heading
/// * c
/// ```
///
/// `Text("item")` adds `Inline` content to the first item, `Start(Heading)`
/// adds a `Block` to it, and `Start(Item)` for `c` adds a `ListItem` to the
/// list. `End` events close a tag rather than adding to one, so
/// `ChildKind::of` returns `None` for them.
///
/// This is used by `fit_newlines_to_tight_list` to decide whether a blank line
/// may come before the event.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ChildKind {
    ListItem,
    Block,
    Inline,
}

impl ChildKind {
    fn of(event: &Event<'_>) -> Option<Self> {
        match event {
            Event::Start(Tag::Item) => Some(ChildKind::ListItem),
            Event::Start(tag) => {
                if is_block_tag(tag) {
                    Some(ChildKind::Block)
                } else {
                    Some(ChildKind::Inline)
                }
            }
            Event::Rule => Some(ChildKind::Block),
            Event::Text(_)
            | Event::Code(_)
            | Event::InlineHtml(_)
            | Event::InlineMath(_)
            | Event::DisplayMath(_)
            | Event::FootnoteReference(_)
            | Event::SoftBreak
            | Event::HardBreak
            | Event::TaskListMarker(_) => Some(ChildKind::Inline),
            // Html only appears inside an HtmlBlock, whose Start was already
            // the Block.
            Event::End(_) | Event::Html(_) => None,
        }
    }
}

// Consider this tight list:
//
// * a
// * item
//   # heading
// * c
//
// When the heading ends, we ask for a blank line before whatever comes next,
// so we would write:
//
// * a
// * item
//   # heading
//
// * c
//
// i.e., a loose list, which isn't correct. A blank line between two children
// of the same item does the same thing:
//
// * item
//   # heading
//
//   text
//
// So before each event, if we're in a tight list and the event starts the
// next item or the next child of an item, cut the pending newlines down to
// one.
fn fit_newlines_to_tight_list<F>(kind: Option<ChildKind>, formatter: &mut F, state: &mut State<'_>) -> fmt::Result
where
    F: fmt::Write,
{
    // End(Item) and End(List) can come between a block quote's end and the
    // content after it, so these End events leave the pending line in place.
    let Some(kind) = kind else {
        return Ok(());
    };
    let pending_block_quote_end_line = state.pending_block_quote_end_line.take();
    let in_tight_list = match state.open_tags.as_mut_slice() {
        [parents @ .., OpenTag::Item] => {
            // The item's list is the tag just below it on the stack.
            let list_spacing = match parents.last_mut() {
                Some(OpenTag::List(spacing)) => Some(spacing),
                Some(OpenTag::Item | OpenTag::Other) | None => None,
            };
            match kind {
                ChildKind::Inline => {
                    // In a loose list, item text is wrapped in a paragraph:
                    //
                    // * item
                    //
                    //   # heading
                    //
                    // produces `Start(Item), Start(Paragraph), Text("item")`.
                    // So inline content directly inside an item means the list
                    // is tight. Record that for the rest of the list.
                    if let Some(spacing) = list_spacing {
                        *spacing = ListSpacing::Tight;
                    }
                    true
                }
                ChildKind::Block => match list_spacing {
                    Some(ListSpacing::Tight) => true,
                    // We don't know yet whether the list is tight. Consider
                    // these two lists:
                    //
                    // * # heading
                    //
                    //   # heading 2
                    // * c
                    //
                    // * # heading
                    //   # heading 2
                    // * c
                    //
                    // The first is loose, so `c` is wrapped in a paragraph. The
                    // second is tight, so it isn't. Their events are otherwise
                    // the same, and `c` comes after the gap before `heading 2`,
                    // so when we write that gap we can't tell the two apart.
                    //
                    // Keep the blank line, which is correct for the first list.
                    // This does mean that the second list comes out loose, with
                    // a paragraph around `c`; fixing that would require some
                    // kind of lookahead, either by us or within pulldown-cmark.
                    Some(ListSpacing::Unknown) | None => false,
                },
                // An item can't start directly inside another item (a nested
                // item is inside a nested list).
                ChildKind::ListItem => false,
            }
        }
        [.., OpenTag::List(spacing)] => match kind {
            ChildKind::ListItem => match spacing {
                ListSpacing::Tight => true,
                ListSpacing::Unknown => false,
            },
            ChildKind::Block | ChildKind::Inline => false,
        },
        [.., OpenTag::Other] | [] => false,
    };
    if !in_tight_list {
        return Ok(());
    }

    match (kind, pending_block_quote_end_line) {
        (ChildKind::Block | ChildKind::Inline, Some(pending)) => {
            // Consider this tight list:
            //
            // * a
            //   > q
            //   >
            //   text
            //
            // After the block quote ends, `text` needs its own line. But with
            // just a newline, we would write:
            //
            // * a
            //   > q
            //   text
            //
            // i.e., `text` would be a lazy continuation of the `q` paragraph,
            // and end up inside the block quote. A blank line would make the
            // list loose. Instead, write an empty `>` line. It ends the `q`
            // paragraph, and since it's inside the block quote, it doesn't
            // make the list loose.
            formatter.write_char('\n')?;
            formatter.write_str(&pending)?;
            state.newlines_before_start = 1;
        }
        // A new list item can't cause a block quote to be continued, so we
        // don't need to output the pending block quote end line in that case.
        (ChildKind::ListItem, _) | (ChildKind::Block | ChildKind::Inline, None) => {
            let newlines = usize::from(!state.html_block_has_trailing_newline);
            state.newlines_before_start = state.newlines_before_start.min(newlines);
        }
    }
    Ok(())
}

fn is_inline_content(event: &Event<'_>) -> bool {
    match event {
        Event::Text(_)
        | Event::Code(_)
        | Event::InlineHtml(_)
        | Event::InlineMath(_)
        | Event::DisplayMath(_)
        | Event::FootnoteReference(_)
        | Event::SoftBreak
        | Event::HardBreak
        | Event::TaskListMarker(_) => true,
        Event::End(tag) => !is_block_tag_end(*tag),
        Event::Start(_) | Event::Html(_) | Event::Rule => false,
    }
}

fn is_block_tag(tag: &Tag<'_>) -> bool {
    is_block_tag_end(tag.to_end())
}

fn is_block_tag_end(tag: TagEnd) -> bool {
    match tag {
        TagEnd::Paragraph
        | TagEnd::Heading(_)
        | TagEnd::BlockQuote(_)
        | TagEnd::CodeBlock
        | TagEnd::HtmlBlock
        | TagEnd::List(_)
        | TagEnd::Item
        | TagEnd::FootnoteDefinition
        | TagEnd::DefinitionList
        | TagEnd::DefinitionListTitle
        | TagEnd::DefinitionListDefinition
        | TagEnd::Table
        | TagEnd::TableHead
        | TagEnd::TableRow
        | TagEnd::TableCell
        | TagEnd::MetadataBlock(_) => true,
        TagEnd::Emphasis
        | TagEnd::Strong
        | TagEnd::Strikethrough
        | TagEnd::Superscript
        | TagEnd::Subscript
        | TagEnd::Link
        | TagEnd::Image => false,
    }
}

fn needs_blank_line_after_nested_list(tag: &Tag<'_>, tail: ItemTail) -> bool {
    // Consider a standard list:
    //
    // 1. item
    //
    //    * a
    //    * b
    //
    //    para
    //
    // After the nested list ends, we must insert a blank line before `para`. If
    // we don't do that, the CommonMark rules say that `para` becomes a
    // continuation of the last item (`b`).
    //
    // Now, consider a tight list:
    //
    // 1. item
    //    * a
    //    * b
    //    ```
    //    code
    //    ```
    //
    // In this case, we must _not_ insert a blank line before the fenced `code`
    // block. If we do that, then the list will become loose.
    //
    // The rule we follow is: add a blank line if leaving it out would change
    // how the Markdown parses. That happens either when the next line would be
    // absorbed into the last item, or when a loose list would become tight.
    // (HTML blocks are a known exception; see below.)
    match tag {
        // Paragraph events only appear in loose containers, so a blank line is
        // either required or harmless.
        Tag::Paragraph => true,
        // Tables can't interrupt a paragraph, so only an open paragraph (lazy
        // continuation) can absorb it.
        Tag::Table(_) => match tail {
            // 1. item
            //    * a
            //    * b
            //
            //    | x | y |
            //    | - | - |
            //
            // If we don't insert a blank line, the table will be glued onto the
            // `b` item, producing garbled text.
            ItemTail::OpenParagraph => true,
            // The Empty case:
            //
            // * a
            //   * b
            //   *
            //   | x | y |
            //   | - | - |
            //
            // If we insert a blank line, then the list will become loose. The
            // ClosedBlock case is similar.
            ItemTail::Empty | ItemTail::ClosedBlock => false,
        },
        // Like tables, indented code blocks can't interrupt a paragraph, so an
        // open paragraph can absorb them. But unlike tables, they're written
        // four columns past the parent's content, which is far enough to reach
        // the content column of an empty item.
        Tag::CodeBlock(pulldown_cmark::CodeBlockKind::Indented) => match tail {
            // The OpenParagraph case:
            //
            // 1. item
            //
            //    10000. a
            //
            //        code
            //
            // If we don't insert a blank line, `code` will be glued onto the `a`
            // item. (The wide `10000.` marker is what keeps `code` out of the
            // `a` item in the first place.)
            //
            // The Empty case:
            //
            // 1. item
            //    * a
            //    *
            //
            //        code
            //
            // If we don't insert a blank line, the empty item will take `code`
            // as its content.
            ItemTail::OpenParagraph | ItemTail::Empty => true,
            // As with tables, if we insert a blank line, then the list will
            // become loose.
            ItemTail::ClosedBlock => false,
        },
        // Like tables, definition lists can't interrupt a paragraph: the title
        // is plain paragraph text, so an open paragraph would absorb it. For
        // example:
        //
        // 1. item
        //    * a
        //    * b
        //
        //    term
        //    : definition
        //
        // If there's no blank line before `term`, it will be glued onto the `b`
        // item. But the `DefinitionListTitle` start always emits an extra
        // newline, which already produces that blank line. If we returned true
        // here, we'd get two blank lines.
        //
        // The `nested_list_followed_by_definition_list` test checks this.
        Tag::DefinitionList => false,
        // There are 7 types of HTML blocks defined in CommonMark: see
        // https://spec.commonmark.org/0.31.2/#html-blocks. Types 1-6 can
        // interrupt a paragraph, but type 7 cannot. But pulldown-cmark doesn't
        // tell us which type an HTML block is.
        //
        // We always return false here (assuming types 1-6) in lieu of figuring
        // out the HTML block's type within this library.
        Tag::HtmlBlock => false,
        // These tags can all interrupt a paragraph, so they're never absorbed
        // into the nested list's last item, even without a blank line before
        // them. For example:
        //
        // 1. item
        //    * a
        //    * b
        //    # heading
        //
        // Here, `# heading` is a heading of its own, not part of `b`. If we
        // insert a blank line, then the list will become loose.
        Tag::Heading { .. }
        | Tag::BlockQuote(_)
        | Tag::CodeBlock(pulldown_cmark::CodeBlockKind::Fenced(_))
        | Tag::FootnoteDefinition(_) => false,
        // There are two kinds of lists:
        //
        // 1. Those that can interrupt a paragraph, such as a bulleted list,
        //    or an ordered list starting at `1.` as long as the first item
        //    isn't empty. In these cases, similar to the heading example above,
        //    inserting a blank line will make the parent list loose. So
        //    returning `false` here is correct.
        //
        // 2. Those that cannot interrupt a paragraph, such as one starting at
        //    `2.`, or one whose first item is empty (see
        //    https://spec.commonmark.org/0.31.2/#list-items). But
        //    pulldown-cmark only applies that rule when the list marker is
        //    indented enough to be inside the item holding the paragraph.
        //
        //    In the following example, `2. c` is indented to `b`'s content, so
        //    it can't interrupt `b`'s paragraph and is glued onto it as text:
        //
        //    1. item
        //       * a
        //       * b
        //         2. c
        //
        //    `2. c` is treated as plain text, so this code path isn't hit.
        //
        //    But in the following example, `2. c` is at the parent's
        //    indentation, outside `b`, so the rule doesn't apply and it starts
        //    a new ordered list inside `item`, next to the nested one:
        //
        //    1. item
        //       * a
        //       * b
        //       2. c
        //
        //    Only this second example reaches this code path. We write the new
        //    list back at the parent's indentation, so it starts a new list
        //    again. As with the first kind, inserting a blank line will make
        //    the parent list loose, and returning `false` is correct.
        Tag::List(_) => false,
        // List items, table parts, and definition list parts only appear inside
        // lists, tables, and definition lists respectively. None of those can
        // directly contain a list, so these tags can't come right after a
        // nested list ends.
        Tag::Item
        | Tag::TableHead
        | Tag::TableRow
        | Tag::TableCell
        | Tag::DefinitionListTitle
        | Tag::DefinitionListDefinition => false,
        // Metadata blocks can only appear at the start of a document.
        Tag::MetadataBlock(_) => false,
        // Inline content can come right after a nested list ends, but only
        // directly inside a tight parent item, and only when the nested list's
        // last item left no paragraph open. For example:
        //
        // - a
        //   - b
        //     <!-- c -->
        //   *d* e
        //
        // Here, `*d* e` isn't wrapped in a paragraph because the parent list
        // is tight. Inserting a blank line would make it loose.
        Tag::Emphasis
        | Tag::Strong
        | Tag::Strikethrough
        | Tag::Superscript
        | Tag::Subscript
        | Tag::Link { .. }
        | Tag::Image { .. } => false,
    }
}

impl State<'_> {
    /// Finalize the serialization state by writing any remaining shortcuts.
    ///
    /// This should be called after all events have been processed to ensure
    /// reference-style links are written at the end of the document.
    pub fn finalize<F>(mut self, mut formatter: F) -> Result<Self, Error>
    where
        F: fmt::Write,
    {
        self.write_pending_definition_list_marker(&mut formatter)?;
        if self.shortcuts.is_empty() {
            return Ok(self);
        }

        formatter.write_str("\n")?;
        let mut written_shortcuts = HashSet::new();
        for shortcut in self.shortcuts.drain(..) {
            if written_shortcuts.contains(&shortcut) {
                continue;
            }
            write!(formatter, "\n[{}", shortcut.0)?;
            close_link(&shortcut.1, &shortcut.2, &mut formatter, LinkType::Shortcut)?;
            written_shortcuts.insert(shortcut);
        }
        Ok(self)
    }

    /// Returns `true` if currently serializing content inside a code block.
    pub fn is_in_code_block(&self) -> bool {
        self.code_block.is_some()
    }

    fn write_pending_definition_list_marker(&mut self, formatter: &mut impl fmt::Write) -> fmt::Result {
        if std::mem::take(&mut self.pending_definition_list_marker) {
            formatter.write_str(": ")?;
            self.padding.push("  ".into());
        }
        Ok(())
    }

    /// Ensure that [`State::newlines_before_start`] is at least as large as
    /// the provided option value.
    fn set_minimum_newlines_before_start(&mut self, option_value: usize) {
        if self.newlines_before_start < option_value {
            self.newlines_before_start = option_value
        }
    }
}

/// Return the `<seen amount of consecutive fenced code-block tokens> + 1` that occur *within* a
/// fenced code-block `events`.
///
/// Use this function to obtain the correct value for `code_block_token_count` field of [`Options`]
/// to assure that the enclosing code-blocks remain functional as such.
///
/// Returns `None` if `events` didn't include any code-block, or the code-block didn't contain
/// a nested block. In that case, the correct amount of fenced code-block tokens is
/// [`DEFAULT_CODE_BLOCK_TOKEN_COUNT`].
///
/// ```rust
/// use pulldown_cmark::Event;
/// use pulldown_cmark_to_cmark::*;
///
/// let events = &[Event::Text("text".into())];
/// let code_block_token_count = calculate_code_block_token_count(events).unwrap_or(DEFAULT_CODE_BLOCK_TOKEN_COUNT);
/// let options = Options {
///     code_block_token_count,
///     ..Default::default()
/// };
/// let mut buf = String::new();
/// cmark_with_options(events.iter(), &mut buf, options);
/// ```
pub fn calculate_code_block_token_count<'a, I, E>(events: I) -> Option<usize>
where
    I: IntoIterator<Item = E>,
    E: Borrow<Event<'a>>,
{
    let mut in_codeblock = false;
    let mut max_token_count = 0;

    // token_count should be taken over Text events
    // because a continuous text may be splitted to some Text events.
    let mut token_count = 0;
    let mut prev_token_char = None;
    for event in events {
        match event.borrow() {
            Event::Start(Tag::CodeBlock(_)) => {
                in_codeblock = true;
            }
            Event::End(TagEnd::CodeBlock) => {
                in_codeblock = false;
                prev_token_char = None;
            }
            Event::Text(x) if in_codeblock => {
                for c in x.chars() {
                    let prev_token = prev_token_char.take();
                    if c == '`' || c == '~' {
                        prev_token_char = Some(c);
                        if Some(c) == prev_token {
                            token_count += 1;
                        } else {
                            max_token_count = max_token_count.max(token_count);
                            token_count = 1;
                        }
                    }
                }
            }
            _ => prev_token_char = None,
        }
    }

    max_token_count = max_token_count.max(token_count);
    (max_token_count >= 3).then_some(max_token_count + 1)
}
