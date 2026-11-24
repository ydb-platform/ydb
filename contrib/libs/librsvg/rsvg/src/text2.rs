// ! development file for text2
use cssparser::Parser;
use markup5ever::{expanded_name, local_name, ns};
use pango::IsAttribute;
use rctree::NodeEdge;

use crate::element::{Element, ElementData, ElementTrait, set_attribute};
use crate::error::ParseError;
use crate::layout::FontProperties;
use crate::length::{Horizontal, Length, NormalizeParams, Vertical};
use crate::node::{Node, NodeData};
use crate::parsers::{CommaSeparatedList, Parse, ParseValue};
use crate::properties::WhiteSpace;
use crate::session::Session;
use crate::text::BidiControl;
use crate::xml;
use crate::{parse_identifiers, rsvg_log};

/// Type for the `x/y/dx/dy` attributes of the `<text>` and `<tspan>` elements
///
/// <https://svgwg.org/svg2-draft/text.html#TSpanAttributes>
///
/// Explanation of this type:
///
/// * Option - the attribute can be specified or not, so make it optional
///
///  `CommaSeparatedList<Length<Horizontal>>` - This type knows how to parse a list of values
///  that are separated by commas and/or spaces; the values are eventually available as a Vec.
///
/// * 1 is the minimum number of elements in the list, so one can have x="42" for example.
///
/// * 4096 is an arbitrary limit on the number of length values for each array, as a mitigation
///   against malicious files which may want to have millions of elements to exhaust memory.
type OptionalLengthList<N> = Option<CommaSeparatedList<Length<N>, 1, 4096>>;

/// Type for the `rotate` attribute of the `<text>` and `<tspan>` elements
///
/// <https://svgwg.org/svg2-draft/text.html#TSpanAttributes>
///
/// See [`OptionalLengthList`] for a description of the structure of the type.
type OptionalRotateList = Option<CommaSeparatedList<f64, 1, 4096>>;

/// Enum for the `lengthAdjust` attribute
///
/// <https://svgwg.org/svg2-draft/text.html#LengthAdjustProperty>
#[derive(Debug, Default, Copy, Clone, PartialEq)]
enum LengthAdjust {
    #[default]
    Spacing,
    SpacingAndGlyphs,
}

impl Parse for LengthAdjust {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "spacing" => LengthAdjust::Spacing,
            "spacingAndGlyphs" => LengthAdjust::SpacingAndGlyphs,
        )?)
    }
}

#[allow(dead_code)]
#[derive(Default)]
pub struct Text2 {
    x: OptionalLengthList<Horizontal>,
    y: OptionalLengthList<Vertical>,
    dx: OptionalLengthList<Horizontal>,
    dy: OptionalLengthList<Vertical>,
    rotate: OptionalRotateList,
    text_length: Length<Horizontal>,
    length_adjust: LengthAdjust, // Implemented
}

// HOMEWORK
//
// see text.rs and how it implements set_attributes() for the Text element.
// The attributes are described here:
//
// <https://svgwg.org/svg2-draft/text.html#TSpanAttributes>
//
// Attributes to parse:
//   "x"
//   "y"
//   "dx"
//   "dy"
//   "rotate"
//   "textLength"
//   "lengthAdjust"
impl ElementTrait for Text2 {
    fn set_attributes(&mut self, attrs: &xml::Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "x") => set_attribute(&mut self.x, attr.parse(value), session),
                expanded_name!("", "y") => set_attribute(&mut self.y, attr.parse(value), session),
                expanded_name!("", "dx") => set_attribute(&mut self.dx, attr.parse(value), session),
                expanded_name!("", "dy") => set_attribute(&mut self.dy, attr.parse(value), session),
                expanded_name!("", "rotate") => {
                    set_attribute(&mut self.rotate, attr.parse(value), session)
                }
                expanded_name!("", "textLength") => {
                    set_attribute(&mut self.text_length, attr.parse(value), session)
                }
                expanded_name!("", "lengthAdjust") => {
                    set_attribute(&mut self.length_adjust, attr.parse(value), session)
                }
                _ => (),
            }
        }
    }
}

#[derive(Default)]
#[allow(dead_code)]
struct Character {
    // https://www.w3.org/TR/SVG2/text.html#TextLayoutAlgorithm
    // Section "11.5.1 Setup"
    //
    // global_index: u32,
    // x: f64,
    // y: f64,
    // angle: Angle,
    // hidden: bool,
    addressable: bool,
    character: char,
    // must_include: bool,
    // middle: bool,
    // anchored_chunk: bool,
}

//              <tspan>   hello</tspan>
// addressable:        tffttttt

//              <tspan direction="ltr">A <tspan direction="rtl"> B </tspan> C</tspan>
//              A xx B xx C          "xx" are bidi control characters
// addressable: ttfffttffft

// HOMEWORK
#[allow(unused)]
fn collapse_white_space(input: &str, white_space: WhiteSpace) -> Vec<Character> {
    match white_space {
        WhiteSpace::Normal | WhiteSpace::NoWrap => compute_normal_nowrap(input),
        WhiteSpace::Pre | WhiteSpace::PreWrap => compute_pre_prewrap(input),
        _ => unimplemented!(),
    }
}

fn is_bidi_control(ch: char) -> bool {
    use crate::text::directional_formatting_characters::*;
    matches!(ch, LRE | RLE | LRO | RLO | PDF | LRI | RLI | FSI | PDI)
}

// move to inline constant if conditions needs to change
fn is_space(ch: char) -> bool {
    matches!(ch, ' ' | '\t' | '\n')
}

// Summary of white-space rules from https://www.w3.org/TR/css-text-3/#white-space-property
//
//              New Lines   Spaces and Tabs   Text Wrapping   End-of-line   End-of-line
//                                                            spaces        other space separators
// -----------------------------------------------------------------------------------------------
// normal       Collapse    Collapse          Wrap            Remove        Hang
// pre          Preserve    Preserve          No wrap         Preserve      No wrap
// nowrap       Collapse    Collapse          No wrap         Remove        Hang
// pre-wrap     Preserve    Preserve          Wrap            Hang          Hang
// break-spaces Preserve    Preserve          Wrap            Wrap          Wrap
// pre-line     Preserve    Collapse          Wrap            Remove        Hang

fn compute_normal_nowrap(input: &str) -> Vec<Character> {
    let mut result: Vec<Character> = Vec::with_capacity(input.len());

    let mut prev_was_space: bool = false;

    for ch in input.chars() {
        if is_bidi_control(ch) {
            result.push(Character {
                addressable: false,
                character: ch,
            });
            continue;
        }

        if is_space(ch) {
            if prev_was_space {
                result.push(Character {
                    addressable: false,
                    character: ch,
                });
            } else {
                result.push(Character {
                    addressable: true,
                    character: ch,
                });
                prev_was_space = true;
            }
        } else {
            result.push(Character {
                addressable: true,
                character: ch,
            });

            prev_was_space = false;
        }
    }

    result
}

fn compute_pre_prewrap(input: &str) -> Vec<Character> {
    let mut result: Vec<Character> = Vec::with_capacity(input.len());

    for ch in input.chars() {
        if is_bidi_control(ch) {
            result.push(Character {
                addressable: false,
                character: ch,
            });
        } else {
            result.push(Character {
                addressable: true,
                character: ch,
            });
        }
    }

    result
}

fn get_bidi_control(element: &Element) -> BidiControl {
    // Extract bidi control logic to separate function to avoid duplication
    let computed_values = element.get_computed_values();

    let unicode_bidi = computed_values.unicode_bidi();
    let direction = computed_values.direction();

    BidiControl::from_unicode_bidi_and_direction(unicode_bidi, direction)
}

// FIXME: Remove the following line when this code actually starts getting used outside of tests.
#[allow(unused)]
fn collect_text_from_node(node: &Node) -> String {
    let mut result = String::new();

    for edge in node.traverse() {
        match edge {
            NodeEdge::Start(child_node) => match *child_node.borrow() {
                NodeData::Text(ref text) => {
                    result.push_str(&text.get_string());
                }

                NodeData::Element(ref element) => match element.element_data {
                    ElementData::TSpan(_) | ElementData::Text(_) | ElementData::Text2(_) => {
                        let bidi_control = get_bidi_control(element);

                        for &ch in bidi_control.start {
                            result.push(ch);
                        }
                    }
                    _ => {}
                },
            },

            NodeEdge::End(child_node) => {
                if let NodeData::Element(ref element) = *child_node.borrow() {
                    match element.element_data {
                        ElementData::TSpan(_) | ElementData::Text(_) | ElementData::Text2(_) => {
                            let bidi_control = get_bidi_control(element);

                            for &ch in bidi_control.end {
                                result.push(ch);
                            }
                        }

                        _ => {}
                    }
                }
            }
        }
    }

    result
}

/// A range onto which font properties are applied.
///
/// The indices are relative to a certain string, which is then passed on to Pango.
/// The font properties will get translated to a pango::AttrList.
#[allow(unused)]
struct Attributes {
    /// Start byte offset within the `text` of [`FormattedText`].
    start_index: usize,

    /// End byte offset within the `text` of [`FormattedText`].
    end_index: usize,

    /// Font style and properties for this range of text.
    props: FontProperties,
}

/// Text and ranged attributes just prior to text layout.
///
/// This is what gets shipped to Pango for layout.
#[allow(unused)]
struct FormattedText {
    text: String,
    attributes: Vec<Attributes>,
}

// HOMEWORK:
//
// Traverse the text_node in the same way as when collecting the text.  See the comment below
// on what needs to happen while traversing.  We are building a FormattedText that has only
// the addressable characters AND the BidiControl chars, and the corresponding Attributtes/
// for text styling.
//
//
#[allow(unused)]
fn build_formatted_text(
    characters: &[Character],
    text_node: &Node,
    params: &NormalizeParams,
) -> FormattedText {
    let mut indices_stack = Vec::new();
    let mut byte_index = 0;
    let mut num_visited_characters = 0;
    let mut text = String::new();
    let mut attributes = Vec::new();

    for edge in text_node.traverse() {
        match edge {
            NodeEdge::Start(child_node) => match *child_node.borrow() {
                NodeData::Element(ref element) => match element.element_data {
                    ElementData::TSpan(_) | ElementData::Text(_) | ElementData::Text2(_) => {
                        indices_stack.push(byte_index);
                        let bidi_control = get_bidi_control(element);
                        for &ch in bidi_control.start {
                            byte_index += ch.len_utf8();
                            num_visited_characters += 1;
                            text.push(ch);
                        }
                    }
                    _ => {}
                },
                NodeData::Text(_) => {}
            },

            NodeEdge::End(child_node) => match *child_node.borrow() {
                NodeData::Element(ref element) => match element.element_data {
                    ElementData::TSpan(_) | ElementData::Text(_) | ElementData::Text2(_) => {
                        let bidi_control = get_bidi_control(element);
                        for &ch in bidi_control.end {
                            byte_index += ch.len_utf8();
                            num_visited_characters += 1;
                            text.push(ch);
                        }

                        let start_index = indices_stack
                            .pop()
                            .expect("start_index must be pushed already");
                        let values = element.get_computed_values();
                        let font_props = FontProperties::new(values, params);

                        if byte_index > start_index {
                            attributes.push(Attributes {
                                start_index,
                                end_index: byte_index,
                                props: font_props,
                            });
                        }
                    }
                    _ => {}
                },

                NodeData::Text(ref text_ref) => {
                    let text_len = text_ref.get_string().chars().count();
                    for character in characters
                        .iter()
                        .skip(num_visited_characters)
                        .take(text_len)
                    {
                        if character.addressable {
                            text.push(character.character);
                            byte_index += character.character.len_utf8();
                        }
                        num_visited_characters += 1;
                    }
                }
            },
        }
    }

    FormattedText { text, attributes }
}

/// Builds a Pango attribute list from a FormattedText structure.
///
/// This function converts the text styling information in FormattedText
/// into Pango attributes that can be applied to a Pango layout.
#[allow(unused)]
fn build_pango_attr_list(session: &Session, formatted_text: &FormattedText) -> pango::AttrList {
    let attr_list = pango::AttrList::new();

    if formatted_text.text.is_empty() {
        return attr_list;
    }

    for attribute in &formatted_text.attributes {
        // Skip invalid or empty ranges
        if attribute.start_index >= attribute.end_index {
            continue;
        }

        // Validate indices
        let start_index = attribute.start_index.min(formatted_text.text.len());
        let end_index = attribute.end_index.min(formatted_text.text.len());

        assert!(start_index <= end_index);

        let start_index =
            u32::try_from(start_index).expect("Pango attribute index must fit in u32");
        let end_index = u32::try_from(end_index).expect("Pango attribute index must fit in u32");

        // Create font description
        let mut font_desc = pango::FontDescription::new();
        font_desc.set_family(&attribute.props.font_family.0);

        // Handle font size scaling with bounds checking
        if let Some(font_size) = PangoUnits::from_pixels(attribute.props.font_size) {
            font_desc.set_size(font_size.0);
        } else {
            rsvg_log!(
                session,
                "font-size {} is out of bounds; skipping attribute range",
                attribute.props.font_size
            );
        }

        font_desc.set_weight(pango::Weight::from(attribute.props.font_weight));
        font_desc.set_style(pango::Style::from(attribute.props.font_style));
        font_desc.set_stretch(pango::Stretch::from(attribute.props.font_stretch));
        font_desc.set_variant(pango::Variant::from(attribute.props.font_variant));

        let mut font_attr = pango::AttrFontDesc::new(&font_desc).upcast();
        font_attr.set_start_index(start_index);
        font_attr.set_end_index(end_index);
        attr_list.insert(font_attr);

        // Add letter spacing with bounds checking
        if attribute.props.letter_spacing != 0.0 {
            if let Some(spacing) = PangoUnits::from_pixels(attribute.props.letter_spacing) {
                let mut spacing_attr = pango::AttrInt::new_letter_spacing(spacing.0).upcast();
                spacing_attr.set_start_index(start_index);
                spacing_attr.set_end_index(end_index);
                attr_list.insert(spacing_attr);
            } else {
                rsvg_log!(
                    session,
                    "letter-spacing {} is out of bounds; skipping attribute range",
                    attribute.props.letter_spacing
                );
            }
        }

        // Add text decoration attributes
        if attribute.props.text_decoration.overline {
            let mut overline_attr = pango::AttrInt::new_overline(pango::Overline::Single).upcast();
            overline_attr.set_start_index(start_index);
            overline_attr.set_end_index(end_index);
            attr_list.insert(overline_attr);
        }

        if attribute.props.text_decoration.underline {
            let mut underline_attr =
                pango::AttrInt::new_underline(pango::Underline::Single).upcast();
            underline_attr.set_start_index(start_index);
            underline_attr.set_end_index(end_index);
            attr_list.insert(underline_attr);
        }

        if attribute.props.text_decoration.strike {
            let mut strike_attr = pango::AttrInt::new_strikethrough(true).upcast();
            strike_attr.set_start_index(start_index);
            strike_attr.set_end_index(end_index);
            attr_list.insert(strike_attr);
        }
    }

    attr_list
}

struct PangoUnits(i32);

impl PangoUnits {
    fn from_pixels(v: f64) -> Option<Self> {
        // We want (v * f64::from(pango::SCALE) + 0.5) as i32
        // But check for overflow.
        cast::i32(v * f64::from(pango::SCALE) + 0.5)
            .ok()
            .map(PangoUnits)
    }
}

#[cfg(test)]
mod tests {
    use crate::document::Document;
    use crate::dpi::Dpi;
    use crate::element::ElementData;
    use crate::node::NodeBorrow;
    use crate::properties::{FontStyle, FontWeight};

    use super::*;

    #[test]
    fn collects_text_in_a_single_string() {
        let doc_str = br##"<?xml version="1.0" encoding="UTF-8"?>
<svg version="1.1" xmlns="http://www.w3.org/2000/svg" width="100" height="100">

  <text2 id="sample">
    Hello
    <tspan font-style="italic">
      <tspan font-weight="bold">bold</tspan>
      world!
    </tspan>
    How are you.
  </text2>
</svg>
"##;

        let document = Document::load_from_bytes(doc_str);

        let text2_node = document.lookup_internal_node("sample").unwrap();
        assert!(matches!(
            *text2_node.borrow_element_data(),
            ElementData::Text2(_)
        ));

        let text_string = collect_text_from_node(&text2_node);
        assert_eq!(
            text_string,
            "\n    \
             Hello\n    \
             \n      \
             bold\n      \
             world!\n    \
             \n    \
             How are you.\
             \n  "
        );
    }

    #[test]
    fn adds_bidi_control_characters() {
        let doc_str = br##"<?xml version="1.0" encoding="UTF-8"?>
<svg version="1.1" xmlns="http://www.w3.org/2000/svg" width="100" height="100">

  <text2 id="sample">
    Hello
    <tspan direction="rtl" unicode-bidi="embed">
      <tspan direction="ltr" unicode-bidi="isolate-override">bold</tspan>
      world!
    </tspan>
    How are <tspan direction="rtl" unicode-bidi="isolate">you</tspan>.
  </text2>
</svg>
"##;

        let document = Document::load_from_bytes(doc_str);

        let text2_node = document.lookup_internal_node("sample").unwrap();
        assert!(matches!(
            *text2_node.borrow_element_data(),
            ElementData::Text2(_)
        ));

        let text_string = collect_text_from_node(&text2_node);
        assert_eq!(
            text_string,
            "\n    \
             Hello\n    \
             \u{202b}\n      \
             \u{2068}\u{202d}bold\u{202c}\u{2069}\n      \
             world!\n    \
             \u{202c}\n    \
             How are \u{2067}you\u{2069}.\
             \n  "
        );
    }

    // Takes a string made of 't' and 'f' characters, and compares it
    // to the `addressable` field of the Characters slice.
    fn check_true_false_template(template: &str, characters: &[Character]) {
        assert_eq!(characters.len(), template.len());

        // HOMEWORK
        // it's a loop with assert_eq!(characters[i].addressable, ...);
        for (i, ch) in template.chars().enumerate() {
            assert_eq!(characters[i].addressable, ch == 't');
        }
    }

    fn check_modes_with_identical_processing(
        string: &str,
        template: &str,
        mode1: WhiteSpace,
        mode2: WhiteSpace,
    ) {
        let result1 = collapse_white_space(string, mode1);
        check_true_false_template(template, &result1);

        let result2 = collapse_white_space(string, mode2);
        check_true_false_template(template, &result2);
    }

    // white-space="normal" and "nowrap"; these are processed in the same way

    #[rustfmt::skip]
    #[test]
    fn handles_white_space_normal_trivial_case() {
        check_modes_with_identical_processing(
            "hello  world",
            "ttttttfttttt",
            WhiteSpace::Normal,
            WhiteSpace::NoWrap
        );
    }

    #[rustfmt::skip]
    #[test]
    fn handles_white_space_normal_start_of_the_line() {
        check_modes_with_identical_processing(
            "   hello  world",
            "tffttttttfttttt",
            WhiteSpace::Normal,
            WhiteSpace::NoWrap
        );
    }

    #[rustfmt::skip]
    #[test]
    fn handles_white_space_normal_ignores_bidi_control() {
        check_modes_with_identical_processing(
            "A \u{202b} B \u{202c} C",
            "ttffttfft",
            WhiteSpace::Normal,
            WhiteSpace::NoWrap
        );
    }

    // FIXME: here, we need to collapse newlines.  See section https://www.w3.org/TR/css-text-3/#line-break-transform
    //
    // Also, we need to test that consecutive newlines get replaced by a single space, FOR NOW,
    // at least for languages where inter-word spaces actually exist.  For ideographic languages,
    // consecutive newlines need to be removed.
    /*
    #[rustfmt::skip]
    #[test]
    fn handles_white_space_normal_collapses_newlines() {
        check_modes_with_identical_processing(
            "A \n  B \u{202c} C\n\n",
            "ttfffttffttf",
            WhiteSpace::Normal,
            WhiteSpace::NoWrap
        );
    }
    */

    // white-space="pre" and "pre-wrap"; these are processed in the same way

    #[rustfmt::skip]
    #[test]
    fn handles_white_space_pre_trivial_case() {
        check_modes_with_identical_processing(
            "   hello  \n  \n  \n\n\nworld",
            "tttttttttttttttttttttttt",
            WhiteSpace::Pre,
            WhiteSpace::PreWrap
        );
    }

    #[rustfmt::skip]
    #[test]
    fn handles_white_space_pre_ignores_bidi_control() {
        check_modes_with_identical_processing(
            "A  \u{202b} \n\n\n B \u{202c} C  ",
            "tttftttttttftttt",
            WhiteSpace::Pre,
            WhiteSpace::PreWrap
        );
    }

    // This is just to have a way to construct a `NormalizeParams` for tests; we don't
    // actually care what it contains.
    fn dummy_normalize_params() -> NormalizeParams {
        NormalizeParams::from_dpi(Dpi::new(96.0, 96.0))
    }

    #[test]
    fn builds_non_bidi_formatted_text() {
        let doc_str = r##"<?xml version="1.0" encoding="UTF-8"?>
<svg version="1.1" xmlns="http://www.w3.org/2000/svg" width="100" height="100">

  <text2 id="sample" font-family="Foobar">
    Hello <tspan font-weight="bold">böld</tspan> world <tspan font-style="italic">in italics</tspan>!
  </text2>
</svg>
"##;

        let document = Document::load_from_bytes(doc_str.as_bytes());

        let text2_node = document.lookup_internal_node("sample").unwrap();
        assert!(matches!(
            *text2_node.borrow_element_data(),
            ElementData::Text2(_)
        ));

        let collected_text = collect_text_from_node(&text2_node);
        let collapsed_characters = collapse_white_space(&collected_text, WhiteSpace::Normal);

        let formatted = build_formatted_text(
            &collapsed_characters,
            &text2_node,
            &dummy_normalize_params(),
        );

        assert_eq!(&formatted.text, "\nHello böld world in italics!\n");

        // "böld" (note that the ö takes two bytes in UTF-8)
        assert_eq!(formatted.attributes[0].start_index, 7);
        assert_eq!(formatted.attributes[0].end_index, 12);
        assert_eq!(formatted.attributes[0].props.font_weight, FontWeight::Bold);

        // "in italics"
        assert_eq!(formatted.attributes[1].start_index, 19);
        assert_eq!(formatted.attributes[1].end_index, 29);
        assert_eq!(formatted.attributes[1].props.font_style, FontStyle::Italic);

        // the whole string
        assert_eq!(formatted.attributes[2].start_index, 0);
        assert_eq!(formatted.attributes[2].end_index, 31);
        assert_eq!(formatted.attributes[2].props.font_family.0, "Foobar");
    }

    #[test]
    fn builds_bidi_formatted_text() {
        let doc_str = r##"<?xml version="1.0" encoding="UTF-8"?>
<svg version="1.1" xmlns="http://www.w3.org/2000/svg" width="100" height="100">

  <text2 id="sample" font-family="Foobar">
    LTR<tspan direction="rtl" unicode-bidi="embed" font-style="italic">RTL</tspan><tspan font-weight="bold">LTR</tspan>
  </text2>
</svg>
"##;

        let document = Document::load_from_bytes(doc_str.as_bytes());

        let text2_node = document.lookup_internal_node("sample").unwrap();
        assert!(matches!(
            *text2_node.borrow_element_data(),
            ElementData::Text2(_)
        ));

        let collected_text = collect_text_from_node(&text2_node);
        let collapsed_characters = collapse_white_space(&collected_text, WhiteSpace::Normal);

        let formatted = build_formatted_text(
            &collapsed_characters,
            &text2_node,
            &dummy_normalize_params(),
        );

        assert_eq!(&formatted.text, "\nLTR\u{202b}RTL\u{202c}LTR\n");

        // "RTL" surrounded by bidi control chars
        assert_eq!(formatted.attributes[0].start_index, 4);
        assert_eq!(formatted.attributes[0].end_index, 13);
        assert_eq!(formatted.attributes[0].props.font_style, FontStyle::Italic);

        // "LTR" at the end
        assert_eq!(formatted.attributes[1].start_index, 13);
        assert_eq!(formatted.attributes[1].end_index, 16);
        assert_eq!(formatted.attributes[1].props.font_weight, FontWeight::Bold);

        // the whole string
        assert_eq!(formatted.attributes[2].start_index, 0);
        assert_eq!(formatted.attributes[2].end_index, 17);
        assert_eq!(formatted.attributes[2].props.font_family.0, "Foobar");
    }
}
