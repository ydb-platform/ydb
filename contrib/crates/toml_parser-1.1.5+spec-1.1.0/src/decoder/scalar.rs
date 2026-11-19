use winnow::stream::ContainsToken as _;
use winnow::stream::FindSlice as _;
use winnow::stream::Offset as _;
use winnow::stream::Stream as _;

use crate::ErrorSink;
use crate::Expected;
use crate::ParseError;
use crate::Raw;
use crate::Span;
use crate::decoder::StringBuilder;

const ALLOCATION_ERROR: &str = "could not allocate for string";

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum ScalarKind {
    String,
    Boolean(bool),
    DateTime,
    Float,
    Integer(IntegerRadix),
}

impl ScalarKind {
    pub fn description(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Boolean(_) => "boolean",
            Self::DateTime => "date-time",
            Self::Float => "float",
            Self::Integer(radix) => radix.description(),
        }
    }

    pub fn invalid_description(&self) -> &'static str {
        match self {
            Self::String => "invalid string",
            Self::Boolean(_) => "invalid boolean",
            Self::DateTime => "invalid date-time",
            Self::Float => "invalid float",
            Self::Integer(radix) => radix.invalid_description(),
        }
    }
}

#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum IntegerRadix {
    #[default]
    Dec,
    Hex,
    Oct,
    Bin,
}

impl IntegerRadix {
    pub fn description(&self) -> &'static str {
        match self {
            Self::Dec => "integer",
            Self::Hex => "hexadecimal",
            Self::Oct => "octal",
            Self::Bin => "binary",
        }
    }

    pub fn value(&self) -> u32 {
        match self {
            Self::Dec => 10,
            Self::Hex => 16,
            Self::Oct => 8,
            Self::Bin => 2,
        }
    }

    pub fn invalid_description(&self) -> &'static str {
        match self {
            Self::Dec => "invalid integer number",
            Self::Hex => "invalid hexadecimal number",
            Self::Oct => "invalid octal number",
            Self::Bin => "invalid binary number",
        }
    }

    fn validator(&self) -> fn(char) -> bool {
        match self {
            Self::Dec => |c| c.is_ascii_digit(),
            Self::Hex => |c| c.is_ascii_hexdigit(),
            Self::Oct => |c| matches!(c, '0'..='7'),
            Self::Bin => |c| matches!(c, '0'..='1'),
        }
    }
}

pub(crate) fn decode_unquoted_scalar<'i>(
    raw: Raw<'i>,
    output: &mut dyn StringBuilder<'i>,
    error: &mut dyn ErrorSink,
) -> ScalarKind {
    let s = raw.as_str();
    let Some(first) = s.as_bytes().first() else {
        return decode_invalid(raw, output, error);
    };
    if !first.is_ascii_digit() && s.contains(" ") {
        // Only datetimes can have a space
        return decode_invalid(raw, output, error);
    }
    match first {
        // number starts
        b'+' | b'-' => {
            let value = &raw.as_str()[1..];
            decode_sign_prefix(raw, value, output, error)
        }
        // Report as if they were numbers because its most likely a typo
        b'_' => decode_datetime_or_float_or_integer(raw.as_str(), raw, output, error),
        // Date/number starts
        b'0' => decode_zero_prefix(raw.as_str(), false, raw, output, error),
        b'1'..=b'9' => decode_datetime_or_float_or_integer(raw.as_str(), raw, output, error),
        // Report as if they were numbers because its most likely a typo
        b'.' => {
            let kind = ScalarKind::Float;
            let stream = raw.as_str();
            if ensure_float(stream, raw, error) {
                decode_float_or_integer(stream, raw, kind, output, error)
            } else {
                kind
            }
        }
        b't' | b'T' => {
            const SYMBOL: &str = "true";
            let kind = ScalarKind::Boolean(true);
            let expected = &[Expected::Literal(SYMBOL)];
            decode_symbol(raw, SYMBOL, kind, expected, output, error)
        }
        b'f' | b'F' => {
            const SYMBOL: &str = "false";
            let kind = ScalarKind::Boolean(false);
            let expected = &[Expected::Literal(SYMBOL)];
            decode_symbol(raw, SYMBOL, kind, expected, output, error)
        }
        b'i' | b'I' => {
            const SYMBOL: &str = "inf";
            let kind = ScalarKind::Float;
            let expected = &[Expected::Literal(SYMBOL)];
            decode_symbol(raw, SYMBOL, kind, expected, output, error)
        }
        b'n' | b'N' => {
            const SYMBOL: &str = "nan";
            let kind = ScalarKind::Float;
            let expected = &[Expected::Literal(SYMBOL)];
            decode_symbol(raw, SYMBOL, kind, expected, output, error)
        }
        _ => decode_invalid(raw, output, error),
    }
}

fn decode_sign_prefix<'i>(
    raw: Raw<'i>,
    value: &'i str,
    output: &mut dyn StringBuilder<'i>,
    error: &mut dyn ErrorSink,
) -> ScalarKind {
    let mut value = value;
    let first = loop {
        let Some(first) = value.as_bytes().first() else {
            return decode_invalid(raw, output, error);
        };
        if !matches!(first, b'+' | b'-') {
            break first;
        }
        let start = value.offset_from(&raw.as_str());
        let end = start + 1;
        error.report_error(
            ParseError::new("redundant numeric sign")
                .with_context(Span::new_unchecked(0, raw.len()))
                .with_expected(&[])
                .with_unexpected(Span::new_unchecked(start, end)),
        );
        value = &value[1..];
    };
    match first {
        // Report as if they were numbers because its most likely a typo
        b'_' => decode_datetime_or_float_or_integer(value, raw, output, error),
        // Date/number starts
        b'0' => decode_zero_prefix(value, true, raw, output, error),
        b'1'..=b'9' => decode_datetime_or_float_or_integer(value, raw, output, error),
        // Report as if they were numbers because its most likely a typo
        b'.' => {
            let kind = ScalarKind::Float;
            let stream = raw.as_str();
            if ensure_float(stream, raw, error) {
                decode_float_or_integer(stream, raw, kind, output, error)
            } else {
                kind
            }
        }
        b'i' | b'I' => {
            const SYMBOL: &str = "inf";
            let kind = ScalarKind::Float;
            if value != SYMBOL {
                let expected = &[Expected::Literal(SYMBOL)];
                let start = value.offset_from(&raw.as_str());
                let end = start + value.len();
                error.report_error(
                    ParseError::new(kind.invalid_description())
                        .with_context(Span::new_unchecked(0, raw.len()))
                        .with_expected(expected)
                        .with_unexpected(Span::new_unchecked(start, end)),
                );
                decode_as(raw, SYMBOL, kind, output, error)
            } else {
                decode_as_is(raw, kind, output, error)
            }
        }
        b'n' | b'N' => {
            const SYMBOL: &str = "nan";
            let kind = ScalarKind::Float;
            if value != SYMBOL {
                let expected = &[Expected::Literal(SYMBOL)];
                let start = value.offset_from(&raw.as_str());
                let end = start + value.len();
                error.report_error(
                    ParseError::new(kind.invalid_description())
                        .with_context(Span::new_unchecked(0, raw.len()))
                        .with_expected(expected)
                        .with_unexpected(Span::new_unchecked(start, end)),
                );
                decode_as(raw, SYMBOL, kind, output, error)
            } else {
                decode_as_is(raw, kind, output, error)
            }
        }
        _ => decode_invalid(raw, output, error),
    }
}

fn decode_zero_prefix<'i>(
    value: &'i str,
    signed: bool,
    raw: Raw<'i>,
    output: &mut dyn StringBuilder<'i>,
    error: &mut dyn ErrorSink,
) -> ScalarKind {
    debug_assert_eq!(value.as_bytes()[0], b'0');
    if value.len() == 1 {
        let kind = ScalarKind::Integer(IntegerRadix::Dec);
        // No extra validation needed
        decode_float_or_integer(raw.as_str(), raw, kind, output, error)
    } else {
        let radix = value.as_bytes()[1];
        match radix {
            b'x' | b'X' => {
                if value.contains(" ") {
                    // Only datetimes can have a space
                    return decode_invalid(raw, output, error);
                }
                if signed {
                    error.report_error(
                        ParseError::new("integers with a radix cannot be signed")
                            .with_context(Span::new_unchecked(0, raw.len()))
                            .with_expected(&[])
                            .with_unexpected(Span::new_unchecked(0, 1)),
                    );
                }
                if radix == b'X' {
                    let start = value.offset_from(&raw.as_str());
                    let end = start + 2;
                    error.report_error(
                        ParseError::new("radix must be lowercase")
                            .with_context(Span::new_unchecked(0, raw.len()))
                            .with_expected(&[Expected::Literal("0x")])
                            .with_unexpected(Span::new_unchecked(start, end)),
                    );
                }
                let radix = IntegerRadix::Hex;
                let kind = ScalarKind::Integer(radix);
                let stream = &value[2..];
                if ensure_radixed_value(stream, raw, radix, error) {
                    decode_float_or_integer(stream, raw, kind, output, error)
                } else {
                    kind
                }
            }
            b'o' | b'O' => {
                if value.contains(" ") {
                    // Only datetimes can have a space
                    return decode_invalid(raw, output, error);
                }
                if signed {
                    error.report_error(
                        ParseError::new("integers with a radix cannot be signed")
                            .with_context(Span::new_unchecked(0, raw.len()))
                            .with_expected(&[])
                            .with_unexpected(Span::new_unchecked(0, 1)),
                    );
                }
                if radix == b'O' {
                    let start = value.offset_from(&raw.as_str());
                    let end = start + 2;
                    error.report_error(
                        ParseError::new("radix must be lowercase")
                            .with_context(Span::new_unchecked(0, raw.len()))
                            .with_expected(&[Expected::Literal("0o")])
                            .with_unexpected(Span::new_unchecked(start, end)),
                    );
                }
                let radix = IntegerRadix::Oct;
                let kind = ScalarKind::Integer(radix);
                let stream = &value[2..];
                if ensure_radixed_value(stream, raw, radix, error) {
                    decode_float_or_integer(stream, raw, kind, output, error)
                } else {
                    kind
                }
            }
            b'b' | b'B' => {
                if value.contains(" ") {
                    // Only datetimes can have a space
                    return decode_invalid(raw, output, error);
                }
                if signed {
                    error.report_error(
                        ParseError::new("integers with a radix cannot be signed")
                            .with_context(Span::new_unchecked(0, raw.len()))
                            .with_expected(&[])
                            .with_unexpected(Span::new_unchecked(0, 1)),
                    );
                }
                if radix == b'B' {
                    let start = value.offset_from(&raw.as_str());
                    let end = start + 2;
                    error.report_error(
                        ParseError::new("radix must be lowercase")
                            .with_context(Span::new_unchecked(0, raw.len()))
                            .with_expected(&[Expected::Literal("0b")])
                            .with_unexpected(Span::new_unchecked(start, end)),
                    );
                }
                let radix = IntegerRadix::Bin;
                let kind = ScalarKind::Integer(radix);
                let stream = &value[2..];
                if ensure_radixed_value(stream, raw, radix, error) {
                    decode_float_or_integer(stream, raw, kind, output, error)
                } else {
                    kind
                }
            }
            b'd' | b'D' => {
                if value.contains(" ") {
                    // Only datetimes can have a space
                    return decode_invalid(raw, output, error);
                }
                if signed {
                    error.report_error(
                        ParseError::new("integers with a radix cannot be signed")
                            .with_context(Span::new_unchecked(0, raw.len()))
                            .with_expected(&[])
                            .with_unexpected(Span::new_unchecked(0, 1)),
                    );
                }
                let radix = IntegerRadix::Dec;
                let kind = ScalarKind::Integer(radix);
                let stream = &value[2..];
                error.report_error(
                    ParseError::new("redundant integer number prefix")
                        .with_context(Span::new_unchecked(0, raw.len()))
                        .with_expected(&[])
                        .with_unexpected(Span::new_unchecked(0, 2)),
                );
                if ensure_radixed_value(stream, raw, radix, error) {
                    decode_float_or_integer(stream, raw, kind, output, error)
                } else {
                    kind
                }
            }
            _ => decode_datetime_or_float_or_integer(value, raw, output, error),
        }
    }
}

fn decode_datetime_or_float_or_integer<'i>(
    value: &'i str,
    raw: Raw<'i>,
    output: &mut dyn StringBuilder<'i>,
    error: &mut dyn ErrorSink,
) -> ScalarKind {
    let Some(digit_end) = value
        .as_bytes()
        .offset_for(|b| !(b'0'..=b'9').contains_token(b))
    else {
        let kind = ScalarKind::Integer(IntegerRadix::Dec);
        let stream = raw.as_str();
        if ensure_no_leading_zero(value, raw, error) {
            return decode_float_or_integer(stream, raw, kind, output, error);
        } else {
            return kind;
        }
    };

    #[cfg(feature = "unsafe")] // SAFETY: ascii digits ensures UTF-8 boundary
    let rest = unsafe { &value.get_unchecked(digit_end..) };
    #[cfg(not(feature = "unsafe"))]
    let rest = &value[digit_end..];

    if rest.starts_with("-") || rest.starts_with(":") {
        decode_as_is(raw, ScalarKind::DateTime, output, error)
    } else if rest.contains(" ") {
        decode_invalid(raw, output, error)
    } else if is_float(rest) {
        let kind = ScalarKind::Float;
        let stream = raw.as_str();
        if ensure_float(value, raw, error) {
            decode_float_or_integer(stream, raw, kind, output, error)
        } else {
            kind
        }
    } else if rest.starts_with("_") {
        let kind = ScalarKind::Integer(IntegerRadix::Dec);
        let stream = raw.as_str();
        if ensure_no_leading_zero(value, raw, error) {
            decode_float_or_integer(stream, raw, kind, output, error)
        } else {
            kind
        }
    } else {
        decode_invalid(raw, output, error)
    }
}

/// ```abnf
/// ;; Float
///
/// float = float-int-part ( exp / frac [ exp ] )
///
/// float-int-part = dec-int
/// frac = decimal-point zero-prefixable-int
/// decimal-point = %x2E               ; .
/// zero-prefixable-int = DIGIT *( DIGIT / underscore DIGIT )
///
/// exp = "e" float-exp-part
/// float-exp-part = [ minus / plus ] zero-prefixable-int
/// ```
#[must_use]
fn ensure_float<'i>(mut value: &'i str, raw: Raw<'i>, error: &mut dyn ErrorSink) -> bool {
    let mut is_valid = true;

    is_valid &= ensure_dec_uint(&mut value, raw, false, "invalid mantissa", error);

    if value.starts_with(".") {
        let _ = value.next_token();
        is_valid &= ensure_dec_uint(&mut value, raw, true, "invalid fraction", error);
    }

    if value.starts_with(['e', 'E']) {
        let _ = value.next_token();
        if value.starts_with(['+', '-']) {
            let _ = value.next_token();
        }
        is_valid &= ensure_dec_uint(&mut value, raw, true, "invalid exponent", error);
    }

    if !value.is_empty() {
        let start = value.offset_from(&raw.as_str());
        let end = raw.len();
        error.report_error(
            ParseError::new(ScalarKind::Float.invalid_description())
                .with_context(Span::new_unchecked(0, raw.len()))
                .with_expected(&[])
                .with_unexpected(Span::new_unchecked(start, end)),
        );
        is_valid = false;
    }

    is_valid
}

#[must_use]
fn ensure_dec_uint<'i>(
    value: &mut &'i str,
    raw: Raw<'i>,
    zero_prefix: bool,
    invalid_description: &'static str,
    error: &mut dyn ErrorSink,
) -> bool {
    let mut is_valid = true;

    let start = *value;
    let mut digit_count = 0;
    while let Some(current) = value.chars().next() {
        if current.is_ascii_digit() {
            digit_count += 1;
        } else if current == '_' {
        } else {
            break;
        }
        let _ = value.next_token();
    }

    match digit_count {
        0 => {
            let start = start.offset_from(&raw.as_str());
            let end = start;
            error.report_error(
                ParseError::new(invalid_description)
                    .with_context(Span::new_unchecked(0, raw.len()))
                    .with_expected(&[Expected::Description("digits")])
                    .with_unexpected(Span::new_unchecked(start, end)),
            );
            is_valid = false;
        }
        1 => {}
        _ if start.starts_with("0") && !zero_prefix => {
            let start = start.offset_from(&raw.as_str());
            let end = start + 1;
            error.report_error(
                ParseError::new("unexpected leading zero")
                    .with_context(Span::new_unchecked(0, raw.len()))
                    .with_expected(&[])
                    .with_unexpected(Span::new_unchecked(start, end)),
            );
            is_valid = false;
        }
        _ => {}
    }

    is_valid
}

#[must_use]
fn ensure_no_leading_zero<'i>(value: &'i str, raw: Raw<'i>, error: &mut dyn ErrorSink) -> bool {
    let mut is_valid = true;

    if value.starts_with("0") {
        let start = value.offset_from(&raw.as_str());
        let end = start + 1;
        error.report_error(
            ParseError::new("unexpected leading zero")
                .with_context(Span::new_unchecked(0, raw.len()))
                .with_expected(&[])
                .with_unexpected(Span::new_unchecked(start, end)),
        );
        is_valid = false;
    }

    is_valid
}

#[must_use]
fn ensure_radixed_value(
    value: &str,
    raw: Raw<'_>,
    radix: IntegerRadix,
    error: &mut dyn ErrorSink,
) -> bool {
    let mut is_valid = true;

    let invalid = ['+', '-'];
    let value = if let Some(value) = value.strip_prefix(invalid) {
        let pos = raw.as_str().find(invalid).unwrap();
        error.report_error(
            ParseError::new("unexpected sign")
                .with_context(Span::new_unchecked(0, raw.len()))
                .with_expected(&[])
                .with_unexpected(Span::new_unchecked(pos, pos + 1)),
        );
        is_valid = false;
        value
    } else {
        value
    };

    let valid = radix.validator();
    for (index, c) in value.char_indices() {
        if !valid(c) && c != '_' {
            let pos = value.offset_from(&raw.as_str()) + index;
            error.report_error(
                ParseError::new(radix.invalid_description())
                    .with_context(Span::new_unchecked(0, raw.len()))
                    .with_unexpected(Span::new_unchecked(pos, pos)),
            );
            is_valid = false;
        }
    }

    is_valid
}

fn decode_float_or_integer<'i>(
    mut stream: &'i str,
    raw: Raw<'i>,
    kind: ScalarKind,
    output: &mut dyn StringBuilder<'i>,
    error: &mut dyn ErrorSink,
) -> ScalarKind {
    output.clear();

    let underscore = "_";

    let stream_start = stream.offset_from(&raw.as_str());
    while !stream.is_empty() {
        let sep_pos = stream.find_slice(underscore);
        let sep_start = sep_pos
            .clone()
            .map(|r| r.start)
            .unwrap_or_else(|| stream.len());

        let part_start = stream.offset_from(&raw.as_str());
        let part_end = part_start + sep_start;
        let part = stream.next_slice(sep_start);

        if sep_pos.is_some() {
            let _ = stream.next_slice(underscore.len());

            let mut is_invalid_sep = false;
            if let Some(last_pos) = sep_start.checked_sub(1) {
                let last_byte = raw.as_bytes()[part_start + last_pos];
                if !is_any_digit(last_byte, kind) {
                    is_invalid_sep = true;
                }
            } else if part_start == stream_start {
                is_invalid_sep = true;
            }

            if let Some(next_byte) = stream.as_bytes().first() {
                if !is_any_digit(*next_byte, kind) {
                    is_invalid_sep = true;
                }
            } else if stream.is_empty() {
                is_invalid_sep = true;
            }

            if is_invalid_sep {
                let start = part_end;
                let end = start + underscore.len();
                error.report_error(
                    ParseError::new("`_` may only go between digits")
                        .with_context(Span::new_unchecked(0, raw.len()))
                        .with_expected(&[])
                        .with_unexpected(Span::new_unchecked(end - underscore.len(), end)),
                );
            }
        }

        if !part.is_empty() && !output.push_str(part) {
            error.report_error(
                ParseError::new(ALLOCATION_ERROR)
                    .with_unexpected(Span::new_unchecked(part_start, part_end)),
            );
        }
    }

    kind
}

fn is_any_digit(b: u8, kind: ScalarKind) -> bool {
    if kind == ScalarKind::Float {
        is_dec_integer_digit(b)
    } else {
        is_any_integer_digit(b)
    }
}

fn is_any_integer_digit(b: u8) -> bool {
    (b'0'..=b'9', b'a'..=b'f', b'A'..=b'F').contains_token(b)
}

fn is_dec_integer_digit(b: u8) -> bool {
    (b'0'..=b'9').contains_token(b)
}

fn is_float(raw: &str) -> bool {
    raw.as_bytes().find_slice((b'.', b'e', b'E')).is_some()
}

fn decode_as_is<'i>(
    raw: Raw<'i>,
    kind: ScalarKind,
    output: &mut dyn StringBuilder<'i>,
    error: &mut dyn ErrorSink,
) -> ScalarKind {
    let kind = decode_as(raw, raw.as_str(), kind, output, error);
    kind
}

fn decode_as<'i>(
    raw: Raw<'i>,
    symbol: &'i str,
    kind: ScalarKind,
    output: &mut dyn StringBuilder<'i>,
    error: &mut dyn ErrorSink,
) -> ScalarKind {
    output.clear();
    if !output.push_str(symbol) {
        error.report_error(
            ParseError::new(ALLOCATION_ERROR).with_unexpected(Span::new_unchecked(0, raw.len())),
        );
    }
    kind
}

fn decode_symbol<'i>(
    raw: Raw<'i>,
    symbol: &'static str,
    kind: ScalarKind,
    expected: &'static [Expected],
    output: &mut dyn StringBuilder<'i>,
    error: &mut dyn ErrorSink,
) -> ScalarKind {
    if raw.as_str() != symbol {
        if raw.as_str().contains(" ") {
            return decode_invalid(raw, output, error);
        } else {
            error.report_error(
                ParseError::new(kind.invalid_description())
                    .with_context(Span::new_unchecked(0, raw.len()))
                    .with_expected(expected)
                    .with_unexpected(Span::new_unchecked(0, raw.len())),
            );
        }
    }

    decode_as(raw, symbol, kind, output, error)
}

fn decode_invalid<'i>(
    raw: Raw<'i>,
    output: &mut dyn StringBuilder<'i>,
    error: &mut dyn ErrorSink,
) -> ScalarKind {
    if raw.as_str().ends_with("'''") {
        error.report_error(
            ParseError::new("missing opening quote")
                .with_context(Span::new_unchecked(0, raw.len()))
                .with_expected(&[Expected::Literal(r#"'''"#)])
                .with_unexpected(Span::new_unchecked(0, 0)),
        );
    } else if raw.as_str().ends_with(r#"""""#) {
        error.report_error(
            ParseError::new("missing opening quote")
                .with_context(Span::new_unchecked(0, raw.len()))
                .with_expected(&[Expected::Description("multi-line basic string")])
                .with_expected(&[Expected::Literal(r#"""""#)])
                .with_unexpected(Span::new_unchecked(0, 0)),
        );
    } else if raw.as_str().ends_with("'") {
        error.report_error(
            ParseError::new("missing opening quote")
                .with_context(Span::new_unchecked(0, raw.len()))
                .with_expected(&[Expected::Literal(r#"'"#)])
                .with_unexpected(Span::new_unchecked(0, 0)),
        );
    } else if raw.as_str().ends_with(r#"""#) {
        error.report_error(
            ParseError::new("missing opening quote")
                .with_context(Span::new_unchecked(0, raw.len()))
                .with_expected(&[Expected::Literal(r#"""#)])
                .with_unexpected(Span::new_unchecked(0, 0)),
        );
    } else {
        error.report_error(
            ParseError::new("string values must be quoted")
                .with_context(Span::new_unchecked(0, raw.len()))
                .with_expected(&[Expected::Description("literal string")])
                .with_unexpected(Span::new_unchecked(0, raw.len())),
        );
    }

    output.clear();
    if !output.push_str(raw.as_str()) {
        error.report_error(
            ParseError::new(ALLOCATION_ERROR).with_unexpected(Span::new_unchecked(0, raw.len())),
        );
    }
    ScalarKind::String
}

#[cfg(test)]
#[cfg(feature = "std")]
mod test {
    use super::*;
    use alloc::borrow::Cow;

    #[test]
    fn many_redundant_signs() {
        // Regression test: decode_sign_prefix previously recursed once per sign
        // character, causing a stack overflow on long runs.
        let signs = "-".repeat(5_000);
        let input = format!("{signs}1");

        let mut error = Vec::new();
        let mut output = Cow::Borrowed("");
        decode_unquoted_scalar(
            Raw::new_unchecked(&input, None, Default::default()),
            &mut output,
            &mut error,
        );
        assert!(!error.is_empty());
    }
}
