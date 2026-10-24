use super::{
    fmt::{self, Write},
    Cow, LinkType, Options, State,
};

/// Write a newline followed by the current [`State::padding`]
/// text that indents the current nested content.
///
/// [`write_padded_newline()`] takes care of writing both a newline character,
/// and the appropriate padding characters, abstracting over the need to
/// carefully pair those actions.
///
/// # Purpose
///
/// Consider a scenario where we're trying to write out the following Markdown
/// (space indents visualized as '·'):
///
/// ```markdown
/// >·A block quote with an embedded list:
/// >·
/// >·* This is a list item that itself contains
/// >···multiple lines and paragraphs of content.
/// >···
/// >···Second paragraph.
/// ```
///
/// Each line of output within the block quote needs to include the text `">·"`
/// at the beginning of the line. Additionally, within the list, each line
/// _also_ needs to start with `"··"` spaces so that the content of the
/// list item is indented.
///
/// Concretely, a call to [`write_padded_newline()`] after the first line in the
/// paragraph of the list item would write `"\n>···"`.
pub(crate) fn write_padded_newline(formatter: &mut impl fmt::Write, state: &State<'_>) -> Result<(), fmt::Error> {
    formatter.write_char('\n')?;
    padding(formatter, &state.padding)?;
    Ok(())
}

pub(crate) fn padding<F>(f: &mut F, p: &[Cow<'_, str>]) -> fmt::Result
where
    F: fmt::Write,
{
    for padding in p {
        write!(f, "{padding}")?;
    }
    Ok(())
}
pub(crate) fn consume_newlines<F>(f: &mut F, s: &mut State<'_>) -> fmt::Result
where
    F: fmt::Write,
{
    while s.newlines_before_start != 0 {
        s.newlines_before_start -= 1;
        write_padded_newline(f, s)?;
    }
    Ok(())
}

pub(crate) fn print_text_without_trailing_newline<F>(t: &str, f: &mut F, state: &State<'_>) -> fmt::Result
where
    F: fmt::Write,
{
    let line_count = t.split('\n').count();
    for (tid, token) in t.split('\n').enumerate() {
        f.write_str(token)?;
        if tid + 1 < line_count {
            write_padded_newline(f, state)?;
        }
    }
    Ok(())
}

pub(crate) fn list_item_padding_of(l: Option<u64>) -> Cow<'static, str> {
    match l {
        None => "  ".into(),
        Some(n) => format!("{n}. ").chars().map(|_| ' ').collect::<String>().into(),
    }
}

pub(crate) fn close_link<F>(uri: &str, title: &str, f: &mut F, link_type: LinkType) -> fmt::Result
where
    F: fmt::Write,
{
    let needs_brackets = {
        let mut depth = 0;
        for b in uri.bytes() {
            match b {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b' ' => {
                    depth += 1;
                    break;
                }
                _ => {}
            }
            if depth > 3 {
                break;
            }
        }
        depth != 0
    };
    let separator = match link_type {
        LinkType::Shortcut => ": ",
        _ => "(",
    };

    if needs_brackets {
        write!(f, "]{separator}<{uri}>")?;
    } else {
        write!(f, "]{separator}{uri}")?;
    }
    if !title.is_empty() {
        write!(f, " \"{title}\"", title = EscapeLinkTitle(title))?;
    }
    if link_type != LinkType::Shortcut {
        f.write_char(')')?;
    }

    Ok(())
}

struct EscapeLinkTitle<'a>(&'a str);

/// Writes a link title with double quotes escaped.
/// See https://spec.commonmark.org/0.30/#link-title for the rules around
/// link titles and the characters they may contain.
impl fmt::Display for EscapeLinkTitle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for c in self.0.chars() {
            match c {
                '"' => f.write_str(r#"\""#)?,
                '\\' => f.write_str(r"\\")?,
                c => f.write_char(c)?,
            }
        }
        Ok(())
    }
}

pub(crate) fn escape_special_characters<'a>(t: &'a str, state: &State<'a>, options: &Options<'a>) -> Cow<'a, str> {
    if state.is_in_code_block() || t.is_empty() {
        return Cow::Borrowed(t);
    }

    let first = t.chars().next().expect("at least one char");
    let first_special = options.special_characters().contains(first);
    let ends_with_special =
        (state.next_is_link_like && t.ends_with("!")) || (state.current_heading.is_some() && t.ends_with("#"));
    let table_contains_pipe = !state.table_alignments.is_empty() && t.contains("|");
    if first_special || ends_with_special || table_contains_pipe {
        let mut s = String::with_capacity(t.len() + 1);
        for (i, c) in t.char_indices() {
            if (i == 0 && first_special) || (i == t.len() - 1 && ends_with_special) || (c == '|' && table_contains_pipe)
            {
                s.push('\\');
            }
            s.push(c);
        }
        Cow::Owned(s)
    } else {
        Cow::Borrowed(t)
    }
}

pub(crate) fn max_consecutive_chars(text: &str, search: char) -> usize {
    let mut in_search_chars = false;
    let mut max_count = 0;
    let mut cur_count = 0;

    for ch in text.chars() {
        if ch == search {
            cur_count += 1;
            in_search_chars = true;
        } else if in_search_chars {
            max_count = max_count.max(cur_count);
            cur_count = 0;
            in_search_chars = false;
        }
    }
    max_count.max(cur_count)
}

#[cfg(test)]
mod max_consecutive_chars {
    use super::max_consecutive_chars;

    #[test]
    fn happens_in_the_entire_string() {
        assert_eq!(
            max_consecutive_chars("``a```b``", '`'),
            3,
            "the highest seen consecutive segment of backticks counts"
        );
        assert_eq!(
            max_consecutive_chars("```a``b`", '`'),
            3,
            "it can't be downgraded later"
        );
    }
}

//=====================================
// General-purpose formatting utilities
//=====================================

/// `Repeated(content, count` formats as `content` repeated `count` times.
#[derive(Debug)]
pub(crate) struct Repeated<T>(pub T, pub usize);

impl<T: fmt::Display> fmt::Display for Repeated<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Repeated(content, count) = self;

        for _ in 0..*count {
            T::fmt(content, f)?;
        }
        Ok(())
    }
}
