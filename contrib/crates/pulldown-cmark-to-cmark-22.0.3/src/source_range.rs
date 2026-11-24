use super::{cmark_resume_one_event, fmt, Borrow, Error, Event, Options, Range, State};

/// Serialize a stream of [pulldown-cmark-Events][Event] while preserving the escape characters in `source`.
/// Each input [Event] is accompanied by an optional [Range] that maps it back to the `source` string.
///
/// Different from [`cmark_resume_with_options`](super::cmark_resume_with_options), which always escape
/// Markdown special characters like `#` or `[`, this function only escapes a special character if
/// it is escaped in `source`.
///
/// 1. **source**
///     * Markdown source from which `event_and_ranges` are created.
/// 1. **event_and_ranges**
///    * An iterator over [`Event`]-range pairs, for example as returned by [`pulldown_cmark::OffsetIter`].
///      Must match what's provided in `source`.
/// 1. **formatter**
///    * A format writer, can be a `String`.
/// 1. **state**
///    * The optional initial state of the serialization, useful when the operation should be resumed.
/// 1. **options**
///    * Customize the appearance of the serialization. All otherwise magic values are contained
///      here.
///
/// *Returns* the [`State`] of the serialization on success. You can use it as initial state in the
/// next call if you are halting event serialization.
///
/// *Errors* if the underlying buffer fails (which is unlikely) or if the [`Event`] stream
/// iterated over by `event_and_ranges` cannot ever be produced by deserializing valid Markdown.
/// Each failure mode corresponds to one of [`Error`]'s variants.
pub fn cmark_resume_with_source_range_and_options<'a, I, E, F>(
    event_and_ranges: I,
    source: &'a str,
    mut formatter: F,
    state: Option<State<'a>>,
    options: Options<'_>,
) -> Result<State<'a>, Error>
where
    I: Iterator<Item = (E, Option<Range<usize>>)>,
    E: Borrow<Event<'a>>,
    F: fmt::Write,
{
    let mut state = state.unwrap_or_default();
    for (event, range) in event_and_ranges {
        let update_event_end_index = !matches!(*event.borrow(), Event::Start(_));
        let prevent_escape_leading_special_characters = match (&range, event.borrow()) {
            // Headers and tables can have special characters that aren't at the start
            // of the line, because headers end with `#` and tables have pipes in the middle.
            _ if state.current_heading.is_some() || !state.table_alignments.is_empty() => false,
            // IMPORTANT: Any changes that allow anything other than `Text`
            // breaks the assumption below.
            (Some(range), Event::Text(_)) => {
                range.start <= state.last_event_end_index ||
                // Some source characters are not captured,
                // so check the previous character.
                source.as_bytes().get(range.start.saturating_sub(1)) != Some(&b'\\')
            }
            _ => false,
        } && !state.is_in_code_block();
        if prevent_escape_leading_special_characters {
            // Hack to not escape leading special characters.
            state.code_block = Some(crate::CodeBlockKind::Fenced);
        }
        cmark_resume_one_event(event, &mut formatter, &mut state, &options)?;
        if prevent_escape_leading_special_characters {
            // Assumption: this case only happens when `event` is `Text`,
            // so `state.is_in_code_block` should not be changed to `true`.
            // Also, `state.is_in_code_block` was `false`.
            state.code_block = None;
        }

        if let (true, Some(range)) = (update_event_end_index, range) {
            state.last_event_end_index = range.end;
        }
    }
    Ok(state)
}

/// As [`cmark_resume_with_source_range_and_options`], but with default [`Options`].
pub fn cmark_resume_with_source_range<'a, I, E, F>(
    event_and_ranges: I,
    source: &'a str,
    formatter: F,
    state: Option<State<'a>>,
) -> Result<State<'a>, Error>
where
    I: Iterator<Item = (E, Option<Range<usize>>)>,
    E: Borrow<Event<'a>>,
    F: fmt::Write,
{
    cmark_resume_with_source_range_and_options(event_and_ranges, source, formatter, state, Options::default())
}

/// As [`cmark_resume_with_source_range_and_options`], but with the [`State`] finalized.
pub fn cmark_with_source_range_and_options<'a, I, E, F>(
    event_and_ranges: I,
    source: &'a str,
    mut formatter: F,
    options: Options<'_>,
) -> Result<State<'a>, Error>
where
    I: Iterator<Item = (E, Option<Range<usize>>)>,
    E: Borrow<Event<'a>>,
    F: fmt::Write,
{
    let state = cmark_resume_with_source_range_and_options(
        event_and_ranges,
        source,
        &mut formatter,
        Default::default(),
        options,
    )?;
    state.finalize(formatter)
}

/// As [`cmark_with_source_range_and_options`], but with default [`Options`].
pub fn cmark_with_source_range<'a, I, E, F>(
    event_and_ranges: I,
    source: &'a str,
    mut formatter: F,
) -> Result<State<'a>, Error>
where
    I: Iterator<Item = (E, Option<Range<usize>>)>,
    E: Borrow<Event<'a>>,
    F: fmt::Write,
{
    cmark_with_source_range_and_options(event_and_ranges, source, &mut formatter, Default::default())
}
