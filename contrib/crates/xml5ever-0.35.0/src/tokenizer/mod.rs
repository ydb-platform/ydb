// Copyright 2014-2017 The html5ever Project Developers. See the
// COPYRIGHT file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

mod char_ref;
mod interface;
mod qname;
pub mod states;

pub use self::interface::{
    Doctype, EmptyTag, EndTag, Pi, ShortTag, StartTag, Tag, TagKind, Token, TokenSink,
};
pub use crate::{LocalName, Namespace, Prefix};

use crate::macros::{time, unwrap_or_return};
use crate::tendril::StrTendril;
use crate::{buffer_queue, Attribute, QualName, SmallCharSet};
use log::debug;
use markup5ever::{local_name, namespace_prefix, ns, small_char_set, TokenizerResult};
use std::borrow::Cow::{self, Borrowed};
use std::cell::{Cell, RefCell, RefMut};
use std::collections::BTreeMap;
use std::mem::replace;

use self::buffer_queue::{BufferQueue, FromSet, NotFromSet, SetResult};
use self::char_ref::{CharRef, CharRefTokenizer};
use self::qname::QualNameTokenizer;
use self::states::XmlState;
use self::states::{DoctypeKind, Public, System};
use self::states::{DoubleQuoted, SingleQuoted, Unquoted};

/// Copy of Tokenizer options, with an impl for `Default`.
#[derive(Copy, Clone)]
pub struct XmlTokenizerOpts {
    /// Report all parse errors described in the spec, at some
    /// performance penalty?  Default: false
    pub exact_errors: bool,

    /// Discard a `U+FEFF BYTE ORDER MARK` if we see one at the beginning
    /// of the stream?  Default: true
    pub discard_bom: bool,

    /// Keep a record of how long we spent in each state?  Printed
    /// when `end()` is called.  Default: false
    pub profile: bool,

    /// Initial state override.  Only the test runner should use
    /// a non-`None` value!
    pub initial_state: Option<states::XmlState>,
}

fn process_qname(tag_name: StrTendril) -> QualName {
    // If tag name can't possibly contain full namespace, skip qualified name
    // parsing altogether. For a tag to have namespace it must look like:
    //     a:b
    // Since StrTendril are UTF-8, we know that minimal size in bytes must be
    // three bytes minimum.
    let split = if (*tag_name).len() < 3 {
        None
    } else {
        QualNameTokenizer::new((*tag_name).as_bytes()).run()
    };

    match split {
        None => QualName::new(None, ns!(), LocalName::from(&*tag_name)),
        Some(col) => {
            let len = (*tag_name).len() as u32;
            let prefix = tag_name.subtendril(0, col);
            let local = tag_name.subtendril(col + 1, len - col - 1);
            let ns = ns!(); // Actual namespace URL set in XmlTreeBuilder::bind_qname
            QualName::new(Some(Prefix::from(&*prefix)), ns, LocalName::from(&*local))
        },
    }
}

fn option_push(opt_str: &mut Option<StrTendril>, c: char) {
    match *opt_str {
        Some(ref mut s) => s.push_char(c),
        None => *opt_str = Some(StrTendril::from_char(c)),
    }
}

impl Default for XmlTokenizerOpts {
    fn default() -> XmlTokenizerOpts {
        XmlTokenizerOpts {
            exact_errors: false,
            discard_bom: true,
            profile: false,
            initial_state: None,
        }
    }
}
/// The Xml tokenizer.
pub struct XmlTokenizer<Sink> {
    /// Options controlling the behavior of the tokenizer.
    opts: XmlTokenizerOpts,

    /// Destination for tokens we emit.
    pub sink: Sink,

    /// The abstract machine state as described in the spec.
    state: Cell<states::XmlState>,

    /// Are we at the end of the file, once buffers have been processed
    /// completely? This affects whether we will wait for lookahead or not.
    at_eof: Cell<bool>,

    /// Tokenizer for character references, if we're tokenizing
    /// one at the moment.
    char_ref_tokenizer: RefCell<Option<Box<CharRefTokenizer>>>,

    /// Current input character.  Just consumed, may reconsume.
    current_char: Cell<char>,

    /// Should we reconsume the current input character?
    reconsume: Cell<bool>,

    /// Did we just consume \r, translating it to \n?  In that case we need
    /// to ignore the next character if it's \n.
    ignore_lf: Cell<bool>,

    /// Discard a U+FEFF BYTE ORDER MARK if we see one?  Only done at the
    /// beginning of the stream.
    discard_bom: Cell<bool>,

    /// Temporary buffer
    temp_buf: RefCell<StrTendril>,

    /// Current tag kind.
    current_tag_kind: Cell<TagKind>,

    /// Current tag name.
    current_tag_name: RefCell<StrTendril>,

    /// Current tag attributes.
    current_tag_attrs: RefCell<Vec<Attribute>>,

    /// Current attribute name.
    current_attr_name: RefCell<StrTendril>,

    /// Current attribute value.
    current_attr_value: RefCell<StrTendril>,

    current_doctype: RefCell<Doctype>,

    /// Current comment.
    current_comment: RefCell<StrTendril>,

    /// Current processing instruction target.
    current_pi_target: RefCell<StrTendril>,

    /// Current processing instruction value.
    current_pi_data: RefCell<StrTendril>,

    /// Record of how many ns we spent in each state, if profiling is enabled.
    state_profile: RefCell<BTreeMap<states::XmlState, u64>>,

    /// Record of how many ns we spent in the token sink.
    time_in_sink: Cell<u64>,
}

impl<Sink: TokenSink> XmlTokenizer<Sink> {
    /// Create a new tokenizer which feeds tokens to a particular `TokenSink`.
    pub fn new(sink: Sink, opts: XmlTokenizerOpts) -> XmlTokenizer<Sink> {
        if opts.profile && cfg!(for_c) {
            panic!("Can't profile tokenizer when built as a C library");
        }

        let state = *opts.initial_state.as_ref().unwrap_or(&states::Data);
        let discard_bom = opts.discard_bom;
        XmlTokenizer {
            opts,
            sink,
            state: Cell::new(state),
            char_ref_tokenizer: RefCell::new(None),
            at_eof: Cell::new(false),
            current_char: Cell::new('\0'),
            reconsume: Cell::new(false),
            ignore_lf: Cell::new(false),
            temp_buf: RefCell::new(StrTendril::new()),
            discard_bom: Cell::new(discard_bom),
            current_tag_kind: Cell::new(StartTag),
            current_tag_name: RefCell::new(StrTendril::new()),
            current_tag_attrs: RefCell::new(vec![]),
            current_attr_name: RefCell::new(StrTendril::new()),
            current_attr_value: RefCell::new(StrTendril::new()),
            current_comment: RefCell::new(StrTendril::new()),
            current_pi_data: RefCell::new(StrTendril::new()),
            current_pi_target: RefCell::new(StrTendril::new()),
            current_doctype: RefCell::new(Doctype::default()),
            state_profile: RefCell::new(BTreeMap::new()),
            time_in_sink: Cell::new(0),
        }
    }

    /// Feed an input string into the tokenizer.
    pub fn feed(&self, input: &BufferQueue) -> TokenizerResult<Sink::Handle> {
        if input.is_empty() {
            return TokenizerResult::Done;
        }

        if self.discard_bom.get() {
            if let Some(c) = input.peek() {
                if c == '\u{feff}' {
                    input.next();
                }
            } else {
                return TokenizerResult::Done;
            }
        };

        self.run(input)
    }

    fn process_token(&self, token: Token) -> ProcessResult<Sink::Handle> {
        if self.opts.profile {
            let (result, dt) = time!(self.sink.process_token(token));
            self.time_in_sink.set(self.time_in_sink.get() + dt);
            result
        } else {
            self.sink.process_token(token)
        }
    }

    // Get the next input character, which might be the character
    // 'c' that we already consumed from the buffers.
    fn get_preprocessed_char(&self, mut c: char, input: &BufferQueue) -> Option<char> {
        if self.ignore_lf.get() {
            self.ignore_lf.set(false);
            if c == '\n' {
                c = input.next()?;
            }
        }

        if c == '\r' {
            self.ignore_lf.set(true);
            c = '\n';
        }

        // Normalize \x00 into \uFFFD
        if c == '\x00' {
            c = '\u{FFFD}'
        }

        // Exclude forbidden Unicode characters
        if self.opts.exact_errors
            && match c as u32 {
                0x01..=0x08 | 0x0B | 0x0E..=0x1F | 0x7F..=0x9F | 0xFDD0..=0xFDEF => true,
                n if (n & 0xFFFE) == 0xFFFE => true,
                _ => false,
            }
        {
            let msg = format!("Bad character {c}");
            self.emit_error(Cow::Owned(msg));
        }

        debug!("got character {c}");
        self.current_char.set(c);
        Some(c)
    }

    fn bad_eof_error(&self) {
        let msg = if self.opts.exact_errors {
            Cow::from(format!("Saw EOF in state {:?}", self.state))
        } else {
            Cow::from("Unexpected EOF")
        };
        self.emit_error(msg);
    }

    fn pop_except_from(&self, input: &BufferQueue, set: SmallCharSet) -> Option<SetResult> {
        // Bail to the slow path for various corner cases.
        // This means that `FromSet` can contain characters not in the set!
        // It shouldn't matter because the fallback `FromSet` case should
        // always do the same thing as the `NotFromSet` case.
        if self.opts.exact_errors || self.reconsume.get() || self.ignore_lf.get() {
            return self.get_char(input).map(FromSet);
        }

        let d = input.pop_except_from(set);
        debug!("got characters {d:?}");
        match d {
            Some(FromSet(c)) => self.get_preprocessed_char(c, input).map(FromSet),

            // NB: We don't set self.current_char for a run of characters not
            // in the set.  It shouldn't matter for the codepaths that use
            // this.
            _ => d,
        }
    }

    // Check if the next characters are an ASCII case-insensitive match.  See
    // BufferQueue::eat.
    //
    // NB: this doesn't do input stream preprocessing or set the current input
    // character.
    fn eat(&self, input: &BufferQueue, pat: &str) -> Option<bool> {
        input.push_front(replace(&mut *self.temp_buf.borrow_mut(), StrTendril::new()));
        match input.eat(pat, u8::eq_ignore_ascii_case) {
            None if self.at_eof.get() => Some(false),
            None => {
                let mut temp_buf = self.temp_buf.borrow_mut();
                while let Some(data) = input.next() {
                    temp_buf.push_char(data);
                }
                None
            },
            Some(matched) => Some(matched),
        }
    }

    /// Run the state machine for as long as we can.
    pub fn run(&self, input: &BufferQueue) -> TokenizerResult<Sink::Handle> {
        if self.opts.profile {
            loop {
                let state = self.state.get();
                let old_sink = self.time_in_sink.get();
                let (run, mut dt) = time!(self.step(input));
                dt -= self.time_in_sink.get() - old_sink;
                let new = match self.state_profile.borrow_mut().get_mut(&state) {
                    Some(x) => {
                        *x += dt;
                        false
                    },
                    None => true,
                };
                if new {
                    // do this here because of borrow shenanigans
                    self.state_profile.borrow_mut().insert(state, dt);
                }
                match run {
                    ProcessResult::Continue => continue,
                    ProcessResult::Done => return TokenizerResult::Done,
                    ProcessResult::Script(handle) => return TokenizerResult::Script(handle),
                }
            }
        } else {
            loop {
                match self.step(input) {
                    ProcessResult::Continue => continue,
                    ProcessResult::Done => return TokenizerResult::Done,
                    ProcessResult::Script(handle) => return TokenizerResult::Script(handle),
                }
            }
        }
    }

    //§ tokenization
    // Get the next input character, if one is available.
    fn get_char(&self, input: &BufferQueue) -> Option<char> {
        if self.reconsume.get() {
            self.reconsume.set(false);
            Some(self.current_char.get())
        } else {
            input
                .next()
                .and_then(|c| self.get_preprocessed_char(c, input))
        }
    }

    fn bad_char_error(&self) {
        let msg = if self.opts.exact_errors {
            let c = self.current_char.get();
            let state = self.state.get();
            Cow::from(format!("Saw {c} in state {state:?}"))
        } else {
            Cow::from("Bad character")
        };
        self.emit_error(msg);
    }

    fn discard_tag(&self) {
        *self.current_tag_name.borrow_mut() = StrTendril::new();
        *self.current_tag_attrs.borrow_mut() = Vec::new();
    }

    fn create_tag(&self, kind: TagKind, c: char) {
        self.discard_tag();
        self.current_tag_name.borrow_mut().push_char(c);
        self.current_tag_kind.set(kind);
    }

    // This method creates a PI token and
    // sets its target to given char
    fn create_pi(&self, c: char) {
        *self.current_pi_target.borrow_mut() = StrTendril::new();
        *self.current_pi_data.borrow_mut() = StrTendril::new();
        self.current_pi_target.borrow_mut().push_char(c);
    }

    fn emit_char(&self, c: char) {
        self.process_token(Token::Characters(StrTendril::from_char(match c {
            '\0' => '\u{FFFD}',
            c => c,
        })));
    }

    fn emit_short_tag(&self) -> ProcessResult<Sink::Handle> {
        self.current_tag_kind.set(ShortTag);
        *self.current_tag_name.borrow_mut() = StrTendril::new();
        self.emit_current_tag()
    }

    fn emit_empty_tag(&self) -> ProcessResult<Sink::Handle> {
        self.current_tag_kind.set(EmptyTag);
        self.emit_current_tag()
    }

    fn set_empty_tag(&self) {
        self.current_tag_kind.set(EmptyTag);
    }

    fn emit_start_tag(&self) -> ProcessResult<Sink::Handle> {
        self.current_tag_kind.set(StartTag);
        self.emit_current_tag()
    }

    fn emit_current_tag(&self) -> ProcessResult<Sink::Handle> {
        self.finish_attribute();

        let qname = process_qname(replace(
            &mut *self.current_tag_name.borrow_mut(),
            StrTendril::new(),
        ));

        match self.current_tag_kind.get() {
            StartTag | EmptyTag => {},
            EndTag => {
                if !self.current_tag_attrs.borrow().is_empty() {
                    self.emit_error(Borrowed("Attributes on an end tag"));
                }
            },
            ShortTag => {
                if !self.current_tag_attrs.borrow().is_empty() {
                    self.emit_error(Borrowed("Attributes on a short tag"));
                }
            },
        }

        let token = Token::Tag(Tag {
            kind: self.current_tag_kind.get(),
            name: qname,
            attrs: self.current_tag_attrs.take(),
        });

        self.process_token(token)
    }

    // The string must not contain '\0'!
    fn emit_chars(&self, b: StrTendril) {
        self.process_token(Token::Characters(b));
    }

    // Emits the current Processing Instruction
    fn emit_pi(&self) -> ProcessResult<<Sink as TokenSink>::Handle> {
        let token = Token::ProcessingInstruction(Pi {
            target: replace(&mut *self.current_pi_target.borrow_mut(), StrTendril::new()),
            data: replace(&mut *self.current_pi_data.borrow_mut(), StrTendril::new()),
        });
        self.process_token(token)
    }

    fn consume_char_ref(&self, addnl_allowed: Option<char>) {
        // NB: The char ref tokenizer assumes we have an additional allowed
        // character iff we're tokenizing in an attribute value.
        *self.char_ref_tokenizer.borrow_mut() =
            Some(Box::new(CharRefTokenizer::new(addnl_allowed)));
    }

    fn emit_eof(&self) {
        self.process_token(Token::EndOfFile);
    }

    fn emit_error(&self, error: Cow<'static, str>) {
        self.process_token(Token::ParseError(error));
    }

    fn emit_current_comment(&self) {
        let comment = self.current_comment.take();
        self.process_token(Token::Comment(comment));
    }

    fn emit_current_doctype(&self) {
        let doctype = self.current_doctype.take();
        self.process_token(Token::Doctype(doctype));
    }

    fn doctype_id(&self, kind: DoctypeKind) -> RefMut<'_, Option<StrTendril>> {
        let current_doctype = self.current_doctype.borrow_mut();
        match kind {
            Public => RefMut::map(current_doctype, |d| &mut d.public_id),
            System => RefMut::map(current_doctype, |d| &mut d.system_id),
        }
    }

    fn clear_doctype_id(&self, kind: DoctypeKind) {
        let mut id = self.doctype_id(kind);
        match *id {
            Some(ref mut s) => s.clear(),
            None => *id = Some(StrTendril::new()),
        }
    }

    fn peek(&self, input: &BufferQueue) -> Option<char> {
        if self.reconsume.get() {
            Some(self.current_char.get())
        } else {
            input.peek()
        }
    }

    fn discard_char(&self, input: &BufferQueue) {
        let c = self.get_char(input);
        assert!(c.is_some());
    }

    fn unconsume(&self, input: &BufferQueue, buf: StrTendril) {
        input.push_front(buf);
    }
}

// Shorthand for common state machine behaviors.
macro_rules! shorthand (
    ( $me:ident : emit $c:expr                     ) => ( $me.emit_char($c)                                   );
    ( $me:ident : create_tag $kind:ident $c:expr   ) => ( $me.create_tag($kind, $c)                           );
    ( $me:ident : push_tag $c:expr                 ) => ( $me.current_tag_name.borrow_mut().push_char($c)     );
    ( $me:ident : discard_tag $input:expr          ) => ( $me.discard_tag($input)                             );
    ( $me:ident : discard_char                     ) => ( $me.discard_char()                                  );
    ( $me:ident : push_temp $c:expr                ) => ( $me.temp_buf.borrow_mut().push_char($c)             );
    ( $me:ident : emit_temp                        ) => ( $me.emit_temp_buf()                                 );
    ( $me:ident : clear_temp                       ) => ( $me.clear_temp_buf()                                );
    ( $me:ident : create_attr $c:expr              ) => ( $me.create_attribute($c)                            );
    ( $me:ident : push_name $c:expr                ) => ( $me.current_attr_name.borrow_mut().push_char($c)    );
    ( $me:ident : push_value $c:expr               ) => ( $me.current_attr_value.borrow_mut().push_char($c)   );
    ( $me:ident : append_value $c:expr             ) => ( $me.current_attr_value.borrow_mut().push_tendril($c));
    ( $me:ident : push_comment $c:expr             ) => ( $me.current_comment.borrow_mut().push_char($c)      );
    ( $me:ident : append_comment $c:expr           ) => ( $me.current_comment.borrow_mut().push_slice($c)     );
    ( $me:ident : emit_comment                     ) => ( $me.emit_current_comment()                          );
    ( $me:ident : clear_comment                    ) => ( $me.current_comment.borrow_mut().clear()            );
    ( $me:ident : create_doctype                   ) => ( *$me.current_doctype.borrow_mut() = Doctype::default() );
    ( $me:ident : push_doctype_name $c:expr        ) => ( option_push(&mut $me.current_doctype.borrow_mut().name, $c) );
    ( $me:ident : push_doctype_id $k:ident $c:expr ) => ( option_push(&mut $me.doctype_id($k), $c)            );
    ( $me:ident : clear_doctype_id $k:ident        ) => ( $me.clear_doctype_id($k)                            );
    ( $me:ident : emit_doctype                     ) => ( $me.emit_current_doctype()                          );
    ( $me:ident : error                            ) => ( $me.bad_char_error()                                );
    ( $me:ident : error_eof                        ) => ( $me.bad_eof_error()                                 );
    ( $me:ident : create_pi $c:expr                ) => ( $me.create_pi($c)                                   );
    ( $me:ident : push_pi_target $c:expr           ) => ( $me.current_pi_target.borrow_mut().push_char($c)    );
    ( $me:ident : push_pi_data $c:expr             ) => ( $me.current_pi_data.borrow_mut().push_char($c)      );
    ( $me:ident : set_empty_tag                    ) => ( $me.set_empty_tag()                                 );
);

// Tracing of tokenizer actions.  This adds significant bloat and compile time,
// so it's behind a cfg flag.
#[cfg(feature = "trace_tokenizer")]
macro_rules! sh_trace ( ( $me:ident : $($cmds:tt)* ) => ({
    debug!("  {:?}", stringify!($($cmds)*));
    shorthand!($me : $($cmds)*);
}));

#[cfg(not(feature = "trace_tokenizer"))]
macro_rules! sh_trace ( ( $me:ident : $($cmds:tt)* ) => ( shorthand!($me: $($cmds)*) ) );

// A little DSL for sequencing shorthand actions.
macro_rules! go (
    // A pattern like $($cmd:tt)* ; $($rest:tt)* causes parse ambiguity.
    // We have to tell the parser how much lookahead we need.

    ( $me:ident : $a:tt                   ; $($rest:tt)* ) => ({ sh_trace!($me: $a);          go!($me: $($rest)*); });
    ( $me:ident : $a:tt $b:tt             ; $($rest:tt)* ) => ({ sh_trace!($me: $a $b);       go!($me: $($rest)*); });
    ( $me:ident : $a:tt $b:tt $c:tt       ; $($rest:tt)* ) => ({ sh_trace!($me: $a $b $c);    go!($me: $($rest)*); });
    ( $me:ident : $a:tt $b:tt $c:tt $d:tt ; $($rest:tt)* ) => ({ sh_trace!($me: $a $b $c $d); go!($me: $($rest)*); });

    // These can only come at the end.

    ( $me:ident : to $s:ident                    ) => ({ $me.state.set(states::$s); return ProcessResult::Continue;           });
    ( $me:ident : to $s:ident $k1:expr           ) => ({ $me.state.set(states::$s($k1)); return ProcessResult::Continue;      });
    ( $me:ident : to $s:ident $k1:ident $k2:expr ) => ({ $me.state.set(states::$s($k1($k2))); return ProcessResult::Continue; });

    ( $me:ident : reconsume $s:ident                    ) => ({ $me.reconsume.set(true); go!($me: to $s);         });
    ( $me:ident : reconsume $s:ident $k1:expr           ) => ({ $me.reconsume.set(true); go!($me: to $s $k1);     });
    ( $me:ident : reconsume $s:ident $k1:ident $k2:expr ) => ({ $me.reconsume.set(true); go!($me: to $s $k1 $k2); });

    ( $me:ident : consume_char_ref             ) => ({ $me.consume_char_ref(None); return ProcessResult::Continue;         });
    ( $me:ident : consume_char_ref $addnl:expr ) => ({ $me.consume_char_ref(Some($addnl)); return ProcessResult::Continue; });

    // We have a default next state after emitting a tag, but the sink can override.
    ( $me:ident : emit_tag $s:ident ) => ({
        $me.state.set(states::$s);
        return $me.emit_current_tag();
    });

    // We have a special when dealing with empty and short tags in Xml
    ( $me:ident : emit_short_tag $s:ident ) => ({
        $me.state.set(states::$s);
        return $me.emit_short_tag();
    });

    ( $me:ident : emit_empty_tag $s:ident ) => ({
        $me.state.set(states::$s);
        return $me.emit_empty_tag();
    });

    ( $me:ident : emit_start_tag $s:ident ) => ({
        $me.state.set(states::$s);
        return $me.emit_start_tag();
    });

    ( $me:ident : emit_pi $s:ident ) => ({
        $me.state.set(states::$s);
        return $me.emit_pi();
    });

    ( $me:ident : eof ) => ({ $me.emit_eof(); return ProcessResult::Done; });

    // If nothing else matched, it's a single command
    ( $me:ident : $($cmd:tt)+ ) => ( sh_trace!($me: $($cmd)+) );

    // or nothing.
    ( $me:ident : ) => (());
);

// This is a macro because it can cause early return
// from the function where it is used.
macro_rules! get_char ( ($me:expr, $input:expr) => (
    unwrap_or_return!($me.get_char($input), ProcessResult::Done)
));

macro_rules! pop_except_from ( ($me:expr, $input:expr, $set:expr) => (
    unwrap_or_return!($me.pop_except_from($input, $set), ProcessResult::Done)
));

macro_rules! eat ( ($me:expr, $input:expr, $pat:expr) => (
    unwrap_or_return!($me.eat($input, $pat), ProcessResult::Done)
));

/// The result of a single tokenization operation
pub enum ProcessResult<Handle> {
    /// The tokenizer needs more input before it can continue
    Done,
    /// The tokenizer can be invoked again immediately
    Continue,
    /// The tokenizer encountered a script element that must be executed
    /// before tokenization can continue
    Script(Handle),
}

impl<Sink: TokenSink> XmlTokenizer<Sink> {
    // Run the state machine for a while.
    #[allow(clippy::never_loop)]
    fn step(&self, input: &BufferQueue) -> ProcessResult<Sink::Handle> {
        if self.char_ref_tokenizer.borrow().is_some() {
            return self.step_char_ref_tokenizer(input);
        }

        debug!("processing in state {:?}", self.state);
        match self.state.get() {
            XmlState::Quiescent => {
                self.state.set(XmlState::Data);
                ProcessResult::Done
            },
            //§ data-state
            XmlState::Data => loop {
                match pop_except_from!(self, input, small_char_set!('\r' '&' '<')) {
                    FromSet('&') => go!(self: consume_char_ref),
                    FromSet('<') => go!(self: to TagState),
                    FromSet(c) => go!(self: emit c),
                    NotFromSet(b) => self.emit_chars(b),
                }
            },
            //§ tag-state
            XmlState::TagState => loop {
                match get_char!(self, input) {
                    '!' => go!(self: to MarkupDecl),
                    '/' => go!(self: to EndTagState),
                    '?' => go!(self: to Pi),
                    '\t' | '\n' | ' ' | ':' | '<' | '>' => {
                        go!(self: error; emit '<'; reconsume Data)
                    },
                    cl => go!(self: create_tag StartTag cl; to TagName),
                }
            },
            //§ end-tag-state
            XmlState::EndTagState => loop {
                match get_char!(self, input) {
                    '>' => go!(self:  emit_short_tag Data),
                    '\t' | '\n' | ' ' | '<' | ':' => {
                        go!(self: error; emit '<'; emit '/'; reconsume Data)
                    },
                    cl => go!(self: create_tag EndTag cl; to EndTagName),
                }
            },
            //§ end-tag-name-state
            XmlState::EndTagName => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | ' ' => go!(self: to EndTagNameAfter),
                    '/' => go!(self: error; to EndTagNameAfter),
                    '>' => go!(self: emit_tag Data),
                    cl => go!(self: push_tag cl),
                }
            },
            //§ end-tag-name-after-state
            XmlState::EndTagNameAfter => loop {
                match get_char!(self, input) {
                    '>' => go!(self: emit_tag Data),
                    '\t' | '\n' | ' ' => (),
                    _ => self.emit_error(Borrowed("Unexpected element in tag name")),
                }
            },
            //§ pi-state
            XmlState::Pi => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | ' ' => go!(self: error; reconsume BogusComment),
                    cl => go!(self: create_pi cl; to PiTarget),
                }
            },
            //§ pi-target-state
            XmlState::PiTarget => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | ' ' => go!(self: to PiTargetAfter),
                    '?' => go!(self: to PiAfter),
                    cl => go!(self: push_pi_target cl),
                }
            },
            //§ pi-target-after-state
            XmlState::PiTargetAfter => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | ' ' => (),
                    _ => go!(self: reconsume PiData),
                }
            },
            //§ pi-data-state
            XmlState::PiData => loop {
                match get_char!(self, input) {
                    '?' => go!(self: to PiAfter),
                    cl => go!(self: push_pi_data cl),
                }
            },
            //§ pi-after-state
            XmlState::PiAfter => loop {
                match get_char!(self, input) {
                    '>' => go!(self: emit_pi Data),
                    '?' => go!(self: to PiAfter),
                    cl => go!(self: push_pi_data cl),
                }
            },
            //§ markup-declaration-state
            XmlState::MarkupDecl => loop {
                if eat!(self, input, "--") {
                    go!(self: clear_comment; to CommentStart);
                } else if eat!(self, input, "[CDATA[") {
                    go!(self: to Cdata);
                } else if eat!(self, input, "DOCTYPE") {
                    go!(self: to Doctype);
                } else {
                    // FIXME: 'error' gives wrong message
                    go!(self: error; to BogusComment);
                }
            },
            //§ comment-start-state
            XmlState::CommentStart => loop {
                match get_char!(self, input) {
                    '-' => go!(self: to CommentStartDash),
                    '>' => go!(self: error; emit_comment; to Data),
                    _ => go!(self: reconsume Comment),
                }
            },
            //§ comment-start-dash-state
            XmlState::CommentStartDash => loop {
                match get_char!(self, input) {
                    '-' => go!(self: to CommentEnd),
                    '>' => go!(self: error; emit_comment; to Data),
                    _ => go!(self: push_comment '-'; reconsume Comment),
                }
            },
            //§ comment-state
            XmlState::Comment => loop {
                match get_char!(self, input) {
                    '<' => go!(self: push_comment '<'; to CommentLessThan),
                    '-' => go!(self: to CommentEndDash),
                    c => go!(self: push_comment c),
                }
            },
            //§ comment-less-than-sign-state
            XmlState::CommentLessThan => loop {
                match get_char!(self, input) {
                    '!' => go!(self: push_comment '!';to CommentLessThanBang),
                    '<' => go!(self: push_comment '<'),
                    _ => go!(self: reconsume Comment),
                }
            },
            //§ comment-less-than-sign-bang-state
            XmlState::CommentLessThanBang => loop {
                match get_char!(self, input) {
                    '-' => go!(self: to CommentLessThanBangDash),
                    _ => go!(self: reconsume Comment),
                }
            },
            //§ comment-less-than-sign-bang-dash-state
            XmlState::CommentLessThanBangDash => loop {
                match get_char!(self, input) {
                    '-' => go!(self: to CommentLessThanBangDashDash),
                    _ => go!(self: reconsume CommentEndDash),
                }
            },
            //§ comment-less-than-sign-bang-dash-dash-state
            XmlState::CommentLessThanBangDashDash => loop {
                match get_char!(self, input) {
                    '>' => go!(self: reconsume CommentEnd),
                    _ => go!(self: error; reconsume CommentEnd),
                }
            },
            //§ comment-end-dash-state
            XmlState::CommentEndDash => loop {
                match get_char!(self, input) {
                    '-' => go!(self: to CommentEnd),
                    _ => go!(self: push_comment '-'; reconsume Comment),
                }
            },
            //§ comment-end-state
            XmlState::CommentEnd => loop {
                match get_char!(self, input) {
                    '>' => go!(self: emit_comment; to Data),
                    '!' => go!(self: to CommentEndBang),
                    '-' => go!(self: push_comment '-'),
                    _ => go!(self: append_comment "--"; reconsume Comment),
                }
            },
            //§ comment-end-bang-state
            XmlState::CommentEndBang => loop {
                match get_char!(self, input) {
                    '-' => go!(self: append_comment "--!"; to CommentEndDash),
                    '>' => go!(self: error; emit_comment; to Data),
                    _ => go!(self: append_comment "--!"; reconsume Comment),
                }
            },
            //§ bogus-comment-state
            XmlState::BogusComment => loop {
                match get_char!(self, input) {
                    '>' => go!(self: emit_comment; to Data),
                    c => go!(self: push_comment c),
                }
            },
            //§ cdata-state
            XmlState::Cdata => loop {
                match get_char!(self, input) {
                    ']' => go!(self: to CdataBracket),
                    cl => go!(self: emit cl),
                }
            },
            //§ cdata-bracket-state
            XmlState::CdataBracket => loop {
                match get_char!(self, input) {
                    ']' => go!(self: to CdataEnd),
                    cl => go!(self: emit ']'; emit cl; to Cdata),
                }
            },
            //§ cdata-end-state
            XmlState::CdataEnd => loop {
                match get_char!(self, input) {
                    '>' => go!(self: to Data),
                    ']' => go!(self: emit ']'),
                    cl => go!(self: emit ']'; emit ']'; emit cl; to Cdata),
                }
            },
            //§ tag-name-state
            XmlState::TagName => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | ' ' => go!(self: to TagAttrNameBefore),
                    '>' => go!(self: emit_tag Data),
                    '/' => go!(self: set_empty_tag; to TagEmpty),
                    cl => go!(self: push_tag cl),
                }
            },
            //§ empty-tag-state
            XmlState::TagEmpty => loop {
                match get_char!(self, input) {
                    '>' => go!(self: emit_empty_tag Data),
                    _ => go!(self: reconsume TagAttrValueBefore),
                }
            },
            //§ tag-attribute-name-before-state
            XmlState::TagAttrNameBefore => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | ' ' => (),
                    '>' => go!(self: emit_tag Data),
                    '/' => go!(self: set_empty_tag; to TagEmpty),
                    ':' => go!(self: error),
                    cl => go!(self: create_attr cl; to TagAttrName),
                }
            },
            //§ tag-attribute-name-state
            XmlState::TagAttrName => loop {
                match get_char!(self, input) {
                    '=' => go!(self: to TagAttrValueBefore),
                    '>' => go!(self: emit_tag Data),
                    '\t' | '\n' | ' ' => go!(self: to TagAttrNameAfter),
                    '/' => go!(self: set_empty_tag; to TagEmpty),
                    cl => go!(self: push_name cl),
                }
            },
            //§ tag-attribute-name-after-state
            XmlState::TagAttrNameAfter => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | ' ' => (),
                    '=' => go!(self: to TagAttrValueBefore),
                    '>' => go!(self: emit_tag Data),
                    '/' => go!(self: set_empty_tag; to TagEmpty),
                    cl => go!(self: create_attr cl; to TagAttrName),
                }
            },
            //§ tag-attribute-value-before-state
            XmlState::TagAttrValueBefore => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | ' ' => (),
                    '"' => go!(self: to TagAttrValue DoubleQuoted),
                    '\'' => go!(self: to TagAttrValue SingleQuoted),
                    '&' => go!(self: reconsume TagAttrValue(Unquoted)),
                    '>' => go!(self: emit_tag Data),
                    cl => go!(self: push_value cl; to TagAttrValue(Unquoted)),
                }
            },
            //§ tag-attribute-value-double-quoted-state
            XmlState::TagAttrValue(DoubleQuoted) => loop {
                match pop_except_from!(self, input, small_char_set!('\n' '"' '&')) {
                    FromSet('"') => go!(self: to TagAttrNameBefore),
                    FromSet('&') => go!(self: consume_char_ref '"' ),
                    FromSet(c) => go!(self: push_value c),
                    NotFromSet(ref b) => go!(self: append_value b),
                }
            },
            //§ tag-attribute-value-single-quoted-state
            XmlState::TagAttrValue(SingleQuoted) => loop {
                match pop_except_from!(self, input, small_char_set!('\n' '\'' '&')) {
                    FromSet('\'') => go!(self: to TagAttrNameBefore),
                    FromSet('&') => go!(self: consume_char_ref '\''),
                    FromSet(c) => go!(self: push_value c),
                    NotFromSet(ref b) => go!(self: append_value b),
                }
            },
            //§ tag-attribute-value-double-quoted-state
            XmlState::TagAttrValue(Unquoted) => loop {
                match pop_except_from!(self, input, small_char_set!('\n' '\t' ' ' '&' '>')) {
                    FromSet('\t') | FromSet('\n') | FromSet(' ') => go!(self: to TagAttrNameBefore),
                    FromSet('&') => go!(self: consume_char_ref),
                    FromSet('>') => go!(self: emit_tag Data),
                    FromSet(c) => go!(self: push_value c),
                    NotFromSet(ref b) => go!(self: append_value b),
                }
            },

            //§ doctype-state
            XmlState::Doctype => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | '\x0C' | ' ' => go!(self: to BeforeDoctypeName),
                    _ => go!(self: error; reconsume BeforeDoctypeName),
                }
            },
            //§ before-doctype-name-state
            XmlState::BeforeDoctypeName => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | '\x0C' | ' ' => (),
                    '>' => go!(self: error; emit_doctype; to Data),
                    c => go!(self: create_doctype; push_doctype_name (c.to_ascii_lowercase());
                                  to DoctypeName),
                }
            },
            //§ doctype-name-state
            XmlState::DoctypeName => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | '\x0C' | ' ' => go!(self: to AfterDoctypeName),
                    '>' => go!(self: emit_doctype; to Data),
                    c => go!(self: push_doctype_name (c.to_ascii_lowercase());
                                  to DoctypeName),
                }
            },
            //§ after-doctype-name-state
            XmlState::AfterDoctypeName => loop {
                if eat!(self, input, "public") {
                    go!(self: to AfterDoctypeKeyword Public);
                } else if eat!(self, input, "system") {
                    go!(self: to AfterDoctypeKeyword System);
                } else {
                    match get_char!(self, input) {
                        '\t' | '\n' | '\x0C' | ' ' => (),
                        '>' => go!(self: emit_doctype; to Data),
                        _ => go!(self: error; to BogusDoctype),
                    }
                }
            },
            //§ after-doctype-public-keyword-state
            XmlState::AfterDoctypeKeyword(Public) => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | '\x0C' | ' ' => go!(self: to BeforeDoctypeIdentifier Public),
                    '"' => {
                        go!(self: error; clear_doctype_id Public; to DoctypeIdentifierDoubleQuoted Public)
                    },
                    '\'' => {
                        go!(self: error; clear_doctype_id Public; to DoctypeIdentifierSingleQuoted Public)
                    },
                    '>' => go!(self: error; emit_doctype; to Data),
                    _ => go!(self: error; to BogusDoctype),
                }
            },
            //§ after-doctype-system-keyword-state
            XmlState::AfterDoctypeKeyword(System) => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | '\x0C' | ' ' => go!(self: to BeforeDoctypeIdentifier System),
                    '"' => {
                        go!(self: error; clear_doctype_id System; to DoctypeIdentifierDoubleQuoted System)
                    },
                    '\'' => {
                        go!(self: error; clear_doctype_id System; to DoctypeIdentifierSingleQuoted System)
                    },
                    '>' => go!(self: error; emit_doctype; to Data),
                    _ => go!(self: error; to BogusDoctype),
                }
            },
            //§ before_doctype_public_identifier_state before_doctype_system_identifier_state
            XmlState::BeforeDoctypeIdentifier(kind) => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | '\x0C' | ' ' => (),
                    '"' => go!(self: clear_doctype_id kind; to DoctypeIdentifierDoubleQuoted kind),
                    '\'' => go!(self: clear_doctype_id kind; to DoctypeIdentifierSingleQuoted kind),
                    '>' => go!(self: error; emit_doctype; to Data),
                    _ => go!(self: error; to BogusDoctype),
                }
            },
            //§ doctype_public_identifier_double_quoted_state doctype_system_identifier_double_quoted_state
            XmlState::DoctypeIdentifierDoubleQuoted(kind) => loop {
                match get_char!(self, input) {
                    '"' => go!(self: to AfterDoctypeIdentifier kind),
                    '>' => go!(self: error; emit_doctype; to Data),
                    c => go!(self: push_doctype_id kind c),
                }
            },
            //§ doctype_public_identifier_single_quoted_state doctype_system_identifier_single_quoted_state
            XmlState::DoctypeIdentifierSingleQuoted(kind) => loop {
                match get_char!(self, input) {
                    '\'' => go!(self: to AfterDoctypeIdentifier kind),
                    '>' => go!(self: error; emit_doctype; to Data),
                    c => go!(self: push_doctype_id kind c),
                }
            },
            //§ doctype_public_identifier_single_quoted_state
            XmlState::AfterDoctypeIdentifier(Public) => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | '\x0C' | ' ' => {
                        go!(self: to BetweenDoctypePublicAndSystemIdentifiers)
                    },
                    '\'' => {
                        go!(self: error; clear_doctype_id System; to DoctypeIdentifierSingleQuoted(System))
                    },
                    '"' => {
                        go!(self: error; clear_doctype_id System; to DoctypeIdentifierDoubleQuoted(System))
                    },
                    '>' => go!(self: emit_doctype; to Data),
                    _ => go!(self: error; to BogusDoctype),
                }
            },
            //§ doctype_system_identifier_single_quoted_state
            XmlState::AfterDoctypeIdentifier(System) => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | '\x0C' | ' ' => (),
                    '>' => go!(self: emit_doctype; to Data),
                    _ => go!(self: error; to BogusDoctype),
                }
            },
            //§ between_doctype_public_and_system_identifier_state
            XmlState::BetweenDoctypePublicAndSystemIdentifiers => loop {
                match get_char!(self, input) {
                    '\t' | '\n' | '\x0C' | ' ' => (),
                    '>' => go!(self: emit_doctype; to Data),
                    '\'' => go!(self: to DoctypeIdentifierSingleQuoted System),
                    '"' => go!(self: to DoctypeIdentifierDoubleQuoted System),
                    _ => go!(self: error; to BogusDoctype),
                }
            },
            //§ bogus_doctype_state
            XmlState::BogusDoctype => loop {
                if get_char!(self, input) == '>' {
                    go!(self: emit_doctype; to Data);
                }
            },
        }
    }

    /// Indicate that we have reached the end of the input.
    pub fn end(&self) {
        // Handle EOF in the char ref sub-tokenizer, if there is one.
        // Do this first because it might un-consume stuff.
        let input = BufferQueue::default();
        match self.char_ref_tokenizer.take() {
            None => (),
            Some(mut tok) => {
                tok.end_of_file(self, &input);
                self.process_char_ref(tok.get_result());
            },
        }

        // Process all remaining buffered input.
        // If we're waiting for lookahead, we're not gonna get it.
        self.at_eof.set(true);
        let _ = self.run(&input);

        loop {
            if !matches!(self.eof_step(), ProcessResult::Continue) {
                break;
            }
        }

        self.sink.end();

        if self.opts.profile {
            self.dump_profile();
        }
    }

    #[cfg(for_c)]
    fn dump_profile(&self) {
        unreachable!();
    }

    #[cfg(not(for_c))]
    fn dump_profile(&self) {
        let mut results: Vec<(states::XmlState, u64)> = self
            .state_profile
            .borrow()
            .iter()
            .map(|(s, t)| (*s, *t))
            .collect();
        results.sort_by(|&(_, x), &(_, y)| y.cmp(&x));

        let total: u64 = results
            .iter()
            .map(|&(_, t)| t)
            .fold(0, ::std::ops::Add::add);
        debug!("\nTokenizer profile, in nanoseconds");
        debug!(
            "\n{:12}         total in token sink",
            self.time_in_sink.get()
        );
        debug!("\n{total:12}         total in tokenizer");

        for (k, v) in results.into_iter() {
            let pct = 100.0 * (v as f64) / (total as f64);
            debug!("{v:12}  {pct:4.1}%  {k:?}");
        }
    }

    fn eof_step(&self) -> ProcessResult<Sink::Handle> {
        debug!("processing EOF in state {:?}", self.state.get());
        match self.state.get() {
            XmlState::Data | XmlState::Quiescent => go!(self: eof),
            XmlState::CommentStart | XmlState::CommentLessThan | XmlState::CommentLessThanBang => {
                go!(self: reconsume Comment)
            },
            XmlState::CommentLessThanBangDash => go!(self: reconsume CommentEndDash),
            XmlState::CommentLessThanBangDashDash => go!(self: reconsume CommentEnd),
            XmlState::CommentStartDash
            | XmlState::Comment
            | XmlState::CommentEndDash
            | XmlState::CommentEnd
            | XmlState::CommentEndBang => go!(self: error_eof; emit_comment; eof),
            XmlState::TagState => go!(self: error_eof; emit '<'; to Data),
            XmlState::EndTagState => go!(self: error_eof; emit '<'; emit '/'; to Data),
            XmlState::TagEmpty => go!(self: error_eof; to TagAttrNameBefore),
            XmlState::Cdata | XmlState::CdataBracket | XmlState::CdataEnd => {
                go!(self: error_eof; to Data)
            },
            XmlState::Pi => go!(self: error_eof; to BogusComment),
            XmlState::PiTargetAfter | XmlState::PiAfter => go!(self: reconsume PiData),
            XmlState::MarkupDecl => go!(self: error_eof; to BogusComment),
            XmlState::TagName
            | XmlState::TagAttrNameBefore
            | XmlState::EndTagName
            | XmlState::TagAttrNameAfter
            | XmlState::EndTagNameAfter
            | XmlState::TagAttrValueBefore
            | XmlState::TagAttrValue(_) => go!(self: error_eof; emit_tag Data),
            XmlState::PiData | XmlState::PiTarget => go!(self: error_eof; emit_pi Data),
            XmlState::TagAttrName => go!(self: error_eof; emit_start_tag Data),
            XmlState::BeforeDoctypeName
            | XmlState::Doctype
            | XmlState::DoctypeName
            | XmlState::AfterDoctypeName
            | XmlState::AfterDoctypeKeyword(_)
            | XmlState::BeforeDoctypeIdentifier(_)
            | XmlState::AfterDoctypeIdentifier(_)
            | XmlState::DoctypeIdentifierSingleQuoted(_)
            | XmlState::DoctypeIdentifierDoubleQuoted(_)
            | XmlState::BetweenDoctypePublicAndSystemIdentifiers => {
                go!(self: error_eof; emit_doctype; to Data)
            },
            XmlState::BogusDoctype => go!(self: emit_doctype; to Data),
            XmlState::BogusComment => go!(self: emit_comment; to Data),
        }
    }

    fn process_char_ref(&self, char_ref: CharRef) {
        let CharRef {
            mut chars,
            mut num_chars,
        } = char_ref;

        if num_chars == 0 {
            chars[0] = '&';
            num_chars = 1;
        }

        for i in 0..num_chars {
            let c = chars[i as usize];
            match self.state.get() {
                states::Data | states::Cdata => go!(self: emit c),

                states::TagAttrValue(_) => go!(self: push_value c),

                _ => panic!(
                    "state {:?} should not be reachable in process_char_ref",
                    self.state.get()
                ),
            }
        }
    }

    fn step_char_ref_tokenizer(&self, input: &BufferQueue) -> ProcessResult<Sink::Handle> {
        let mut tok = self.char_ref_tokenizer.take().unwrap();
        let outcome = tok.step(self, input);

        let progress = match outcome {
            char_ref::Done => {
                self.process_char_ref(tok.get_result());
                return ProcessResult::Continue;
            },

            char_ref::Stuck => ProcessResult::Done,
            char_ref::Progress => ProcessResult::Continue,
        };

        *self.char_ref_tokenizer.borrow_mut() = Some(tok);
        progress
    }

    fn finish_attribute(&self) {
        if self.current_attr_name.borrow().is_empty() {
            return;
        }

        // Check for a duplicate attribute.
        // FIXME: the spec says we should error as soon as the name is finished.
        // FIXME: linear time search, do we care?
        let dup = {
            let current_attr_name = self.current_attr_name.borrow();
            let name = &current_attr_name[..];
            self.current_tag_attrs
                .borrow()
                .iter()
                .any(|a| &*a.name.local == name)
        };

        if dup {
            self.emit_error(Borrowed("Duplicate attribute"));
            self.current_attr_name.borrow_mut().clear();
            self.current_attr_value.borrow_mut().clear();
        } else {
            let qname = process_qname(replace(
                &mut self.current_attr_name.borrow_mut(),
                StrTendril::new(),
            ));
            let attr = Attribute {
                name: qname.clone(),
                value: replace(&mut self.current_attr_value.borrow_mut(), StrTendril::new()),
            };

            if qname.local == local_name!("xmlns")
                || qname.prefix == Some(namespace_prefix!("xmlns"))
            {
                self.current_tag_attrs.borrow_mut().insert(0, attr);
            } else {
                self.current_tag_attrs.borrow_mut().push(attr);
            }
        }
    }

    fn create_attribute(&self, c: char) {
        self.finish_attribute();

        self.current_attr_name.borrow_mut().push_char(c);
    }
}

#[cfg(test)]
mod test {

    use super::process_qname;
    use crate::tendril::SliceExt;
    use crate::{LocalName, Prefix};

    #[test]
    fn simple_namespace() {
        let qname = process_qname("prefix:local".to_tendril());
        assert_eq!(qname.prefix, Some(Prefix::from("prefix")));
        assert_eq!(qname.local, LocalName::from("local"));

        let qname = process_qname("a:b".to_tendril());
        assert_eq!(qname.prefix, Some(Prefix::from("a")));
        assert_eq!(qname.local, LocalName::from("b"));
    }

    #[test]
    fn wrong_namespaces() {
        let qname = process_qname(":local".to_tendril());
        assert_eq!(qname.prefix, None);
        assert_eq!(qname.local, LocalName::from(":local"));

        let qname = process_qname("::local".to_tendril());
        assert_eq!(qname.prefix, None);
        assert_eq!(qname.local, LocalName::from("::local"));

        let qname = process_qname("a::local".to_tendril());
        assert_eq!(qname.prefix, None);
        assert_eq!(qname.local, LocalName::from("a::local"));

        let qname = process_qname("fake::".to_tendril());
        assert_eq!(qname.prefix, None);
        assert_eq!(qname.local, LocalName::from("fake::"));

        let qname = process_qname(":::".to_tendril());
        assert_eq!(qname.prefix, None);
        assert_eq!(qname.local, LocalName::from(":::"));

        let qname = process_qname(":a:b:".to_tendril());
        assert_eq!(qname.prefix, None);
        assert_eq!(qname.local, LocalName::from(":a:b:"));
    }
}
