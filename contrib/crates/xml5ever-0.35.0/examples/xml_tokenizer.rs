#!/usr/bin/env run-cargo-script
//! This is a regular crate doc comment, but it also contains a partial
//! Cargo manifest.  Note the use of a *fenced* code block, and the
//! `cargo` "language".
//!
//! ```cargo
//! [dependencies]
//! xml5ever = "0.2.0"
//! tendril = "0.1.3"
//! markup5ever = "0.7.4"
//! ```
extern crate markup5ever;
extern crate xml5ever;

use std::cell::Cell;
use std::io;

use markup5ever::buffer_queue::BufferQueue;
use xml5ever::tendril::{ByteTendril, ReadExt};
use xml5ever::tokenizer::{
    EmptyTag, EndTag, Pi, ProcessResult, ShortTag, StartTag, Token, TokenSink, XmlTokenizer,
    XmlTokenizerOpts,
};

#[derive(Clone)]
struct TokenPrinter {
    in_char_run: Cell<bool>,
}

impl TokenPrinter {
    fn is_char(&self, is_char: bool) {
        match (self.in_char_run.get(), is_char) {
            (false, true) => print!("CHAR : \""),
            (true, false) => println!("\""),
            _ => (),
        }
        self.in_char_run.set(is_char);
    }

    fn do_char(&self, c: char) {
        self.is_char(true);
        print!("{}", c.escape_default().collect::<String>());
    }
}

impl TokenSink for TokenPrinter {
    type Handle = ();

    fn process_token(&self, token: Token) -> ProcessResult<()> {
        match token {
            Token::Characters(b) => {
                for c in b.chars() {
                    self.do_char(c);
                }
            },
            Token::NullCharacter => self.do_char('\0'),
            Token::Tag(tag) => {
                self.is_char(false);
                // This is not proper HTML serialization, of course.
                match tag.kind {
                    StartTag => print!("TAG  : <\x1b[32m{}\x1b[0m", tag.name.local),
                    EndTag => print!("END TAG  : <\x1b[31m/{}\x1b[0m", tag.name.local),
                    ShortTag => print!("Short TAG  : <\x1b[31m/{}\x1b[0m", tag.name.local),
                    EmptyTag => print!("Empty TAG  : <\x1b[31m{}\x1b[0m", tag.name.local),
                }
                for attr in tag.attrs.iter() {
                    print!(
                        " \x1b[36m{}\x1b[0m='\x1b[34m{}\x1b[0m'",
                        attr.name.local, attr.value
                    );
                }
                if tag.kind == EmptyTag {
                    print!("/");
                }
                println!(">");
            },
            Token::ParseError(err) => {
                self.is_char(false);
                println!("ERROR: {err}");
            },
            Token::ProcessingInstruction(Pi { target, data }) => {
                self.is_char(false);
                println!("PI : <?{target:?} {data:?}?>");
            },
            _ => {
                self.is_char(false);
                println!("OTHER: {token:?}");
            },
        };

        ProcessResult::Continue
    }
}

fn main() {
    let sink = TokenPrinter {
        in_char_run: Cell::new(false),
    };
    let mut input = ByteTendril::new();
    io::stdin().read_to_tendril(&mut input).unwrap();
    let input_buffer = BufferQueue::default();
    input_buffer.push_back(input.try_reinterpret().unwrap());

    let tok = XmlTokenizer::new(
        sink,
        XmlTokenizerOpts {
            profile: true,
            exact_errors: true,
            ..Default::default()
        },
    );
    let _ = tok.feed(&input_buffer);
    tok.end();
    tok.sink.is_char(false);
}
