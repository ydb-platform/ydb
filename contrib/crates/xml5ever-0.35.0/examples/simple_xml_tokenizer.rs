#!/usr/bin/env run-cargo-script
//! This is a regular crate doc comment, but it also contains a partial
//! Cargo manifest.  Note the use of a *fenced* code block, and the
//! `cargo` "language".
//!
//! ```cargo
//! [dependencies]
//! xml5ever = "0.1.1"
//! tendril = "0.1.3"
//! markup5ever = "0.7.4"
//! ```
extern crate markup5ever;
extern crate xml5ever;

use std::io;

use markup5ever::buffer_queue::BufferQueue;
use xml5ever::tendril::{ByteTendril, ReadExt};
use xml5ever::tokenizer::{Doctype, Pi, ProcessResult, Token, TokenSink, XmlTokenizer};

struct SimpleTokenPrinter;

impl TokenSink for SimpleTokenPrinter {
    type Handle = ();

    fn process_token(&self, token: Token) -> ProcessResult<()> {
        match token {
            Token::Characters(b) => {
                println!("TEXT: {}", &*b);
            },
            Token::NullCharacter => print!("NULL"),
            Token::Tag(tag) => {
                println!("{:?} {} ", tag.kind, &*tag.name.local);
            },
            Token::ParseError(err) => {
                println!("ERROR: {err}");
            },
            Token::ProcessingInstruction(Pi {
                ref target,
                ref data,
            }) => {
                println!("PI : <?{target} {data}?>");
            },
            Token::Comment(ref comment) => {
                println!("<!--{comment:?}-->");
            },
            Token::EndOfFile => {
                println!("EOF");
            },
            Token::Doctype(Doctype {
                ref name,
                ref public_id,
                ..
            }) => {
                println!("<!DOCTYPE {name:?} {public_id:?}>");
            },
        };
        ProcessResult::Continue
    }
}

fn main() {
    // Our implementation of TokenSink
    let sink = SimpleTokenPrinter;

    // We need a ByteTendril to read a file
    let mut input = ByteTendril::new();

    // Using SliceExt.read_to_tendril we can read stdin
    io::stdin().read_to_tendril(&mut input).unwrap();
    // For xml5ever we need StrTendril, so we reinterpret it
    // into StrTendril.

    // Load input into BufferQueue
    let input_buffer = BufferQueue::default();
    input_buffer.push_back(input.try_reinterpret().unwrap());
    // Here we create and run tokenizer
    let tok = XmlTokenizer::new(sink, Default::default());
    let _ = tok.feed(&input_buffer);
    tok.end();
}
