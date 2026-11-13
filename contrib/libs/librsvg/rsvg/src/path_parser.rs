//! Parser for SVG path data.

use std::fmt;
use std::iter::Enumerate;
use std::str;
use std::str::Bytes;

use crate::path_builder::*;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Token {
    // pub to allow benchmarking
    Number(f64),
    Flag(bool),
    Command(u8),
    Comma,
}

use crate::path_parser::Token::{Comma, Command, Flag, Number};

#[derive(Debug)]
pub struct Lexer<'a> {
    // pub to allow benchmarking
    input: &'a [u8],
    ci: Enumerate<Bytes<'a>>,
    current: Option<(usize, u8)>,
    flags_required: u8,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum LexError {
    // pub to allow benchmarking
    ParseFloatError,
    UnexpectedByte(u8),
    UnexpectedEof,
}

impl<'a> Lexer<'_> {
    pub fn new(input: &'a str) -> Lexer<'a> {
        let mut ci = input.bytes().enumerate();
        let current = ci.next();
        Lexer {
            input: input.as_bytes(),
            ci,
            current,
            flags_required: 0,
        }
    }

    // The way Flag tokens work is a little annoying. We don't have
    // any way to distinguish between numbers and flags without context
    // from the parser. The only time we need to return flags is within the
    // argument sequence of an elliptical arc, and then we need 2 in a row
    // or it's an error. So, when the parser gets to that point, it calls
    // this method and we switch from our usual mode of handling digits as
    // numbers to looking for two 'flag' characters (either 0 or 1) in a row
    // (with optional intervening whitespace, and possibly comma tokens.)
    // Every time we find a flag we decrement flags_required.
    pub fn require_flags(&mut self) {
        self.flags_required = 2;
    }

    fn current_pos(&mut self) -> usize {
        match self.current {
            None => self.input.len(),
            Some((pos, _)) => pos,
        }
    }

    fn advance(&mut self) {
        self.current = self.ci.next();
    }

    fn advance_over_whitespace(&mut self) -> bool {
        let mut found_some = false;
        while self.current.is_some() && self.current.unwrap().1.is_ascii_whitespace() {
            found_some = true;
            self.current = self.ci.next();
        }
        found_some
    }

    fn advance_over_optional(&mut self, needle: u8) -> bool {
        match self.current {
            Some((_, c)) if c == needle => {
                self.advance();
                true
            }
            _ => false,
        }
    }

    fn advance_over_digits(&mut self) -> bool {
        let mut found_some = false;
        while self.current.is_some() && self.current.unwrap().1.is_ascii_digit() {
            found_some = true;
            self.current = self.ci.next();
        }
        found_some
    }

    fn advance_over_simple_number(&mut self) -> bool {
        let _ = self.advance_over_optional(b'-') || self.advance_over_optional(b'+');
        let found_digit = self.advance_over_digits();
        let _ = self.advance_over_optional(b'.');
        self.advance_over_digits() || found_digit
    }

    fn match_number(&mut self) -> Result<Token, LexError> {
        // remember the beginning
        let (start_pos, _) = self.current.unwrap();
        if !self.advance_over_simple_number() && start_pos != self.current_pos() {
            match self.current {
                None => return Err(LexError::UnexpectedEof),
                Some((_pos, c)) => return Err(LexError::UnexpectedByte(c)),
            }
        }
        if self.advance_over_optional(b'e') || self.advance_over_optional(b'E') {
            let _ = self.advance_over_optional(b'-') || self.advance_over_optional(b'+');
            let _ = self.advance_over_digits();
        }
        let end_pos = match self.current {
            None => self.input.len(),
            Some((i, _)) => i,
        };

        // If you need path parsing to be faster, you can do from_utf8_unchecked to
        // avoid re-validating all the chars, and std::str::parse<i*> calls are
        // faster than std::str::parse<f64> for numbers that are not floats.

        // bare unwrap here should be safe since we've already checked all the bytes
        // in the range
        match std::str::from_utf8(&self.input[start_pos..end_pos])
            .unwrap()
            .parse::<f64>()
        {
            Ok(n) => Ok(Number(n)),
            Err(_e) => Err(LexError::ParseFloatError),
        }
    }
}

impl Iterator for Lexer<'_> {
    type Item = (usize, Result<Token, LexError>);

    fn next(&mut self) -> Option<Self::Item> {
        // eat whitespace
        self.advance_over_whitespace();

        match self.current {
            // commas are separators
            Some((pos, b',')) => {
                self.advance();
                Some((pos, Ok(Comma)))
            }

            // alphabetic chars are commands
            Some((pos, c)) if c.is_ascii_alphabetic() => {
                let token = Command(c);
                self.advance();
                Some((pos, Ok(token)))
            }

            Some((pos, c)) if self.flags_required > 0 && c.is_ascii_digit() => match c {
                b'0' => {
                    self.flags_required -= 1;
                    self.advance();
                    Some((pos, Ok(Flag(false))))
                }
                b'1' => {
                    self.flags_required -= 1;
                    self.advance();
                    Some((pos, Ok(Flag(true))))
                }
                _ => Some((pos, Err(LexError::UnexpectedByte(c)))),
            },

            Some((pos, c)) if c.is_ascii_digit() || c == b'-' || c == b'+' || c == b'.' => {
                Some((pos, self.match_number()))
            }

            Some((pos, c)) => {
                self.advance();
                Some((pos, Err(LexError::UnexpectedByte(c))))
            }

            None => None,
        }
    }
}

pub struct PathParser<'b> {
    tokens: Lexer<'b>,
    current_pos_and_token: Option<(usize, Result<Token, LexError>)>,

    builder: &'b mut PathBuilder,

    // Current point; adjusted at every command
    current_x: f64,
    current_y: f64,

    // Last control point from previous cubic curve command, used to reflect
    // the new control point for smooth cubic curve commands.
    cubic_reflection_x: f64,
    cubic_reflection_y: f64,

    // Last control point from previous quadratic curve command, used to reflect
    // the new control point for smooth quadratic curve commands.
    quadratic_reflection_x: f64,
    quadratic_reflection_y: f64,

    // Start point of current subpath (i.e. position of last moveto);
    // used for closepath.
    subpath_start_x: f64,
    subpath_start_y: f64,
}

// This is a recursive descent parser for path data in SVG files,
// as specified in https://www.w3.org/TR/SVG/paths.html#PathDataBNF
// Some peculiarities:
//
// - SVG allows optional commas inside coordinate pairs, and between
// coordinate pairs.  So, for example, these are equivalent:
//
//     M 10 20 30 40
//     M 10, 20 30, 40
//     M 10, 20, 30, 40
//
// - Whitespace is optional.  These are equivalent:
//
//     M10,20 30,40
//     M10,20,30,40
//
//   These are also equivalent:
//
//     M-10,20-30-40
//     M -10 20 -30 -40
//
//     M.1-2,3E2-4
//     M 0.1 -2 300 -4
impl<'b> PathParser<'b> {
    pub fn new(builder: &'b mut PathBuilder, path_str: &'b str) -> PathParser<'b> {
        let mut lexer = Lexer::new(path_str);
        let pt = lexer.next();
        PathParser {
            tokens: lexer,
            current_pos_and_token: pt,

            builder,

            current_x: 0.0,
            current_y: 0.0,

            cubic_reflection_x: 0.0,
            cubic_reflection_y: 0.0,

            quadratic_reflection_x: 0.0,
            quadratic_reflection_y: 0.0,

            subpath_start_x: 0.0,
            subpath_start_y: 0.0,
        }
    }

    // Our match_* methods all either consume the token we requested
    // and return the unwrapped value, or return an error without
    // advancing the token stream.
    //
    // You can safely use them to probe for a particular kind of token,
    // fail to match it, and try some other type.

    fn match_command(&mut self) -> Result<u8, ParseError> {
        let result = match &self.current_pos_and_token {
            Some((_, Ok(Command(c)))) => Ok(*c),
            Some((pos, Ok(t))) => Err(ParseError::new(*pos, UnexpectedToken(*t))),
            Some((pos, Err(e))) => Err(ParseError::new(*pos, LexError(*e))),
            None => Err(ParseError::new(self.tokens.input.len(), UnexpectedEof)),
        };
        if result.is_ok() {
            self.current_pos_and_token = self.tokens.next();
        }
        result
    }

    fn match_number(&mut self) -> Result<f64, ParseError> {
        let result = match &self.current_pos_and_token {
            Some((_, Ok(Number(n)))) => Ok(*n),
            Some((pos, Ok(t))) => Err(ParseError::new(*pos, UnexpectedToken(*t))),
            Some((pos, Err(e))) => Err(ParseError::new(*pos, LexError(*e))),
            None => Err(ParseError::new(self.tokens.input.len(), UnexpectedEof)),
        };
        if result.is_ok() {
            self.current_pos_and_token = self.tokens.next();
        }
        result
    }

    fn match_number_and_flags(&mut self) -> Result<(f64, bool, bool), ParseError> {
        // We can't just do self.match_number() here, because we have to
        // tell the lexer, if we do find a number, to switch to looking for flags
        // before we advance it to the next token. Otherwise it will treat the flag
        // characters as numbers.
        //
        // So, first we do the guts of match_number...
        let n = match &self.current_pos_and_token {
            Some((_, Ok(Number(n)))) => Ok(*n),
            Some((pos, Ok(t))) => Err(ParseError::new(*pos, UnexpectedToken(*t))),
            Some((pos, Err(e))) => Err(ParseError::new(*pos, LexError(*e))),
            None => Err(ParseError::new(self.tokens.input.len(), UnexpectedEof)),
        }?;

        // Then we tell the lexer that we're going to need to find Flag tokens,
        // *then* we can advance the token stream.
        self.tokens.require_flags();
        self.current_pos_and_token = self.tokens.next();

        self.eat_optional_comma();
        let f1 = self.match_flag()?;

        self.eat_optional_comma();
        let f2 = self.match_flag()?;

        Ok((n, f1, f2))
    }

    fn match_comma(&mut self) -> Result<(), ParseError> {
        let result = match &self.current_pos_and_token {
            Some((_, Ok(Comma))) => Ok(()),
            Some((pos, Ok(t))) => Err(ParseError::new(*pos, UnexpectedToken(*t))),
            Some((pos, Err(e))) => Err(ParseError::new(*pos, LexError(*e))),
            None => Err(ParseError::new(self.tokens.input.len(), UnexpectedEof)),
        };
        if result.is_ok() {
            self.current_pos_and_token = self.tokens.next();
        }
        result
    }

    fn eat_optional_comma(&mut self) {
        let _ = self.match_comma();
    }

    // Convenience function; like match_number, but eats a leading comma if present.
    fn match_comma_number(&mut self) -> Result<f64, ParseError> {
        self.eat_optional_comma();
        self.match_number()
    }

    fn match_flag(&mut self) -> Result<bool, ParseError> {
        let result = match self.current_pos_and_token {
            Some((_, Ok(Flag(f)))) => Ok(f),
            Some((pos, Ok(t))) => Err(ParseError::new(pos, UnexpectedToken(t))),
            Some((pos, Err(e))) => Err(ParseError::new(pos, LexError(e))),
            None => Err(ParseError::new(self.tokens.input.len(), UnexpectedEof)),
        };
        if result.is_ok() {
            self.current_pos_and_token = self.tokens.next();
        }
        result
    }

    // peek_* methods are the twins of match_*, but don't consume the token, and so
    // can't return ParseError

    fn peek_command(&mut self) -> Option<u8> {
        match &self.current_pos_and_token {
            Some((_, Ok(Command(c)))) => Some(*c),
            _ => None,
        }
    }

    fn peek_number(&mut self) -> Option<f64> {
        match &self.current_pos_and_token {
            Some((_, Ok(Number(n)))) => Some(*n),
            _ => None,
        }
    }

    // This is the entry point for parsing a given blob of path data.
    // All the parsing just uses various match_* methods to consume tokens
    // and retrieve the values.
    pub fn parse(&mut self) -> Result<(), ParseError> {
        if self.current_pos_and_token.is_none() {
            return Ok(());
        }

        self.moveto_drawto_command_groups()
    }

    fn error(&self, kind: ErrorKind) -> ParseError {
        match self.current_pos_and_token {
            Some((pos, _)) => ParseError {
                position: pos,
                kind,
            },
            None => ParseError { position: 0, kind }, // FIXME: ???
        }
    }

    fn coordinate_pair(&mut self) -> Result<(f64, f64), ParseError> {
        Ok((self.match_number()?, self.match_comma_number()?))
    }

    fn set_current_point(&mut self, x: f64, y: f64) {
        self.current_x = x;
        self.current_y = y;

        self.cubic_reflection_x = self.current_x;
        self.cubic_reflection_y = self.current_y;

        self.quadratic_reflection_x = self.current_x;
        self.quadratic_reflection_y = self.current_y;
    }

    fn set_cubic_reflection_and_current_point(&mut self, x3: f64, y3: f64, x4: f64, y4: f64) {
        self.cubic_reflection_x = x3;
        self.cubic_reflection_y = y3;

        self.current_x = x4;
        self.current_y = y4;

        self.quadratic_reflection_x = self.current_x;
        self.quadratic_reflection_y = self.current_y;
    }

    fn set_quadratic_reflection_and_current_point(&mut self, a: f64, b: f64, c: f64, d: f64) {
        self.quadratic_reflection_x = a;
        self.quadratic_reflection_y = b;

        self.current_x = c;
        self.current_y = d;

        self.cubic_reflection_x = self.current_x;
        self.cubic_reflection_y = self.current_y;
    }

    fn emit_move_to(&mut self, x: f64, y: f64) {
        self.set_current_point(x, y);

        self.subpath_start_x = self.current_x;
        self.subpath_start_y = self.current_y;

        self.builder.move_to(self.current_x, self.current_y);
    }

    fn emit_line_to(&mut self, x: f64, y: f64) {
        self.set_current_point(x, y);

        self.builder.line_to(self.current_x, self.current_y);
    }

    fn emit_curve_to(&mut self, x2: f64, y2: f64, x3: f64, y3: f64, x4: f64, y4: f64) {
        self.set_cubic_reflection_and_current_point(x3, y3, x4, y4);

        self.builder.curve_to(x2, y2, x3, y3, x4, y4);
    }

    fn emit_quadratic_curve_to(&mut self, a: f64, b: f64, c: f64, d: f64) {
        // raise quadratic Bézier to cubic
        let x2 = (self.current_x + 2.0 * a) / 3.0;
        let y2 = (self.current_y + 2.0 * b) / 3.0;
        let x4 = c;
        let y4 = d;
        let x3 = (x4 + 2.0 * a) / 3.0;
        let y3 = (y4 + 2.0 * b) / 3.0;

        self.set_quadratic_reflection_and_current_point(a, b, c, d);

        self.builder.curve_to(x2, y2, x3, y3, x4, y4);
    }

    fn emit_arc(
        &mut self,
        rx: f64,
        ry: f64,
        x_axis_rotation: f64,
        large_arc: LargeArc,
        sweep: Sweep,
        x: f64,
        y: f64,
    ) {
        let (start_x, start_y) = (self.current_x, self.current_y);

        self.set_current_point(x, y);

        self.builder.arc(
            start_x,
            start_y,
            rx,
            ry,
            x_axis_rotation,
            large_arc,
            sweep,
            self.current_x,
            self.current_y,
        );
    }

    fn moveto_argument_sequence(&mut self, absolute: bool) -> Result<(), ParseError> {
        let (mut x, mut y) = self.coordinate_pair()?;

        if !absolute {
            x += self.current_x;
            y += self.current_y;
        }

        self.emit_move_to(x, y);

        if self.match_comma().is_ok() || self.peek_number().is_some() {
            self.lineto_argument_sequence(absolute)
        } else {
            Ok(())
        }
    }

    fn moveto(&mut self) -> Result<(), ParseError> {
        match self.match_command()? {
            b'M' => self.moveto_argument_sequence(true),
            b'm' => self.moveto_argument_sequence(false),
            c => Err(self.error(ErrorKind::UnexpectedCommand(c))),
        }
    }

    fn moveto_drawto_command_group(&mut self) -> Result<(), ParseError> {
        self.moveto()?;
        self.optional_drawto_commands().map(|_| ())
    }

    fn moveto_drawto_command_groups(&mut self) -> Result<(), ParseError> {
        loop {
            self.moveto_drawto_command_group()?;

            if self.current_pos_and_token.is_none() {
                break;
            }
        }

        Ok(())
    }

    fn optional_drawto_commands(&mut self) -> Result<bool, ParseError> {
        while self.drawto_command()? {
            // everything happens in the drawto_command() calls.
        }

        Ok(false)
    }

    // FIXME: This should not just fail to match 'M' and 'm', but make sure the
    // command is in the set of drawto command characters.
    fn match_if_drawto_command_with_absolute(&mut self) -> Option<(u8, bool)> {
        let cmd = self.peek_command();
        let result = match cmd {
            Some(b'M') => None,
            Some(b'm') => None,
            Some(c) => {
                let c_up = c.to_ascii_uppercase();
                if c == c_up {
                    Some((c_up, true))
                } else {
                    Some((c_up, false))
                }
            }
            _ => None,
        };
        if result.is_some() {
            let _ = self.match_command();
        }
        result
    }

    fn drawto_command(&mut self) -> Result<bool, ParseError> {
        match self.match_if_drawto_command_with_absolute() {
            Some((b'Z', _)) => {
                self.emit_close_path();
                Ok(true)
            }
            Some((b'L', abs)) => {
                self.lineto_argument_sequence(abs)?;
                Ok(true)
            }
            Some((b'H', abs)) => {
                self.horizontal_lineto_argument_sequence(abs)?;
                Ok(true)
            }
            Some((b'V', abs)) => {
                self.vertical_lineto_argument_sequence(abs)?;
                Ok(true)
            }
            Some((b'C', abs)) => {
                self.curveto_argument_sequence(abs)?;
                Ok(true)
            }
            Some((b'S', abs)) => {
                self.smooth_curveto_argument_sequence(abs)?;
                Ok(true)
            }
            Some((b'Q', abs)) => {
                self.quadratic_curveto_argument_sequence(abs)?;
                Ok(true)
            }
            Some((b'T', abs)) => {
                self.smooth_quadratic_curveto_argument_sequence(abs)?;
                Ok(true)
            }
            Some((b'A', abs)) => {
                self.elliptical_arc_argument_sequence(abs)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn emit_close_path(&mut self) {
        let (x, y) = (self.subpath_start_x, self.subpath_start_y);
        self.set_current_point(x, y);

        self.builder.close_path();
    }

    fn should_break_arg_sequence(&mut self) -> bool {
        if self.match_comma().is_ok() {
            // if there is a comma (indicating we should continue to loop), eat the comma
            // so we're ready at the next start of the loop to process the next token.
            false
        } else {
            // continue to process args in the sequence unless the next token is a comma
            self.peek_number().is_none()
        }
    }

    fn lineto_argument_sequence(&mut self, absolute: bool) -> Result<(), ParseError> {
        loop {
            let (mut x, mut y) = self.coordinate_pair()?;

            if !absolute {
                x += self.current_x;
                y += self.current_y;
            }

            self.emit_line_to(x, y);

            if self.should_break_arg_sequence() {
                break;
            }
        }

        Ok(())
    }

    fn horizontal_lineto_argument_sequence(&mut self, absolute: bool) -> Result<(), ParseError> {
        loop {
            let mut x = self.match_number()?;

            if !absolute {
                x += self.current_x;
            }

            let y = self.current_y;

            self.emit_line_to(x, y);

            if self.should_break_arg_sequence() {
                break;
            }
        }

        Ok(())
    }

    fn vertical_lineto_argument_sequence(&mut self, absolute: bool) -> Result<(), ParseError> {
        loop {
            let mut y = self.match_number()?;

            if !absolute {
                y += self.current_y;
            }

            let x = self.current_x;

            self.emit_line_to(x, y);

            if self.should_break_arg_sequence() {
                break;
            }
        }

        Ok(())
    }

    fn curveto_argument_sequence(&mut self, absolute: bool) -> Result<(), ParseError> {
        loop {
            let (mut x2, mut y2) = self.coordinate_pair()?;

            self.eat_optional_comma();
            let (mut x3, mut y3) = self.coordinate_pair()?;

            self.eat_optional_comma();
            let (mut x4, mut y4) = self.coordinate_pair()?;

            if !absolute {
                x2 += self.current_x;
                y2 += self.current_y;
                x3 += self.current_x;
                y3 += self.current_y;
                x4 += self.current_x;
                y4 += self.current_y;
            }

            self.emit_curve_to(x2, y2, x3, y3, x4, y4);

            if self.should_break_arg_sequence() {
                break;
            }
        }

        Ok(())
    }

    fn smooth_curveto_argument_sequence(&mut self, absolute: bool) -> Result<(), ParseError> {
        loop {
            let (mut x3, mut y3) = self.coordinate_pair()?;
            self.eat_optional_comma();
            let (mut x4, mut y4) = self.coordinate_pair()?;

            if !absolute {
                x3 += self.current_x;
                y3 += self.current_y;
                x4 += self.current_x;
                y4 += self.current_y;
            }

            let (x2, y2) = (
                self.current_x + self.current_x - self.cubic_reflection_x,
                self.current_y + self.current_y - self.cubic_reflection_y,
            );

            self.emit_curve_to(x2, y2, x3, y3, x4, y4);

            if self.should_break_arg_sequence() {
                break;
            }
        }

        Ok(())
    }

    fn quadratic_curveto_argument_sequence(&mut self, absolute: bool) -> Result<(), ParseError> {
        loop {
            let (mut a, mut b) = self.coordinate_pair()?;
            self.eat_optional_comma();
            let (mut c, mut d) = self.coordinate_pair()?;

            if !absolute {
                a += self.current_x;
                b += self.current_y;
                c += self.current_x;
                d += self.current_y;
            }

            self.emit_quadratic_curve_to(a, b, c, d);

            if self.should_break_arg_sequence() {
                break;
            }
        }

        Ok(())
    }

    fn smooth_quadratic_curveto_argument_sequence(
        &mut self,
        absolute: bool,
    ) -> Result<(), ParseError> {
        loop {
            let (mut c, mut d) = self.coordinate_pair()?;

            if !absolute {
                c += self.current_x;
                d += self.current_y;
            }

            let (a, b) = (
                self.current_x + self.current_x - self.quadratic_reflection_x,
                self.current_y + self.current_y - self.quadratic_reflection_y,
            );

            self.emit_quadratic_curve_to(a, b, c, d);

            if self.should_break_arg_sequence() {
                break;
            }
        }

        Ok(())
    }

    fn elliptical_arc_argument_sequence(&mut self, absolute: bool) -> Result<(), ParseError> {
        loop {
            let rx = self.match_number()?.abs();
            let ry = self.match_comma_number()?.abs();

            self.eat_optional_comma();
            let (x_axis_rotation, f1, f2) = self.match_number_and_flags()?;

            let large_arc = LargeArc(f1);

            let sweep = if f2 { Sweep::Positive } else { Sweep::Negative };

            self.eat_optional_comma();

            let (mut x, mut y) = self.coordinate_pair()?;

            if !absolute {
                x += self.current_x;
                y += self.current_y;
            }

            self.emit_arc(rx, ry, x_axis_rotation, large_arc, sweep, x, y);

            if self.should_break_arg_sequence() {
                break;
            }
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    UnexpectedToken(Token),
    UnexpectedCommand(u8),
    UnexpectedEof,
    LexError(LexError),
}

#[derive(Debug, PartialEq)]
pub struct ParseError {
    pub position: usize,
    pub kind: ErrorKind,
}

impl ParseError {
    fn new(pos: usize, k: ErrorKind) -> ParseError {
        ParseError {
            position: pos,
            kind: k,
        }
    }
}

use crate::path_parser::ErrorKind::*;

impl fmt::Display for ParseError {
    // Skipped for mutation testing; this is just an error formatter.  These errors are just
    // surfaced as log messages.
    #[mutants::skip]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = match self.kind {
            UnexpectedToken(_t) => "unexpected token",
            UnexpectedCommand(_c) => "unexpected command",
            UnexpectedEof => "unexpected end of data",
            LexError(_le) => "error processing token",
        };
        write!(f, "error at position {}: {}", self.position, description)
    }
}

#[cfg(test)]
#[rustfmt::skip]
mod tests {
    use super::*;

    fn find_error_pos(s: &str) -> Option<usize> {
        s.find('^')
    }

    fn make_parse_result(
        error_pos_str: &str,
        error_kind: Option<ErrorKind>,
    ) -> Result<(), ParseError> {
        if let Some(pos) = find_error_pos(error_pos_str) {
            Err(ParseError {
                position: pos,
                kind: error_kind.unwrap(),
            })
        } else {
            assert!(error_kind.is_none());
            Ok(())
        }
    }

    fn test_parser(
        path_str: &str,
        error_pos_str: &str,
        expected_commands: &[PathCommand],
        expected_error_kind: Option<ErrorKind>,
    ) {
        let expected_result = make_parse_result(error_pos_str, expected_error_kind);

        let mut builder = PathBuilder::default();
        let result = builder.parse(path_str);

        let path = builder.into_path();
        let commands = path.iter().collect::<Vec<_>>();

        assert_eq!(expected_commands, commands.as_slice());
        assert_eq!(expected_result, result);
    }

    fn moveto(x: f64, y: f64) -> PathCommand {
        PathCommand::MoveTo(x, y)
    }

    fn lineto(x: f64, y: f64) -> PathCommand {
        PathCommand::LineTo(x, y)
    }

    fn curveto(x2: f64, y2: f64, x3: f64, y3: f64, x4: f64, y4: f64) -> PathCommand {
        PathCommand::CurveTo(CubicBezierCurve {
            pt1: (x2, y2),
            pt2: (x3, y3),
            to: (x4, y4),
        })
    }

    fn arc(x2: f64, y2: f64, xr: f64, large_arc: bool, sweep: bool,
           x3: f64, y3: f64, x4: f64, y4: f64) -> PathCommand {
        PathCommand::Arc(EllipticalArc {
            r: (x2, y2),
            x_axis_rotation: xr,
            large_arc: LargeArc(large_arc),
            sweep: match sweep {
                true => Sweep::Positive,
                false => Sweep::Negative,
            },
            from: (x3, y3),
            to: (x4, y4),
        })
    }

    fn closepath() -> PathCommand {
        PathCommand::ClosePath
    }

    #[test]
    fn handles_empty_data() {
        test_parser(
            "",
            "",
            &Vec::<PathCommand>::new(),
            None,
        );
    }

    #[test]
    fn handles_numbers() {
        test_parser(
            "M 10 20",
            "",
            &[moveto(10.0, 20.0)],
            None,
        );

        test_parser(
            "M -10 -20",
            "",
            &[moveto(-10.0, -20.0)],
            None,
        );

        test_parser(
            "M .10 0.20",
            "",
            &[moveto(0.10, 0.20)],
            None,
        );

        test_parser(
            "M -.10 -0.20",
            "",
            &[moveto(-0.10, -0.20)],
            None,
        );

        test_parser(
            "M-.10-0.20",
            "",
            &[moveto(-0.10, -0.20)],
            None,
        );

        test_parser(
            "M10.5.50",
            "",
            &[moveto(10.5, 0.50)],
            None,
        );

        test_parser(
            "M.10.20",
            "",
            &[moveto(0.10, 0.20)],
            None,
        );

        test_parser(
            "M .10E1 .20e-4",
            "",
            &[moveto(1.0, 0.000020)],
            None,
        );

        test_parser(
            "M-.10E1-.20",
            "",
            &[moveto(-1.0, -0.20)],
            None,
        );

        test_parser(
            "M10.10E2 -0.20e3",
            "",
            &[moveto(1010.0, -200.0)],
            None,
        );

        test_parser(
            "M-10.10E2-0.20e-3",
            "",
            &[moveto(-1010.0, -0.00020)],
            None,
        );

        test_parser(
            "M1e2.5", // a decimal after exponent start the next number
            "",
            &[moveto(100.0, 0.5)],
            None,
        );

        test_parser(
            "M1e-2.5", // but we are allowed a sign after exponent
            "",
            &[moveto(0.01, 0.5)],
            None,
        );

        test_parser(
            "M1e+2.5", // but we are allowed a sign after exponent
            "",
            &[moveto(100.0, 0.5)],
            None,
        );
    }

    #[test]
    fn detects_bogus_numbers() {
        test_parser(
            "M+",
            " ^",
            &[],
            Some(ErrorKind::LexError(LexError::UnexpectedEof)),
        );

        test_parser(
            "M-",
            " ^",
            &[],
            Some(ErrorKind::LexError(LexError::UnexpectedEof)),
        );

        test_parser(
            "M+x",
            " ^",
            &[],
            Some(ErrorKind::LexError(LexError::UnexpectedByte(b'x'))),
        );

        test_parser(
            "M10e",
            " ^",
            &[],
            Some(ErrorKind::LexError(LexError::ParseFloatError)),
        );

        test_parser(
            "M10ex",
            " ^",
            &[],
            Some(ErrorKind::LexError(LexError::ParseFloatError)),
        );

        test_parser(
            "M10e-",
            " ^",
            &[],
            Some(ErrorKind::LexError(LexError::ParseFloatError)),
        );

        test_parser(
            "M10e+x",
            " ^",
            &[],
            Some(ErrorKind::LexError(LexError::ParseFloatError)),
        );

        test_parser(
            "M 10 ;",
            "     ^",
            &[],
            Some(ErrorKind::LexError(LexError::UnexpectedByte(b';'))),
        );
    }

    #[test]
    fn handles_numbers_with_comma() {
        test_parser(
            "M 10, 20",
            "",
            &[moveto(10.0, 20.0)],
            None,
        );

        test_parser(
            "M -10,-20",
            "",
            &[moveto(-10.0, -20.0)],
            None,
        );

        test_parser(
            "M.10    ,    0.20",
            "",
            &[moveto(0.10, 0.20)],
            None,
        );

        test_parser(
            "M -.10, -0.20   ",
            "",
            &[moveto(-0.10, -0.20)],
            None,
        );

        test_parser(
            "M-.10-0.20",
            "",
            &[moveto(-0.10, -0.20)],
            None,
        );

        test_parser(
            "M.10.20",
            "",
            &[moveto(0.10, 0.20)],
            None,
        );

        test_parser(
            "M .10E1,.20e-4",
            "",
            &[moveto(1.0, 0.000020)],
            None,
        );

        test_parser(
            "M-.10E-2,-.20",
            "",
            &[moveto(-0.0010, -0.20)],
            None,
        );

        test_parser(
            "M10.10E2,-0.20e3",
            "",
            &[moveto(1010.0, -200.0)],
            None,
        );

        test_parser(
            "M-10.10E2,-0.20e-3",
            "",
            &[moveto(-1010.0, -0.00020)],
            None,
        );
    }

    #[test]
    fn handles_single_moveto() {
        test_parser(
            "M 10 20 ",
            "",
            &[moveto(10.0, 20.0)],
            None,
        );

        test_parser(
            "M10,20  ",
            "",
            &[moveto(10.0, 20.0)],
            None,
        );

        test_parser(
            "M10 20   ",
            "",
            &[moveto(10.0, 20.0)],
            None,
        );

        test_parser(
            "    M10,20     ",
            "",
            &[moveto(10.0, 20.0)],
            None,
        );
    }

    #[test]
    fn handles_relative_moveto() {
        test_parser(
            "m10 20",
            "",
            &[moveto(10.0, 20.0)],
            None,
        );
    }

    #[test]
    fn handles_absolute_moveto_with_implicit_lineto() {
        test_parser(
            "M10 20 30 40",
            "",
            &[moveto(10.0, 20.0), lineto(30.0, 40.0)],
            None,
        );

        test_parser(
            "M10,20,30,40",
            "",
            &[moveto(10.0, 20.0), lineto(30.0, 40.0)],
            None,
        );

        test_parser(
            "M.1-2,3E2-4",
            "",
            &[moveto(0.1, -2.0), lineto(300.0, -4.0)],
            None,
        );
    }

    #[test]
    fn handles_relative_moveto_with_implicit_lineto() {
        test_parser(
            "m10 20 30 40",
            "",
            &[moveto(10.0, 20.0), lineto(40.0, 60.0)],
            None,
        );
    }

    #[test]
    fn handles_relative_moveto_with_relative_lineto_sequence() {
        test_parser(
            //          1     2    3    4   5
            "m 46,447 l 0,0.5 -1,0 -1,0 0,1 0,12",
            "",
            &vec![moveto(46.0, 447.0), lineto(46.0, 447.5), lineto(45.0, 447.5),
                  lineto(44.0, 447.5), lineto(44.0, 448.5), lineto(44.0, 460.5)],
            None,
        );
    }

    #[test]
    fn handles_absolute_moveto_with_implicit_linetos() {
        test_parser(
            "M10,20 30,40,50 60",
            "",
            &[moveto(10.0, 20.0), lineto(30.0, 40.0), lineto(50.0, 60.0)],
            None,
        );
    }

    #[test]
    fn handles_relative_moveto_with_implicit_linetos() {
        test_parser(
            "m10 20 30 40 50 60",
            "",
            &[moveto(10.0, 20.0), lineto(40.0, 60.0), lineto(90.0, 120.0)],
            None,
        );
    }

    #[test]
    fn handles_absolute_moveto_moveto() {
        test_parser(
            "M10 20 M 30 40",
            "",
            &[moveto(10.0, 20.0), moveto(30.0, 40.0)],
            None,
        );
    }

    #[test]
    fn handles_relative_moveto_moveto() {
        test_parser(
            "m10 20 m 30 40",
            "",
            &[moveto(10.0, 20.0), moveto(40.0, 60.0)],
            None,
        );
    }

    #[test]
    fn handles_relative_moveto_lineto_moveto() {
        test_parser(
            "m10 20 30 40 m 50 60",
            "",
            &[moveto(10.0, 20.0), lineto(40.0, 60.0), moveto(90.0, 120.0)],
            None,
        );
    }

    #[test]
    fn handles_absolute_moveto_lineto() {
        test_parser(
            "M10 20 L30,40",
            "",
            &[moveto(10.0, 20.0), lineto(30.0, 40.0)],
            None,
        );
    }

    #[test]
    fn handles_relative_moveto_lineto() {
        test_parser(
            "m10 20 l30,40",
            "",
            &[moveto(10.0, 20.0), lineto(40.0, 60.0)],
            None,
        );
    }

    #[test]
    fn handles_relative_moveto_lineto_lineto_abs_lineto() {
        test_parser(
            "m10 20 30 40l30,40,50 60L200,300",
            "",
            &vec![
                moveto(10.0, 20.0),
                lineto(40.0, 60.0),
                lineto(70.0, 100.0),
                lineto(120.0, 160.0),
                lineto(200.0, 300.0),
            ],
            None,
        );
    }

    #[test]
    fn handles_horizontal_lineto() {
        test_parser(
            "M10 20 H30",
            "",
            &[moveto(10.0, 20.0), lineto(30.0, 20.0)],
            None,
        );

        test_parser(
            "M10 20 H30 40",
            "",
            &[moveto(10.0, 20.0), lineto(30.0, 20.0), lineto(40.0, 20.0)],
            None,
        );

        test_parser(
            "M10 20 H30,40-50",
            "",
            &vec![
                moveto(10.0, 20.0),
                lineto(30.0, 20.0),
                lineto(40.0, 20.0),
                lineto(-50.0, 20.0),
            ],
            None,
        );

        test_parser(
            "m10 20 h30,40-50",
            "",
            &vec![
                moveto(10.0, 20.0),
                lineto(40.0, 20.0),
                lineto(80.0, 20.0),
                lineto(30.0, 20.0),
            ],
            None,
        );
    }

    #[test]
    fn handles_vertical_lineto() {
        test_parser(
            "M10 20 V30",
            "",
            &[moveto(10.0, 20.0), lineto(10.0, 30.0)],
            None,
        );

        test_parser(
            "M10 20 V30 40",
            "",
            &[moveto(10.0, 20.0), lineto(10.0, 30.0), lineto(10.0, 40.0)],
            None,
        );

        test_parser(
            "M10 20 V30,40-50",
            "",
            &vec![
                moveto(10.0, 20.0),
                lineto(10.0, 30.0),
                lineto(10.0, 40.0),
                lineto(10.0, -50.0),
            ],
            None,
        );

        test_parser(
            "m10 20 v30,40-50",
            "",
            &vec![
                moveto(10.0, 20.0),
                lineto(10.0, 50.0),
                lineto(10.0, 90.0),
                lineto(10.0, 40.0),
            ],
            None,
        );
    }

    #[test]
    fn handles_curveto() {
        test_parser(
            "M10 20 C 30,40 50 60-70,80",
            "",
            &[moveto(10.0, 20.0),
                curveto(30.0, 40.0, 50.0, 60.0, -70.0, 80.0)],
            None,
        );

        test_parser(
            "M10 20 C 30,40 50 60-70,80,90 100,110 120,130,140",
            "",
            &[moveto(10.0, 20.0),
                curveto(30.0, 40.0, 50.0, 60.0, -70.0, 80.0),
                curveto(90.0, 100.0, 110.0, 120.0, 130.0, 140.0)],
            None,
        );

        test_parser(
            "m10 20 c 30,40 50 60-70,80,90 100,110 120,130,140",
            "",
            &[moveto(10.0, 20.0),
                curveto(40.0, 60.0, 60.0, 80.0, -60.0, 100.0),
                curveto(30.0, 200.0, 50.0, 220.0, 70.0, 240.0)],
            None,
        );

        test_parser(
            "m10 20 c 30,40 50 60-70,80,90 100,110 120,130,140",
            "",
            &[moveto(10.0, 20.0),
                curveto(40.0, 60.0, 60.0, 80.0, -60.0, 100.0),
                curveto(30.0, 200.0, 50.0, 220.0, 70.0, 240.0)],
            None,
        );
    }

    #[test]
    fn handles_smooth_curveto() {
        test_parser(
            "M10 20 S 30,40-50,60",
            "",
            &[moveto(10.0, 20.0),
                curveto(10.0, 20.0, 30.0, 40.0, -50.0, 60.0)],
            None,
        );

        test_parser(
            "M10 20 S 30,40 50 60-70,80,90 100",
            "",
            &[moveto(10.0, 20.0),
                curveto(10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
                curveto(70.0, 80.0, -70.0, 80.0, 90.0, 100.0)],
            None,
        );

        test_parser(
            "m10 20 s 30,40 50 60-70,80,90 100",
            "",
            &[moveto(10.0, 20.0),
                curveto(10.0, 20.0, 40.0, 60.0, 60.0, 80.0),
                curveto(80.0, 100.0, -10.0, 160.0, 150.0, 180.0)],
            None,
        );
    }

    #[test]
    fn handles_quadratic_curveto() {
        test_parser(
            "M10 20 Q30 40 50 60",
            "",
            &[moveto(10.0, 20.0),
                curveto(
                    70.0 / 3.0,
                    100.0 / 3.0,
                    110.0 / 3.0,
                    140.0 / 3.0,
                    50.0,
                    60.0,
                )],
            None,
        );

        test_parser(
            "M10 20 Q30 40 50 60,70,80-90 100",
            "",
            &[moveto(10.0, 20.0),
                curveto(
                    70.0 / 3.0,
                    100.0 / 3.0,
                    110.0 / 3.0,
                    140.0 / 3.0,
                    50.0,
                    60.0,
                ),
                curveto(
                    190.0 / 3.0,
                    220.0 / 3.0,
                    50.0 / 3.0,
                    260.0 / 3.0,
                    -90.0,
                    100.0,
                )],
            None,
        );

        test_parser(
            "m10 20 q 30,40 50 60-70,80 90 100",
            "",
            &[moveto(10.0, 20.0),
                curveto(
                    90.0 / 3.0,
                    140.0 / 3.0,
                    140.0 / 3.0,
                    200.0 / 3.0,
                    60.0,
                    80.0,
                ),
                curveto(
                    40.0 / 3.0,
                    400.0 / 3.0,
                    130.0 / 3.0,
                    500.0 / 3.0,
                    150.0,
                    180.0,
                )],
            None,
        );
    }

    #[test]
    fn handles_smooth_quadratic_curveto() {
        test_parser(
            "M10 20 T30 40",
            "",
            &[moveto(10.0, 20.0),
                curveto(10.0, 20.0, 50.0 / 3.0, 80.0 / 3.0, 30.0, 40.0)],
            None,
        );

        test_parser(
            "M10 20 Q30 40 50 60 T70 80",
            "",
            &[moveto(10.0, 20.0),
                curveto(
                    70.0 / 3.0,
                    100.0 / 3.0,
                    110.0 / 3.0,
                    140.0 / 3.0,
                    50.0,
                    60.0,
                ),
                curveto(190.0 / 3.0, 220.0 / 3.0, 70.0, 80.0, 70.0, 80.0)],
            None,
        );

        test_parser(
            "m10 20 q 30,40 50 60t-70,80",
            "",
            &[moveto(10.0, 20.0),
                curveto(
                    90.0 / 3.0,
                    140.0 / 3.0,
                    140.0 / 3.0,
                    200.0 / 3.0,
                    60.0,
                    80.0,
                ),
                curveto(220.0 / 3.0, 280.0 / 3.0, 50.0, 120.0, -10.0, 160.0)],
            None,
        );
    }

    #[test]
    fn handles_elliptical_arc() {
        // no space required between arc flags
        test_parser("M 1 2 A 1 2 3 00 6 7",
                    "",
                    &[moveto(1.0, 2.0),
                          arc(1.0, 2.0, 3.0, false, false, 1.0, 2.0, 6.0, 7.0)],
                    None);
        // or after...
        test_parser("M 1 2 A 1 2 3 016 7",
                    "",
                    &[moveto(1.0, 2.0),
                          arc(1.0, 2.0, 3.0, false, true, 1.0, 2.0, 6.0, 7.0)],
                    None);
        // commas and whitespace are optionally allowed
        test_parser("M 1 2 A 1 2 3 10,6 7",
                    "",
                    &[moveto(1.0, 2.0),
                          arc(1.0, 2.0, 3.0, true, false, 1.0, 2.0, 6.0, 7.0)],
                    None);
        test_parser("M 1 2 A 1 2 3 1,16, 7",
                    "",
                    &[moveto(1.0, 2.0),
                          arc(1.0, 2.0, 3.0, true, true, 1.0, 2.0, 6.0, 7.0)],
                    None);
        test_parser("M 1 2 A 1 2 3 1,1 6 7",
                    "",
                    &[moveto(1.0, 2.0),
                          arc(1.0, 2.0, 3.0, true, true, 1.0, 2.0, 6.0, 7.0)],
                    None);
        test_parser("M 1 2 A 1 2 3 1 1 6 7",
                    "",
                    &[moveto(1.0, 2.0),
                          arc(1.0, 2.0, 3.0, true, true, 1.0, 2.0, 6.0, 7.0)],
                    None);
        test_parser("M 1 2 A 1 2 3 1 16 7",
                    "",
                    &[moveto(1.0, 2.0),
                          arc(1.0, 2.0, 3.0, true, true, 1.0, 2.0, 6.0, 7.0)],
                    None);
    }

    #[test]
    fn handles_close_path() {
        test_parser("M10 20 Z", "", &[moveto(10.0, 20.0), closepath()], None);

        test_parser(
            "m10 20 30 40 m 50 60 70 80 90 100z",
            "",
            &vec![
                moveto(10.0, 20.0),
                lineto(40.0, 60.0),
                moveto(90.0, 120.0),
                lineto(160.0, 200.0),
                lineto(250.0, 300.0),
                closepath(),
            ],
            None,
        );
    }

    #[test]
    fn first_command_must_be_moveto() {
        test_parser(
            "  L10 20",
            "   ^", // FIXME: why is this not at position 2?
            &[],
            Some(ErrorKind::UnexpectedCommand(b'L')),
        );
    }

    #[test]
    fn moveto_args() {
        test_parser(
            "M",
            " ^",
            &[],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M,",
            " ^",
            &[],
            Some(ErrorKind::UnexpectedToken(Comma)),
        );

        test_parser(
            "M10",
            "   ^",
            &[],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10,",
            "    ^",
            &[],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10x",
            "   ^",
            &[],
            Some(ErrorKind::UnexpectedToken(Command(b'x'))),
        );

        test_parser(
            "M10,x",
            "    ^",
            &[],
            Some(ErrorKind::UnexpectedToken(Command(b'x'))),
        );
    }

    #[test]
    fn moveto_implicit_lineto_args() {
        test_parser(
            "M10-20,",
            "       ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20-30",
            "         ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20-30 x",
            "          ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedToken(Command(b'x'))),
        );
    }

    #[test]
    fn closepath_no_args() {
        test_parser(
            "M10-20z10",
            "       ^",
            &[moveto(10.0, -20.0), closepath()],
            Some(ErrorKind::UnexpectedToken(Number(10.0))),
        );

        test_parser(
            "M10-20z,",
            "       ^",
            &[moveto(10.0, -20.0), closepath()],
            Some(ErrorKind::UnexpectedToken(Comma)),
        );
    }

    #[test]
    fn lineto_args() {
        test_parser(
            "M10-20L10",
            "         ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M 10,10 L 20,20,30",
            "                  ^",
            &[moveto(10.0, 10.0), lineto(20.0, 20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M 10,10 L 20,20,",
            "                ^",
            &[moveto(10.0, 10.0), lineto(20.0, 20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
    }

    #[test]
    fn horizontal_lineto_args() {
        test_parser(
            "M10-20H",
            "       ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20H,",
            "       ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedToken(Comma)),
        );

        test_parser(
            "M10-20H30,",
            "          ^",
            &[moveto(10.0, -20.0), lineto(30.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
    }

    #[test]
    fn vertical_lineto_args() {
        test_parser(
            "M10-20v",
            "       ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20v,",
            "       ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedToken(Comma)),
        );

        test_parser(
            "M10-20v30,",
            "          ^",
            &[moveto(10.0, -20.0), lineto(10.0, 10.0)],
            Some(ErrorKind::UnexpectedEof),
        );
    }

    #[test]
    fn curveto_args() {
        test_parser(
            "M10-20C1",
            "        ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20C1,",
            "         ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20C1 2",
            "          ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20C1,2,",
            "           ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20C1 2 3",
            "            ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20C1,2,3",
            "            ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20C1,2,3,",
            "             ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20C1 2 3 4",
            "              ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20C1,2,3,4",
            "              ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20C1,2,3,4,",
            "               ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20C1 2 3 4 5",
            "                ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20C1,2,3,4,5",
            "                ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20C1,2,3,4,5,",
            "                 ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20C1,2,3,4,5,6,",
            "                   ^",
            &[moveto(10.0, -20.0), curveto(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)],
            Some(ErrorKind::UnexpectedEof),
        );
    }

    #[test]
    fn smooth_curveto_args() {
        test_parser(
            "M10-20S1",
            "        ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20S1,",
            "         ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20S1 2",
            "          ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20S1,2,",
            "           ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20S1 2 3",
            "            ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20S1,2,3",
            "            ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20S1,2,3,",
            "             ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20S1,2,3,4,",
            "               ^",
            &[moveto(10.0, -20.0),
                curveto(10.0, -20.0, 1.0, 2.0, 3.0, 4.0)],
            Some(ErrorKind::UnexpectedEof),
        );
    }

    #[test]
    fn quadratic_bezier_curveto_args() {
        test_parser(
            "M10-20Q1",
            "        ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20Q1,",
            "         ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20Q1 2",
            "          ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20Q1,2,",
            "           ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20Q1 2 3",
            "            ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20Q1,2,3",
            "            ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20Q1,2,3,",
            "             ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10 20 Q30 40 50 60,",
            "                    ^",
            &[moveto(10.0, 20.0),
                curveto(
                    70.0 / 3.0,
                    100.0 / 3.0,
                    110.0 / 3.0,
                    140.0 / 3.0,
                    50.0,
                    60.0,
                )],
            Some(ErrorKind::UnexpectedEof),
        );
    }

    #[test]
    fn smooth_quadratic_bezier_curveto_args() {
        test_parser(
            "M10-20T1",
            "        ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20T1,",
            "         ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10 20 T30 40,",
            "              ^",
            &[moveto(10.0, 20.0),
                curveto(10.0, 20.0, 50.0 / 3.0, 80.0 / 3.0, 30.0, 40.0)],
            Some(ErrorKind::UnexpectedEof),
        );
    }

    #[test]
    fn elliptical_arc_args() {
        test_parser(
            "M10-20A1",
            "        ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20A1,",
            "         ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20A1 2",
            "          ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20A1 2,",
            "           ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20A1 2 3",
            "            ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20A1 2 3,",
            "             ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20A1 2 3 4",
            "             ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::LexError(LexError::UnexpectedByte(b'4'))),
        );

        test_parser(
            "M10-20A1 2 3 1",
            "              ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20A1 2 3,1,",
            "               ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20A1 2 3 1 5",
            "               ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::LexError(LexError::UnexpectedByte(b'5'))),
        );

        test_parser(
            "M10-20A1 2 3 1 1",
            "                ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20A1 2 3,1,1,",
            "                 ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        test_parser(
            "M10-20A1 2 3 1 1 6",
            "                  ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );
        test_parser(
            "M10-20A1 2 3,1,1,6,",
            "                   ^",
            &[moveto(10.0, -20.0)],
            Some(ErrorKind::UnexpectedEof),
        );

        // no non 0|1 chars allowed for flags
        test_parser("M 1 2 A 1 2 3 1.0 0.0 6 7",
                    "               ^",
                    &[moveto(1.0, 2.0)],
                    Some(ErrorKind::UnexpectedToken(Number(0.0))));

        test_parser("M10-20A1 2 3,1,1,6,7,",
                    "                     ^",
                    &[moveto(10.0, -20.0),
                          arc(1.0, 2.0, 3.0, true, true, 10.0, -20.0, 6.0, 7.0)],
                    Some(ErrorKind::UnexpectedEof));
    }

    #[test]
    fn bugs() {
        // https://gitlab.gnome.org/GNOME/librsvg/issues/345
        test_parser(
            "M.. 1,0 0,100000",
            " ^", // FIXME: we have to report position of error in lexer errors to make this right
            &[],
            Some(ErrorKind::LexError(LexError::UnexpectedByte(b'.'))),
        );
    }
}
