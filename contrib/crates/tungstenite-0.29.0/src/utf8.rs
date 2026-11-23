use std::{cmp, error::Error, fmt, str};

#[derive(Debug, Copy, Clone)]
pub(crate) enum DecodeError<'a> {
    /// In lossy decoding insert `valid_prefix`, then `"\u{FFFD}"`,
    /// then call `decode()` again with `remaining_input`.
    Invalid { valid_prefix: &'a str, invalid_sequence: &'a [u8], remaining_input: &'a [u8] },

    /// Call the `incomplete_suffix.try_complete` method with more input when available.
    /// If no more input is available, this is an invalid byte sequence.
    Incomplete { valid_prefix: &'a str, incomplete_suffix: Incomplete },
}

impl<'a> fmt::Display for DecodeError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DecodeError::Invalid { valid_prefix, invalid_sequence, remaining_input } => write!(
                f,
                "found invalid byte sequence {invalid_sequence:02x?} after \
                 {valid_byte_count} valid bytes, followed by {unprocessed_byte_count} more \
                 unprocessed bytes",
                invalid_sequence = invalid_sequence,
                valid_byte_count = valid_prefix.len(),
                unprocessed_byte_count = remaining_input.len()
            ),
            DecodeError::Incomplete { valid_prefix, incomplete_suffix } => write!(
                f,
                "found incomplete byte sequence {incomplete_suffix:02x?} after \
                 {valid_byte_count} bytes",
                incomplete_suffix = incomplete_suffix,
                valid_byte_count = valid_prefix.len()
            ),
        }
    }
}

impl<'a> Error for DecodeError<'a> {}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Incomplete {
    pub(crate) buffer: [u8; 4],
    pub(crate) buffer_len: u8,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Completed<'buffer, 'input> {
    pub(crate) result: Result<&'buffer str, &'buffer [u8]>,
    pub(crate) remaining_input: &'input [u8],
}

pub(crate) fn decode(input: &'_ [u8]) -> Result<&'_ str, DecodeError<'_>> {
    let error = match str::from_utf8(input) {
        Ok(valid) => return Ok(valid),
        Err(error) => error,
    };

    // FIXME: separate function from here to guide inlining?
    let (valid, after_valid) = input.split_at(error.valid_up_to());
    let valid = unsafe { str::from_utf8_unchecked(valid) };

    match error.error_len() {
        Some(invalid_sequence_length) => {
            let (invalid, rest) = after_valid.split_at(invalid_sequence_length);
            Err(DecodeError::Invalid {
                valid_prefix: valid,
                invalid_sequence: invalid,
                remaining_input: rest,
            })
        }
        None => Err(DecodeError::Incomplete {
            valid_prefix: valid,
            incomplete_suffix: Incomplete::new(after_valid),
        }),
    }
}

impl Incomplete {
    pub(crate) fn new(bytes: &[u8]) -> Self {
        let mut buffer = [0, 0, 0, 0];
        let len = bytes.len();
        buffer[..len].copy_from_slice(bytes);
        Incomplete { buffer, buffer_len: len as u8 }
    }

    /// * `None`: still incomplete, call `try_complete` again with more input.
    ///   If no more input is available, this is invalid byte sequence.
    /// * `Some(completed)`: We’re done with this `Incomplete`,
    ///   with either a valid chunk on invalid byte sequence in `completed.result`.
    ///   To keep decoding, pass `completed.remaining_input` to `decode()`.
    pub(crate) fn try_complete<'input>(
        &mut self,
        input: &'input [u8],
    ) -> Option<Completed<'_, 'input>> {
        let (consumed, opt_result) = self.try_complete_offsets(input);
        let result = opt_result?;
        let remaining_input = &input[consumed..];
        let result_bytes = self.take_buffer();
        let result = match result {
            Ok(()) => Ok(unsafe { str::from_utf8_unchecked(result_bytes) }),
            Err(()) => Err(result_bytes),
        };
        Some(Completed { result, remaining_input })
    }

    fn take_buffer(&mut self) -> &[u8] {
        let len = self.buffer_len as usize;
        self.buffer_len = 0;
        &self.buffer[..len]
    }

    /// (consumed_from_input, None): not enough input
    /// (consumed_from_input, Some(Err(()))): error bytes in buffer
    /// (consumed_from_input, Some(Ok(()))): UTF-8 string in buffer
    fn try_complete_offsets(&mut self, input: &[u8]) -> (usize, Option<Result<(), ()>>) {
        let initial_buffer_len = self.buffer_len as usize;
        let copied_from_input;
        {
            let unwritten = &mut self.buffer[initial_buffer_len..];
            copied_from_input = cmp::min(unwritten.len(), input.len());
            unwritten[..copied_from_input].copy_from_slice(&input[..copied_from_input]);
        }
        let spliced = &self.buffer[..initial_buffer_len + copied_from_input];
        match str::from_utf8(spliced) {
            Ok(_) => {
                self.buffer_len = spliced.len() as u8;
                (copied_from_input, Some(Ok(())))
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                if valid_up_to > 0 {
                    let consumed = valid_up_to.checked_sub(initial_buffer_len).unwrap();
                    self.buffer_len = valid_up_to as u8;
                    (consumed, Some(Ok(())))
                } else {
                    match error.error_len() {
                        Some(invalid_sequence_length) => {
                            let consumed =
                                invalid_sequence_length.checked_sub(initial_buffer_len).unwrap();
                            self.buffer_len = invalid_sequence_length as u8;
                            (consumed, Some(Err(())))
                        }
                        None => {
                            self.buffer_len = spliced.len() as u8;
                            (copied_from_input, None)
                        }
                    }
                }
            }
        }
    }
}
