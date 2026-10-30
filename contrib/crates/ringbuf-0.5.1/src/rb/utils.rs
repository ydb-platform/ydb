use core::{num::NonZeroUsize, ops::Range};

/// Returns a pair of ranges between `start` and `end` indices in a ring buffer with specific `capacity`.
///
/// `start` and `end` may be arbitrary large, but must satisfy the following condition: `0 <= (start - end) % (2 * capacity) <= capacity`.
/// Actual indices are taken modulo `capacity`.
///
/// The first range starts from `start`. If the first slice is empty then second slice is empty too.
pub fn ranges(capacity: NonZeroUsize, start: usize, end: usize) -> (Range<usize>, Range<usize>) {
    let (head_quo, head_rem) = (start / capacity, start % capacity);
    let (tail_quo, tail_rem) = (end / capacity, end % capacity);

    if (head_quo + tail_quo) % 2 == 0 {
        (head_rem..tail_rem, 0..0)
    } else {
        (head_rem..capacity.get(), 0..tail_rem)
    }
}
