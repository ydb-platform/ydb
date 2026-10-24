use alloc::{boxed::Box, vec::Vec};
use core::cmp;

use crate::{
    cache::Cache,
    hash::{Which, ZopfliHash},
    symbols::{get_dist_symbol, get_length_symbol},
    util::{
        boxed_array, ZOPFLI_MAX_CHAIN_HITS, ZOPFLI_MAX_MATCH, ZOPFLI_MIN_MATCH, ZOPFLI_NUM_D,
        ZOPFLI_NUM_LL, ZOPFLI_WINDOW_MASK, ZOPFLI_WINDOW_SIZE,
    },
};

#[derive(Clone, Copy)]
pub enum LitLen {
    Literal(u16),
    LengthDist(u16, u16),
}

impl LitLen {
    pub const fn size(&self) -> usize {
        match *self {
            Self::Literal(_) => 1,
            Self::LengthDist(len, _) => len as usize,
        }
    }
}

/// Stores lit/length and dist pairs for LZ77.
/// Parameter litlens: Contains the literal symbols or length values.
/// Parameter dists: Contains the distances. A value is 0 to indicate that there is
/// no dist and the corresponding litlens value is a literal instead of a length.
/// Parameter size: The size of both the litlens and dists arrays.
#[derive(Clone, Default)]
pub struct Lz77Store {
    pub litlens: Vec<LitLen>,

    pub pos: Vec<usize>,

    ll_symbol: Vec<u16>,
    d_symbol: Vec<u16>,

    ll_counts: Vec<usize>,
    d_counts: Vec<usize>,
}

impl Lz77Store {
    pub const fn new() -> Self {
        Self {
            litlens: vec![],

            pos: vec![],

            ll_symbol: vec![],
            d_symbol: vec![],

            ll_counts: vec![],
            d_counts: vec![],
        }
    }

    pub fn reset(&mut self) {
        self.litlens.clear();
        self.pos.clear();
        self.ll_symbol.clear();
        self.d_symbol.clear();
        self.ll_counts.clear();
        self.d_counts.clear();
    }

    pub fn size(&self) -> usize {
        self.litlens.len()
    }

    pub fn append_store_item(&mut self, litlen: LitLen, pos: usize) {
        let origsize = self.litlens.len();
        let llstart = ZOPFLI_NUM_LL * (origsize / ZOPFLI_NUM_LL);
        let dstart = ZOPFLI_NUM_D * (origsize / ZOPFLI_NUM_D);

        if origsize % ZOPFLI_NUM_LL == 0 {
            if origsize == 0 {
                self.ll_counts.resize(origsize + ZOPFLI_NUM_LL, 0);
            } else {
                // Append last histogram
                self.ll_counts
                    .extend_from_within((origsize - ZOPFLI_NUM_LL)..origsize);
            }
        }

        if origsize % ZOPFLI_NUM_D == 0 {
            if origsize == 0 {
                self.d_counts.resize(ZOPFLI_NUM_D, 0);
            } else {
                // Append last histogram
                self.d_counts
                    .extend_from_within((origsize - ZOPFLI_NUM_D)..origsize);
            }
        }

        self.pos.push(pos);

        // Why isn't this at the beginning of this function?
        // assert(length < 259);

        self.litlens.push(litlen);
        match litlen {
            LitLen::Literal(length) => {
                self.ll_symbol.push(length);
                self.d_symbol.push(0);
                self.ll_counts[llstart + length as usize] += 1;
            }
            LitLen::LengthDist(length, dist) => {
                let len_sym = get_length_symbol(length as usize);
                self.ll_symbol.push(len_sym as u16);
                self.d_symbol.push(get_dist_symbol(dist));
                self.ll_counts[llstart + len_sym] += 1;
                self.d_counts[dstart + get_dist_symbol(dist) as usize] += 1;
            }
        }
    }

    pub fn lit_len_dist(&mut self, length: u16, dist: u16, pos: usize) {
        let litlen = if dist == 0 {
            LitLen::Literal(length)
        } else {
            LitLen::LengthDist(length, dist)
        };

        self.append_store_item(litlen, pos);
    }

    /// Does LZ77 using an algorithm similar to gzip, with lazy matching, rather than
    /// with the slow but better "squeeze" implementation.
    /// The result is placed in the `Lz77Store`.
    /// If instart is larger than 0, it uses values before instart as starting
    /// dictionary.
    pub fn greedy<C: Cache>(&mut self, lmc: &mut C, in_data: &[u8], instart: usize, inend: usize) {
        if instart == inend {
            return;
        }
        let windowstart = instart.saturating_sub(ZOPFLI_WINDOW_SIZE);
        let mut h = ZopfliHash::new();

        let arr = &in_data[..inend];
        h.warmup(arr, windowstart, inend);

        for i in windowstart..instart {
            h.update(arr, i);
        }

        let mut i = instart;
        let mut leng;
        let mut dist;
        let mut lengthscore;

        /* Lazy matching. */
        let mut prev_length = 0;
        let mut prev_match = 0;
        let mut prevlengthscore;
        let mut match_available = false;
        while i < inend {
            h.update(arr, i);

            let longest_match =
                find_longest_match(lmc, &h, arr, i, inend, instart, ZOPFLI_MAX_MATCH, &mut None);
            dist = longest_match.distance;
            leng = longest_match.length;
            lengthscore = get_length_score(i32::from(leng), i32::from(dist));

            /* Lazy matching. */
            prevlengthscore = get_length_score(prev_length as i32, prev_match as i32);
            if match_available {
                match_available = false;
                if lengthscore > prevlengthscore + 1 {
                    self.lit_len_dist(u16::from(arr[i - 1]), 0, i - 1);
                    if (lengthscore as usize) >= ZOPFLI_MIN_MATCH
                        && (leng as usize) < ZOPFLI_MAX_MATCH
                    {
                        match_available = true;
                        prev_length = u32::from(leng);
                        prev_match = u32::from(dist);
                        i += 1;
                        continue;
                    }
                } else {
                    /* Add previous to output. */
                    leng = prev_length as u16;
                    dist = prev_match as u16;
                    /* Add to output. */
                    verify_len_dist(arr, i - 1, dist, leng);
                    self.lit_len_dist(leng, dist, i - 1);
                    for _ in 2..leng {
                        debug_assert!(i < inend);
                        i += 1;
                        h.update(arr, i);
                    }
                    i += 1;
                    continue;
                }
            } else if (lengthscore as usize) >= ZOPFLI_MIN_MATCH
                && (leng as usize) < ZOPFLI_MAX_MATCH
            {
                match_available = true;
                prev_length = u32::from(leng);
                prev_match = u32::from(dist);
                i += 1;
                continue;
            }
            /* End of lazy matching. */

            /* Add to output. */
            if (lengthscore as usize) >= ZOPFLI_MIN_MATCH {
                verify_len_dist(arr, i, dist, leng);
                self.lit_len_dist(leng, dist, i);
            } else {
                leng = 1;
                self.lit_len_dist(u16::from(arr[i]), 0, i);
            }
            for _ in 1..leng {
                debug_assert!(i < inend);
                i += 1;
                h.update(arr, i);
            }
            i += 1;
        }
    }

    pub fn follow_path<C: Cache>(
        &mut self,
        in_data: &[u8],
        instart: usize,
        inend: usize,
        path: Vec<u16>,
        lmc: &mut C,
    ) {
        let windowstart = instart.saturating_sub(ZOPFLI_WINDOW_SIZE);

        if instart == inend {
            return;
        }

        let mut h = ZopfliHash::new();

        let arr = &in_data[..inend];
        h.warmup(arr, windowstart, inend);

        for i in windowstart..instart {
            h.update(arr, i);
        }

        let mut pos = instart;
        for item in path.into_iter().rev() {
            let mut length = item;
            debug_assert!(pos < inend);

            h.update(arr, pos);

            // Add to output.
            if length >= ZOPFLI_MIN_MATCH as u16 {
                // Get the distance by recalculating longest match. The found length
                // should match the length from the path.
                let longest_match = find_longest_match(
                    lmc,
                    &h,
                    arr,
                    pos,
                    inend,
                    instart,
                    length as usize,
                    &mut None,
                );
                let dist = longest_match.distance;
                let dummy_length = longest_match.length;
                debug_assert!(!(dummy_length != length && length > 2 && dummy_length > 2));
                verify_len_dist(arr, pos, dist, length);
                self.lit_len_dist(length, dist, pos);
            } else {
                length = 1;
                self.lit_len_dist(u16::from(arr[pos]), 0, pos);
            }

            debug_assert!(pos + (length as usize) <= inend);
            for j in 1..(length as usize) {
                h.update(arr, pos + j);
            }

            pos += length as usize;
        }
    }

    fn get_histogram_at(
        &self,
        lpos: usize,
    ) -> (Box<[usize; ZOPFLI_NUM_LL]>, Box<[usize; ZOPFLI_NUM_D]>) {
        let mut ll = boxed_array(0);
        let mut d = boxed_array(0);

        /* The real histogram is created by using the histogram for this chunk, but
        all superfluous values of this chunk subtracted. */
        let llpos = ZOPFLI_NUM_LL * (lpos / ZOPFLI_NUM_LL);
        let dpos = ZOPFLI_NUM_D * (lpos / ZOPFLI_NUM_D);

        for (i, item) in ll.iter_mut().enumerate() {
            *item = self.ll_counts[llpos + i];
        }
        let end = cmp::min(llpos + ZOPFLI_NUM_LL, self.size());
        for i in (lpos + 1)..end {
            ll[self.ll_symbol[i] as usize] -= 1;
        }

        for (i, item) in d.iter_mut().enumerate() {
            *item = self.d_counts[dpos + i];
        }
        let end = cmp::min(dpos + ZOPFLI_NUM_D, self.size());
        for i in (lpos + 1)..end {
            if let LitLen::LengthDist(_, _) = self.litlens[i] {
                d[self.d_symbol[i] as usize] -= 1;
            }
        }

        (ll, d)
    }

    /// Gets the histogram of lit/len and dist symbols in the given range, using the
    /// cumulative histograms, so faster than adding one by one for large range. Does
    /// not add the one end symbol of value 256.
    pub fn get_histogram(
        &self,
        lstart: usize,
        lend: usize,
    ) -> (Box<[usize; ZOPFLI_NUM_LL]>, Box<[usize; ZOPFLI_NUM_D]>) {
        if lstart + ZOPFLI_NUM_LL * 3 > lend {
            let mut ll_counts = boxed_array(0);
            let mut d_counts = boxed_array(0);
            for i in lstart..lend {
                ll_counts[self.ll_symbol[i] as usize] += 1;
                if let LitLen::LengthDist(_, _) = self.litlens[i] {
                    d_counts[self.d_symbol[i] as usize] += 1;
                }
            }
            (ll_counts, d_counts)
        } else {
            /* Subtract the cumulative histograms at the end and the start to get the
            histogram for this range. */
            let (ll, d) = self.get_histogram_at(lend - 1);

            if lstart > 0 {
                let (ll2, d2) = self.get_histogram_at(lstart - 1);

                (
                    ll.iter()
                        .zip(ll2.iter())
                        .map(|(&ll_item1, &ll_item2)| ll_item1 - ll_item2)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                    d.iter()
                        .zip(d2.iter())
                        .map(|(&d_item1, &d_item2)| d_item1 - d_item2)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                )
            } else {
                (ll, d)
            }
        }
    }

    pub fn get_byte_range(&self, lstart: usize, lend: usize) -> usize {
        if lstart == lend {
            return 0;
        }

        let l = lend - 1;
        self.pos[l] + self.litlens[l].size() - self.pos[lstart]
    }
}

pub struct LongestMatch {
    pub distance: u16,
    pub length: u16,
    pub from_cache: bool,
    pub limit: usize,
}

impl LongestMatch {
    pub const fn new(limit: usize) -> Self {
        Self {
            distance: 0,
            length: 0,
            from_cache: false,
            limit,
        }
    }
}

/// Finds how long the match of `scan` and `match` is. Can be used to find how many
/// bytes starting from `scan`, and from `match`, are equal. Returns the last byte
/// after `scan`, which is still equal to the corresponding byte after `match`.
/// `scan` is the position to compare; `match` is the earlier position to compare.
/// `end` is the last possible byte, beyond which to stop looking.
fn get_match(scan_arr: &[u8], match_arr: &[u8]) -> usize {
    let max_prefix_len = cmp::min(scan_arr.len(), match_arr.len()); // The prefix won't be longer than the shortest array
    let mut i = 0;

    // This is a fairly hot function, and Rust's LLVM backend cannot autovectorize
    // a comparison loop over bytes yet. To make it faster, we bring back the
    // "several bytes at a time" optimization present in the original Zopfli code,
    // but with a twist: instead of working with up to 64 bits at a time, let's
    // always do the bulk of the work 128 bits at a time, using Rust's `u128` type.
    // On x64, LLVM optimizes this function excellently, using SSE SIMD instructions
    // such as `pcmpeqb` and `pmovmskb`, and it's likely such gains also translate to
    // other architectures with baseline SIMD support for 128 bit vectors. This is
    // even faster than the original Zopfli C code, while being more portable!
    //
    // The second condition in the while loop is there to guard against overflow when
    // adding CHUNK_SIZE to i, allowing the compiler to not emit bound checks panic code
    const CHUNK_SIZE: usize = core::mem::size_of::<u128>();
    while i + CHUNK_SIZE < max_prefix_len && i + CHUNK_SIZE <= usize::MAX - CHUNK_SIZE {
        let scan_chunk = u128::from_le_bytes(scan_arr[i..i + CHUNK_SIZE].try_into().unwrap());
        let match_chunk = u128::from_le_bytes(match_arr[i..i + CHUNK_SIZE].try_into().unwrap());

        let bit_diff_mask = scan_chunk ^ match_chunk;
        if bit_diff_mask != 0 {
            // Different bit in chunk found. Note that due to chunks being loaded as
            // little-endian for better performance on little-endian CPUs, the number
            // of trailing zeros represents the position of the first differing bit
            // accurate only up to the byte boundary. Precise bit offsets would
            // require reading big-endian integers and counting the leading zeros
            // instead
            return i + bit_diff_mask.trailing_zeros() as usize / 8;
        }

        i += CHUNK_SIZE;
    }

    // Now handle up to 15 remaining bytes naively, one at a time. Any performance
    // gains from smaller chunks are likely to be negligible
    for j in i..max_prefix_len {
        if scan_arr[j] != match_arr[j] {
            return j; // Different byte found
        }
    }

    // All compared bytes are equal, so the common prefix is as long as it can be
    max_prefix_len
}

#[allow(clippy::too_many_arguments)]
pub fn find_longest_match<C: Cache>(
    lmc: &mut C,
    h: &ZopfliHash,
    array: &[u8],
    pos: usize,
    size: usize,
    blockstart: usize,
    limit: usize,
    sublen: &mut Option<&mut [u16]>,
) -> LongestMatch {
    let mut longest_match = lmc.try_get(pos, limit, sublen, blockstart);

    if longest_match.from_cache {
        debug_assert!(pos + (longest_match.length as usize) <= size);
        return longest_match;
    }

    let mut limit = longest_match.limit;

    debug_assert!(limit <= ZOPFLI_MAX_MATCH);
    debug_assert!(limit >= ZOPFLI_MIN_MATCH);
    debug_assert!(pos < size);

    if size - pos < ZOPFLI_MIN_MATCH {
        /* The rest of the code assumes there are at least ZOPFLI_MIN_MATCH bytes to
        try. */
        longest_match.distance = 0;
        longest_match.length = 0;
        longest_match.from_cache = false;
        longest_match.limit = 0;
        return longest_match;
    }

    if pos + limit > size {
        limit = size - pos;
    }

    let (bestdist, bestlength) = find_longest_match_loop(h, array, pos, size, limit, sublen);

    lmc.store(pos, limit, sublen, bestdist, bestlength, blockstart);

    debug_assert!(bestlength <= limit as u16);

    debug_assert!(pos + bestlength as usize <= size);
    longest_match.distance = bestdist;
    longest_match.length = bestlength;
    longest_match.from_cache = false;
    longest_match.limit = limit;
    longest_match
}

fn find_longest_match_loop(
    h: &ZopfliHash,
    array: &[u8],
    pos: usize,
    size: usize,
    limit: usize,
    sublen: &mut Option<&mut [u16]>,
) -> (u16, u16) {
    let mut which_hash = Which::Hash1;
    let hpos = pos & ZOPFLI_WINDOW_MASK;

    let mut pp = hpos; /* During the whole loop, p == hprev[pp]. */
    let mut p = h.prev_at(pp, which_hash);

    let mut dist = if p < pp {
        pp - p
    } else {
        ZOPFLI_WINDOW_SIZE - p + pp
    };

    let mut bestlength = 1;
    let mut bestdist = 0;
    let mut chain_counter = ZOPFLI_MAX_CHAIN_HITS; /* For quitting early. */
    let arrayend = pos + limit;
    let mut scan_offset;
    let mut match_offset;

    /* Go through all distances. */
    while dist < ZOPFLI_WINDOW_SIZE && chain_counter > 0 {
        let mut currentlength = 0;

        debug_assert!(p < ZOPFLI_WINDOW_SIZE);
        debug_assert_eq!(p, h.prev_at(pp, which_hash));
        debug_assert_eq!(h.hash_val_at(p, which_hash), i32::from(h.val(which_hash)));

        if dist > 0 {
            debug_assert!(pos < size);
            debug_assert!(dist <= pos);
            scan_offset = pos;
            match_offset = pos - dist;

            /* Testing the byte at position bestlength first, goes slightly faster. */
            if pos + bestlength >= size
                || array[scan_offset + bestlength] == array[match_offset + bestlength]
            {
                let same0 = h.same[pos & ZOPFLI_WINDOW_MASK];
                if same0 > 2 && array[scan_offset] == array[match_offset] {
                    let same1 = h.same[(pos - dist) & ZOPFLI_WINDOW_MASK];
                    let same = cmp::min(cmp::min(same0, same1), limit as u16) as usize;
                    scan_offset += same;
                    match_offset += same;
                }
                scan_offset = get_match(
                    &array[scan_offset..arrayend],
                    &array[match_offset..arrayend],
                ) + scan_offset;
                currentlength = scan_offset - pos; /* The found length. */
            }

            if currentlength > bestlength {
                if let Some(ref mut subl) = *sublen {
                    for sublength in subl.iter_mut().take(currentlength + 1).skip(bestlength + 1) {
                        *sublength = dist as u16;
                    }
                }
                bestdist = dist;
                bestlength = currentlength;
                if currentlength >= limit {
                    break;
                }
            }
        }

        /* Switch to the other hash once this will be more efficient. */
        if which_hash == Which::Hash1
            && bestlength >= h.same[hpos] as usize
            && i32::from(h.val(Which::Hash2)) == h.hash_val_at(p, Which::Hash2)
        {
            /* Now use the hash that encodes the length and first byte. */
            which_hash = Which::Hash2;
        }

        pp = p;
        p = h.prev_at(p, which_hash);
        if p == pp {
            break; /* Uninited prev value. */
        }

        dist += if p < pp {
            pp - p
        } else {
            ZOPFLI_WINDOW_SIZE - p + pp
        };

        chain_counter -= 1;
    }
    (bestdist as u16, bestlength as u16)
}

/// Gets a score of the length given the distance. Typically, the score of the
/// length is the length itself, but if the distance is very long, decrease the
/// score of the length a bit to make up for the fact that long distances use large
/// amounts of extra bits.
///
/// This is not an accurate score, it is a heuristic only for the greedy LZ77
/// implementation. More accurate cost models are employed later. Making this
/// heuristic more accurate may hurt rather than improve compression.
///
/// The two direct uses of this heuristic are:
/// -avoid using a length of 3 in combination with a long distance. This only has
///  an effect if length == 3.
/// -make a slightly better choice between the two options of the lazy matching.
///
/// Indirectly, this affects:
/// -the block split points if the default of block splitting first is used, in a
///  rather unpredictable way
/// -the first zopfli run, so it affects the chance of the first run being closer
///  to the optimal output
const fn get_length_score(length: i32, distance: i32) -> i32 {
    // At 1024, the distance uses 9+ extra bits and this seems to be the sweet spot
    // on tested files.
    if distance > 1024 {
        length - 1
    } else {
        length
    }
}

#[cfg(debug_assertions)]
fn verify_len_dist(data: &[u8], pos: usize, dist: u16, length: u16) {
    for i in 0..length {
        let d1 = data[pos - (dist as usize) + (i as usize)];
        let d2 = data[pos + (i as usize)];
        if d1 != d2 {
            assert_eq!(d1, d2);
            break;
        }
    }
}

#[cfg(not(debug_assertions))]
fn verify_len_dist(_data: &[u8], _pos: usize, _dist: u16, _length: u16) {}
