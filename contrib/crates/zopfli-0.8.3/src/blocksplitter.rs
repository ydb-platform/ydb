use alloc::vec::Vec;

#[cfg(feature = "std")]
use log::{debug, log_enabled};

use crate::{cache::NoCache, deflate::calculate_block_size_auto_type, lz77::Lz77Store};

/// Finds minimum of function `f(i)` where `i` is of type `usize`, `f(i)` is of type
/// `f64`, `i` is in range `start-end` (excluding `end`).
/// Returns the index to the minimum and the minimum value.
fn find_minimum<F: Fn(usize) -> f64>(f: F, start: usize, end: usize) -> (usize, f64) {
    if end - start < 1024 {
        let mut best = f64::INFINITY;
        let mut result = start;
        for i in start..end {
            let v = f(i);
            if v < best {
                best = v;
                result = i;
            }
        }
        (result, best)
    } else {
        /* Try to find minimum faster by recursively checking multiple points. */
        let mut start = start;
        let mut end = end;
        const NUM: usize = 9; /* Good value: 9. ?!?!?!?! */
        let mut p = [0; NUM];
        let mut vp = [0.0; NUM];
        let mut lastbest = f64::INFINITY;
        let mut pos = start;

        while end - start > NUM {
            let mut besti = 0;
            let mut best = f64::INFINITY;
            let multiplier = (end - start) / (NUM + 1);
            for i in 0..NUM {
                p[i] = start + (i + 1) * multiplier;
                vp[i] = f(p[i]);
                if vp[i] < best {
                    best = vp[i];
                    besti = i;
                }
            }
            if best > lastbest {
                break;
            }

            start = if besti == 0 { start } else { p[besti - 1] };
            end = if besti == NUM - 1 { end } else { p[besti + 1] };

            pos = p[besti];
            lastbest = best;
        }
        (pos, lastbest)
    }
}

/// Returns estimated cost of a block in bits.  It includes the size to encode the
/// tree and the size to encode all literal, length and distance symbols and their
/// extra bits.
///
/// litlens: lz77 lit/lengths
/// dists: ll77 distances
/// lstart: start of block
/// lend: end of block (not inclusive)
fn estimate_cost(lz77: &Lz77Store, lstart: usize, lend: usize) -> f64 {
    calculate_block_size_auto_type(lz77, lstart, lend)
}

/// Finds next block to try to split, the largest of the available ones.
/// The largest is chosen to make sure that if only a limited amount of blocks is
/// requested, their sizes are spread evenly.
/// lz77size: the size of the LL77 data, which is the size of the done array here.
/// done: array indicating which blocks starting at that position are no longer
///     splittable (splitting them increases rather than decreases cost).
/// splitpoints: the splitpoints found so far.
/// npoints: the amount of splitpoints found so far.
/// lstart: output variable, giving start of block.
/// lend: output variable, giving end of block.
/// returns 1 if a block was found, 0 if no block found (all are done).
fn find_largest_splittable_block(
    lz77size: usize,
    done: &[u8],
    splitpoints: &[usize],
) -> Option<(usize, usize)> {
    let mut longest = 0;
    let mut found = None;

    let mut last = 0;

    for &item in splitpoints {
        if done[last] == 0 && item - last > longest {
            found = Some((last, item));
            longest = item - last;
        }
        last = item;
    }

    let end = lz77size - 1;
    if done[last] == 0 && end - last > longest {
        found = Some((last, end));
    }

    found
}

/// Prints the block split points as decimal and hex values in the terminal.
#[inline]
fn print_block_split_points(lz77: &Lz77Store, lz77splitpoints: &[usize]) {
    if !log_enabled!(log::Level::Debug) {
        return;
    }

    let nlz77points = lz77splitpoints.len();
    let mut splitpoints = Vec::with_capacity(nlz77points);

    /* The input is given as lz77 indices, but we want to see the uncompressed
    index values. */
    let mut pos = 0;
    if nlz77points > 0 {
        for (i, item) in lz77.litlens.iter().enumerate() {
            let length = item.size();
            if lz77splitpoints[splitpoints.len()] == i {
                splitpoints.push(pos);
                if splitpoints.len() == nlz77points {
                    break;
                }
            }
            pos += length;
        }
    }
    debug_assert_eq!(splitpoints.len(), nlz77points);

    debug!(
        "block split points: {} (hex: {})",
        splitpoints
            .iter()
            .map(|&sp| format!("{sp}"))
            .collect::<Vec<_>>()
            .join(" "),
        splitpoints
            .iter()
            .map(|&sp| format!("{sp:x}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
}

/// Does blocksplitting on LZ77 data.
/// The output splitpoints are indices in the LZ77 data.
/// maxblocks: set a limit to the amount of blocks. Set to 0 to mean no limit.
pub fn blocksplit_lz77(lz77: &Lz77Store, maxblocks: u16, splitpoints: &mut Vec<usize>) {
    if lz77.size() < 10 {
        return; /* This code fails on tiny files. */
    }

    let mut numblocks = 1u32;
    let mut done = vec![0; lz77.size()];
    let mut lstart = 0;
    let mut lend = lz77.size();

    while maxblocks != 0 && numblocks < u32::from(maxblocks) {
        debug_assert!(lstart < lend);
        let find_minimum_result = find_minimum(
            |i| estimate_cost(lz77, lstart, i) + estimate_cost(lz77, i, lend),
            lstart + 1,
            lend,
        );
        let llpos = find_minimum_result.0;
        let splitcost = find_minimum_result.1;

        debug_assert!(llpos > lstart);
        debug_assert!(llpos < lend);

        let origcost = estimate_cost(lz77, lstart, lend);

        if splitcost > origcost || llpos == lstart + 1 || llpos == lend {
            done[lstart] = 1;
        } else {
            splitpoints.push(llpos);
            splitpoints.sort_unstable();
            numblocks += 1;
        }

        // If `find_largest_splittable_block` returns `None`, no further split will
        // likely reduce compression.
        let is_finished = find_largest_splittable_block(lz77.size(), &done, splitpoints).map_or(
            true,
            |(start, end)| {
                lstart = start;
                lend = end;
                lend - lstart < 10
            },
        );

        if is_finished {
            break;
        }
    }

    print_block_split_points(lz77, splitpoints);
}

/// Does blocksplitting on uncompressed data.
/// The output splitpoints are indices in the uncompressed bytes.
///
/// options: general program options.
/// in: uncompressed input data
/// instart: where to start splitting
/// inend: where to end splitting (not inclusive)
/// maxblocks: maximum amount of blocks to split into, or 0 for no limit
/// splitpoints: dynamic array to put the resulting split point coordinates into.
///   The coordinates are indices in the input array.
/// npoints: pointer to amount of splitpoints, for the dynamic array. The amount of
///   blocks is the amount of splitpoitns + 1.
pub fn blocksplit(
    in_data: &[u8],
    instart: usize,
    inend: usize,
    maxblocks: u16,
    splitpoints: &mut Vec<usize>,
) {
    splitpoints.clear();
    let mut store = Lz77Store::new();

    /* Unintuitively, Using a simple LZ77 method here instead of lz77_optimal
    results in better blocks. */
    {
        store.greedy(&mut NoCache, in_data, instart, inend);
    }

    let mut lz77splitpoints = Vec::with_capacity(maxblocks as usize);
    blocksplit_lz77(&store, maxblocks, &mut lz77splitpoints);

    let nlz77points = lz77splitpoints.len();

    /* Convert LZ77 positions to positions in the uncompressed input. */
    let mut pos = instart;
    if nlz77points > 0 {
        for (i, item) in store.litlens.iter().enumerate() {
            let length = item.size();
            if lz77splitpoints[splitpoints.len()] == i {
                splitpoints.push(pos);
                if splitpoints.len() == nlz77points {
                    break;
                }
            }
            pos += length;
        }
    }
    debug_assert_eq!(splitpoints.len(), nlz77points);
}
