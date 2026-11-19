use alloc::vec::Vec;
use core::cmp;

use crate::{
    lz77::LongestMatch,
    util::{ZOPFLI_CACHE_LENGTH, ZOPFLI_MAX_MATCH, ZOPFLI_MIN_MATCH},
};

// Cache used by ZopfliFindLongestMatch to remember previously found length/dist
// values.
// This is needed because the squeeze runs will ask these values multiple times for
// the same position.
// Uses large amounts of memory, since it has to remember the distance belonging
// to every possible shorter-than-the-best length (the so called "sublen" array).
pub struct ZopfliLongestMatchCache {
    length: Vec<u16>,
    dist: Vec<u16>,
    sublen: Vec<u8>,
}

impl ZopfliLongestMatchCache {
    pub fn new(blocksize: usize) -> Self {
        Self {
            /* length > 0 and dist 0 is invalid combination, which indicates on purpose
            that this cache value is not filled in yet. */
            length: vec![1; blocksize],
            dist: vec![0; blocksize],
            /* Rather large amount of memory. */
            sublen: vec![0; ZOPFLI_CACHE_LENGTH * blocksize * 3],
        }
    }

    fn length_at(&self, pos: usize) -> u16 {
        self.length[pos]
    }

    fn dist_at(&self, pos: usize) -> u16 {
        self.dist[pos]
    }

    fn store_length_at(&mut self, pos: usize, val: u16) {
        self.length[pos] = val;
    }

    fn store_dist_at(&mut self, pos: usize, val: u16) {
        self.dist[pos] = val;
    }

    /// Returns the length up to which could be stored in the cache.
    fn max_sublen(&self, pos: usize) -> u32 {
        let start = ZOPFLI_CACHE_LENGTH * pos * 3;
        if self.sublen[start + 1] == 0 && self.sublen[start + 2] == 0 {
            return 0; // No sublen cached.
        }
        u32::from(self.sublen[start + ((ZOPFLI_CACHE_LENGTH - 1) * 3)]) + 3
    }

    /// Stores sublen array in the cache.
    fn store_sublen(&mut self, sublen: &[u16], pos: usize, length: usize) {
        if length < 3 {
            return;
        }

        let start = ZOPFLI_CACHE_LENGTH * pos * 3;
        let mut i = 3;
        let mut j = 0;
        let mut bestlength = 0;
        while i <= length {
            if i == length || sublen[i] != sublen[i + 1] {
                self.sublen[start + (j * 3)] = (i - 3) as u8;
                self.sublen[start + (j * 3 + 1)] = sublen[i].wrapping_rem(256) as u8;
                self.sublen[start + (j * 3 + 2)] = (sublen[i] >> 8).wrapping_rem(256) as u8;
                bestlength = i as u32;
                j += 1;
                if j >= ZOPFLI_CACHE_LENGTH {
                    break;
                }
            }
            i += 1;
        }

        if j < ZOPFLI_CACHE_LENGTH {
            debug_assert_eq!(bestlength, length as u32);
            self.sublen[start + ((ZOPFLI_CACHE_LENGTH - 1) * 3)] = (bestlength - 3) as u8;
        } else {
            debug_assert!(bestlength <= length as u32);
        }
        debug_assert_eq!(bestlength, self.max_sublen(pos));
    }

    /// Extracts sublen array from the cache.
    fn fetch_sublen(&self, pos: usize, length: usize, sublen: &mut [u16]) {
        if length < 3 {
            return;
        }

        let start = ZOPFLI_CACHE_LENGTH * pos * 3;
        let maxlength = self.max_sublen(pos) as usize;
        let mut prevlength = 0;

        for j in 0..ZOPFLI_CACHE_LENGTH {
            let length = self.sublen[start + (j * 3)] as usize + 3;
            let dist = u16::from(self.sublen[start + (j * 3 + 1)])
                + 256 * u16::from(self.sublen[start + (j * 3 + 2)]);

            let mut i = prevlength;
            while i <= length {
                sublen[i] = dist;
                i += 1;
            }
            if length == maxlength {
                break;
            }
            prevlength = length + 1;
        }
    }
}

pub trait Cache {
    fn try_get(
        &self,
        pos: usize,
        limit: usize,
        sublen: &mut Option<&mut [u16]>,
        blockstart: usize,
    ) -> LongestMatch;
    fn store(
        &mut self,
        pos: usize,
        limit: usize,
        sublen: &mut Option<&mut [u16]>,
        distance: u16,
        length: u16,
        blockstart: usize,
    );
}

pub struct NoCache;

impl Cache for NoCache {
    fn try_get(
        &self,
        _: usize,
        limit: usize,
        _: &mut Option<&mut [u16]>,
        _: usize,
    ) -> LongestMatch {
        LongestMatch::new(limit)
    }

    fn store(&mut self, _: usize, _: usize, _: &mut Option<&mut [u16]>, _: u16, _: u16, _: usize) {
        // Nowhere to store
    }
}

impl Cache for ZopfliLongestMatchCache {
    fn try_get(
        &self,
        pos: usize,
        mut limit: usize,
        sublen: &mut Option<&mut [u16]>,
        blockstart: usize,
    ) -> LongestMatch {
        let mut longest_match = LongestMatch::new(limit);

        /* The LMC cache starts at the beginning of the block rather than the
        beginning of the whole array. */
        let lmcpos = pos - blockstart;

        /* Length > 0 and dist 0 is invalid combination, which indicates on purpose
        that this cache value is not filled in yet. */
        let length_lmcpos = self.length_at(lmcpos);
        let dist_lmcpos = self.dist_at(lmcpos);
        let cache_available = length_lmcpos == 0 || dist_lmcpos != 0;
        let max_sublen = self.max_sublen(lmcpos);
        let limit_ok_for_cache = limit == ZOPFLI_MAX_MATCH
            || length_lmcpos <= limit as u16
            || (sublen.is_some() && max_sublen >= limit as u32);

        if limit_ok_for_cache && cache_available {
            if sublen.is_none() || u32::from(length_lmcpos) <= max_sublen {
                let length = cmp::min(length_lmcpos, limit as u16);
                let distance;
                if let Some(ref mut subl) = *sublen {
                    self.fetch_sublen(lmcpos, length as usize, subl);
                    distance = subl[length as usize];

                    if limit == ZOPFLI_MAX_MATCH && length >= ZOPFLI_MIN_MATCH as u16 {
                        debug_assert_eq!(subl[length as usize], dist_lmcpos);
                    }
                } else {
                    distance = dist_lmcpos;
                }
                longest_match.distance = distance;
                longest_match.length = length;
                longest_match.from_cache = true;
                return longest_match;
            }
            /* Can't use much of the cache, since the "sublens" need to be calculated,
            but at least we already know when to stop. */
            limit = length_lmcpos as usize;
            longest_match.limit = limit;
        }

        longest_match
    }

    fn store(
        &mut self,
        pos: usize,
        limit: usize,
        sublen: &mut Option<&mut [u16]>,
        distance: u16,
        length: u16,
        blockstart: usize,
    ) {
        if let Some(ref mut subl) = *sublen {
            /* Length > 0 and dist 0 is invalid combination, which indicates on purpose
            that this cache value is not filled in yet. */
            let lmcpos = pos - blockstart;
            let cache_available = self.length_at(lmcpos) == 0 || self.dist_at(lmcpos) != 0;

            if limit == ZOPFLI_MAX_MATCH && !cache_available {
                debug_assert!(self.length_at(lmcpos) == 1 && self.dist_at(lmcpos) == 0);
                if length < ZOPFLI_MIN_MATCH as u16 {
                    self.store_dist_at(lmcpos, 0);
                    self.store_length_at(lmcpos, 0);
                } else {
                    self.store_dist_at(lmcpos, distance);
                    self.store_length_at(lmcpos, length);
                }
                debug_assert!(!(self.length_at(lmcpos) == 1 && self.dist_at(lmcpos) == 0));
                self.store_sublen(subl, lmcpos, length as usize);
            }
        }
    }
}
