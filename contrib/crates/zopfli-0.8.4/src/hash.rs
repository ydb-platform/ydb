use alloc::{
    alloc::{alloc, handle_alloc_error, Layout},
    boxed::Box,
};
use core::ptr::{addr_of, addr_of_mut, NonNull};

use crate::util::{ZOPFLI_MIN_MATCH, ZOPFLI_WINDOW_MASK, ZOPFLI_WINDOW_SIZE};

const HASH_SHIFT: i32 = 5;
const HASH_MASK: u16 = 32767;

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Which {
    Hash1,
    Hash2,
}

#[derive(Clone)]
pub struct SmallerHashThing {
    prev: u16,            /* Index to index of prev. occurrence of same hash. */
    hashval: Option<u16>, /* Index to hash value at this index. */
}

#[derive(Clone)]
pub struct HashThing {
    head: [i16; 65536], /* Hash value to index of its most recent occurrence. */
    prev_and_hashval: [SmallerHashThing; ZOPFLI_WINDOW_SIZE],
    val: u16, /* Current hash value. */
}

impl HashThing {
    fn update(&mut self, hpos: usize) {
        let hashval = self.val;
        let index = self.val as usize;
        let head_index = self.head[index];
        let prev = if head_index >= 0
            && self.prev_and_hashval[head_index as usize].hashval == Some(self.val)
        {
            head_index as u16
        } else {
            hpos as u16
        };

        self.prev_and_hashval[hpos] = SmallerHashThing {
            prev,
            hashval: Some(hashval),
        };
        self.head[index] = hpos as i16;
    }
}

#[derive(Clone)]
pub struct ZopfliHash {
    hash1: HashThing,
    hash2: HashThing,
    pub same: [u16; ZOPFLI_WINDOW_SIZE], /* Amount of repetitions of same byte after this .*/
}

impl ZopfliHash {
    pub fn new() -> Box<Self> {
        const LAYOUT: Layout = Layout::new::<ZopfliHash>();

        let ptr = NonNull::new(unsafe { alloc(LAYOUT) }.cast::<Self>())
            .unwrap_or_else(|| handle_alloc_error(LAYOUT));

        unsafe {
            Self::init(ptr);
            Box::from_raw(ptr.as_ptr())
        }
    }

    /// Initializes the [`ZopfliHash`] instance pointed by `hash` to an initial state.
    ///
    /// ## Safety
    /// `hash` must point to aligned, valid memory for writes.
    unsafe fn init(hash: NonNull<Self>) {
        let hash = hash.as_ptr();

        // SAFETY: addr_of(_mut) macros are used to avoid creating intermediate references, which
        //         are undefined behavior when data is uninitialized. Note that it also is UB to
        //         assume that integer values and arrays can be read after allocating their memory:
        //         the allocator returns valid, but uninitialized pointers that are not guaranteed
        //         to hold a fixed bit pattern (c.f. core::mem::MaybeUnit docs and
        //         https://doc.rust-lang.org/std/ptr/index.html#safety).

        for i in 0..ZOPFLI_WINDOW_SIZE {
            // Arrays are guaranteed to be laid out with their elements placed in consecutive
            // memory positions: https://doc.rust-lang.org/reference/type-layout.html#array-layout.
            // Therefore, a pointer to an array has the same address as the pointer to its first
            // element, and adding size_of::<N>() bytes to that address yields the address of the
            // second element, and so on.
            let prev_and_hashval = addr_of_mut!((*hash).hash1.prev_and_hashval)
                .cast::<SmallerHashThing>()
                .add(i);
            addr_of_mut!((*prev_and_hashval).prev).write(i as u16);
            addr_of_mut!((*prev_and_hashval).hashval).write(None);
        }

        // Rust signed integers are guaranteed to be represented in two's complement notation:
        // https://doc.rust-lang.org/reference/types/numeric.html#integer-types
        // In this notation, -1 is expressed as an all-ones value. Therefore, writing
        // size_of::<[i16; N]> all-ones bytes initializes all of them to -1.
        addr_of_mut!((*hash).hash1.head).write_bytes(0xFF, 1);
        addr_of_mut!((*hash).hash1.val).write(0);

        addr_of_mut!((*hash).hash2).copy_from_nonoverlapping(addr_of!((*hash).hash1), 1);

        // Zero-initializes all the array elements
        addr_of_mut!((*hash).same).write_bytes(0, 1);
    }

    pub fn reset(&mut self) {
        unsafe { Self::init(NonNull::new(self).unwrap()) }
    }

    pub fn warmup(&mut self, arr: &[u8], pos: usize, end: usize) {
        let c = arr[pos];
        self.update_val(c);

        if pos + 1 < end {
            let c = arr[pos + 1];
            self.update_val(c);
        }
    }

    /// Update the sliding hash value with the given byte. All calls to this function
    /// must be made on consecutive input characters. Since the hash value exists out
    /// of multiple input bytes, a few warmups with this function are needed initially.
    fn update_val(&mut self, c: u8) {
        self.hash1.val = ((self.hash1.val << HASH_SHIFT) ^ u16::from(c)) & HASH_MASK;
    }

    pub fn update(&mut self, array: &[u8], pos: usize) {
        let hash_value = array.get(pos + ZOPFLI_MIN_MATCH - 1).copied().unwrap_or(0);
        self.update_val(hash_value);

        let hpos = pos & ZOPFLI_WINDOW_MASK;

        self.hash1.update(hpos);

        // Update "same".
        let mut amount = 0;
        let same = self.same[pos.wrapping_sub(1) & ZOPFLI_WINDOW_MASK];
        if same > 1 {
            amount = same - 1;
        }

        let mut another_index = pos + amount as usize + 1;
        let array_pos = array[pos];
        while another_index < array.len() && array_pos == array[another_index] && amount < u16::MAX
        {
            amount += 1;
            another_index += 1;
        }

        self.same[hpos] = amount;

        self.hash2.val = (amount.wrapping_sub(ZOPFLI_MIN_MATCH as u16) & 255) ^ self.hash1.val;

        self.hash2.update(hpos);
    }

    pub fn prev_at(&self, index: usize, which: Which) -> usize {
        (match which {
            Which::Hash1 => self.hash1.prev_and_hashval[index].prev,
            Which::Hash2 => self.hash2.prev_and_hashval[index].prev,
        }) as usize
    }

    pub fn hash_val_at(&self, index: usize, which: Which) -> i32 {
        let hashval = match which {
            Which::Hash1 => self.hash1.prev_and_hashval[index].hashval,
            Which::Hash2 => self.hash2.prev_and_hashval[index].hashval,
        };
        hashval.map_or(-1, i32::from)
    }

    pub fn val(&self, which: Which) -> u16 {
        match which {
            Which::Hash1 => self.hash1.val,
            Which::Hash2 => self.hash2.val,
        }
    }
}
