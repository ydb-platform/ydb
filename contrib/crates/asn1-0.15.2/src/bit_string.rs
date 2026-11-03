use alloc::vec::Vec;

/// Represents an ASN.1 `BIT STRING` whose contents is borrowed.
#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct BitString<'a> {
    data: &'a [u8],
    padding_bits: u8,
}

impl<'a> BitString<'a> {
    pub fn new(data: &'a [u8], padding_bits: u8) -> Option<BitString<'a>> {
        if padding_bits > 7 || (data.is_empty() && padding_bits != 0) {
            return None;
        }
        if padding_bits > 0 && data[data.len() - 1] & ((1 << padding_bits) - 1) != 0 {
            return None;
        }

        Some(BitString { data, padding_bits })
    }

    /// Returns a sequence of bytes representing the data in the `BIT STRING`. Padding bits will
    /// always be 0.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.data
    }

    /// Returns the number of padding bits. Will always be in [0, 8).
    pub fn padding_bits(&self) -> u8 {
        self.padding_bits
    }

    /// Returns whether the requested bit is set. Padding bits will always return false and
    /// asking for bits that exceed the length of the bit string will also return false.
    pub fn has_bit_set(&self, n: usize) -> bool {
        let idx = n / 8;
        let v = 1 << (7 - (n & 0x07));
        if self.data.len() < (idx + 1) {
            false
        } else {
            self.data[idx] & v != 0
        }
    }
}

/// Represents an ASN.1 `BIT STRING` whose contents owned. When used to parse
/// data this will allocate.
#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct OwnedBitString {
    data: Vec<u8>,
    padding_bits: u8,
}

impl OwnedBitString {
    pub fn new(data: Vec<u8>, padding_bits: u8) -> Option<OwnedBitString> {
        BitString::new(&data, padding_bits)?;
        Some(OwnedBitString { data, padding_bits })
    }

    pub fn as_bitstring(&self) -> BitString<'_> {
        BitString::new(&self.data, self.padding_bits).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::{BitString, OwnedBitString};
    use alloc::vec;

    #[test]
    fn test_bitstring_new() {
        assert_eq!(BitString::new(b"abc", 8), None);
        assert_eq!(BitString::new(b"", 2), None);
        assert_eq!(BitString::new(b"\xff", 1), None);

        assert!(BitString::new(b"\xff", 0).is_some());
        assert!(BitString::new(b"\xfe", 1).is_some());
    }

    #[test]
    fn test_owned_bitstring_new() {
        assert_eq!(OwnedBitString::new(vec![b'a', b'b', b'c'], 8), None);
        assert_eq!(OwnedBitString::new(vec![], 2), None);
        assert_eq!(OwnedBitString::new(vec![0xff], 1), None);

        assert!(OwnedBitString::new(vec![0xff], 0).is_some());
        assert!(OwnedBitString::new(vec![0xfe], 1).is_some());
    }

    #[test]
    fn test_bitstring_as_bytes() {
        let bs = BitString::new(b"\xfe", 1).unwrap();
        assert_eq!(bs.as_bytes(), b"\xfe");
    }

    #[test]
    fn test_bitstring_padding_bits() {
        let bs = BitString::new(b"\xfe", 1).unwrap();
        assert_eq!(bs.padding_bits(), 1);
        let bs = BitString::new(b"\xe0", 5).unwrap();
        assert_eq!(bs.padding_bits(), 5);
    }

    #[test]
    fn test_bitstring_has_bit_set() {
        let bs = BitString::new(b"\x80", 0).unwrap();
        assert!(bs.has_bit_set(0));
        assert!(!bs.has_bit_set(1));
        assert!(!bs.has_bit_set(7));
        // An arbitrary bit much bigger than the underlying size of the bitfield
        assert!(!bs.has_bit_set(50));
        let bs = BitString::new(b"\xc0", 4).unwrap();
        // padding bits should always return false when asking if the bit is set
        assert!(bs.has_bit_set(0));
        assert!(bs.has_bit_set(1));
        assert!(!bs.has_bit_set(2));
        assert!(!bs.has_bit_set(3));
        assert!(!bs.has_bit_set(4));
        assert!(!bs.has_bit_set(5));
        assert!(!bs.has_bit_set(6));
        assert!(!bs.has_bit_set(7));
    }
}
