use alloc::vec::Vec;

/// Converts a series of Huffman tree bitlengths, to the bit values of the symbols.
pub fn lengths_to_symbols(lengths: &[u32], max_bits: u32) -> Vec<u32> {
    let mut bl_count = vec![0; (max_bits + 1) as usize];
    let mut next_code = vec![0; (max_bits + 1) as usize];
    let n = lengths.len();

    let mut symbols = vec![0; n];

    // 1) Count the number of codes for each code length. Let bl_count[N] be the
    // number of codes of length N, N >= 1. */
    for &length in lengths {
        assert!(length <= max_bits);
        bl_count[length as usize] += 1;
    }
    // 2) Find the numerical value of the smallest code for each code length.
    let mut code = 0;
    bl_count[0] = 0;
    for bits in 1..=max_bits {
        code = (code + bl_count[(bits - 1) as usize]) << 1;
        next_code[bits as usize] = code;
    }
    // 3) Assign numerical values to all codes, using consecutive values for all
    // codes of the same length with the base values determined at step 2.
    for i in 0..n {
        let len = lengths[i] as usize;
        if len != 0 {
            symbols[i] = next_code[len];
            next_code[len] += 1;
        }
    }
    symbols
}
