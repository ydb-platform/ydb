SELECT
    -- Use explicit comparasion instead of canonization to produce more human readable test input data.
    -- Canonization of binary data produces encoded bytes.
    key,
    String::ReverseBits(subkey) == value FROM Input;
