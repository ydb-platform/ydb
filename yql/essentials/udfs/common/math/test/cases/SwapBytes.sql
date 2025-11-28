SELECT
    Math::SwapBytes(CAST(128 as Uint8)),
    Math::SwapBytes(CAST(128 as Uint16)),
    Math::SwapBytes(CAST(128 as Uint32)),
    Math::SwapBytes(CAST(128 as Uint64)),

    Math::SwapBytes(CAST(Null as Uint64?)),
    Math::SwapBytes(CAST(0x1234567890abcdef as Uint64));
