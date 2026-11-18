#include "eq_height_histogram_test_helpers.h"

namespace NKikimr::NHistogramTest {

// Memcomparable key from an integer: 4 bytes big-endian, sign bit flipped.
TString MakeKey(i32 value) {
    const ui32 unsignedValue = static_cast<ui32>(value) ^ (1U << 31);
    TString key(4, '\0');
    key[0] = static_cast<char>((unsignedValue >> 24) & 0xFF);
    key[1] = static_cast<char>((unsignedValue >> 16) & 0xFF);
    key[2] = static_cast<char>((unsignedValue >> 8) & 0xFF);
    key[3] = static_cast<char>(unsignedValue & 0xFF);
    return key;
}

TEqHeightHistogramBuilder::TParams MakeParams(ui32 numBuckets, ui64 maxStateBytes) {
    return {
        .NumBuckets = numBuckets,
        .EmissionRate = numBuckets * 8,
        .MaxStateBytes = maxStateBytes,
    };
}

// Sorted stream: 0,0,...,1,1,...,2,2,... with `perKey` copies each.
TVector<TString> MakeSortedStream(ui32 numKeys, ui32 perKey) {
    TVector<TString> keys(Reserve(static_cast<size_t>(numKeys) * perKey));
    for (ui32 keyIndex = 0; keyIndex < numKeys; ++keyIndex) {
        for (ui32 repetitionIndex = 0; repetitionIndex < perKey; ++repetitionIndex) {
            keys.push_back(MakeKey(static_cast<i32>(keyIndex)));
        }
    }
    return keys;
}

TEqHeightHistogramBuilder BuildFromStream(const TVector<TString>& keys, const TEqHeightHistogramBuilder::TParams& params) {
    TEqHeightHistogramBuilder builder(params);
    for (const auto& key : keys) {
        builder.Add(key);
    }
    return builder;
}

} // namespace NKikimr::NHistogramTest
