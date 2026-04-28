#pragma once

#include <util/generic/string.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <bit>
#include <climits>
#include <memory>

namespace NKikimr::NOlap::NIndexes {

// Bitset builder over an array with size a power of 2. In comparison to util/generic/bitmap.h impls it simultaneously
// * does absolute minimum of operations on insertion
// * has runtime-defined size
class TArrayPower2BitsStorage {
private:
    const ui32 DataSize;
    // ui64[] is slightly faster than ui8[], perhaps because there is no conversion inside operator()
    std::unique_ptr<ui64[]> Data;
    const ui32 SizeMask;
    static_assert(CHAR_BIT == 8);
    static constexpr ui64 BitsInItem = sizeof(ui64) * CHAR_BIT;
    static constexpr ui64 ItemMask = BitsInItem - 1;
    static constexpr ui64 SizeShift = std::popcount(ItemMask);

    TArrayPower2BitsStorage(ui32 dataSize, std::unique_ptr<ui64[]> data)
        : DataSize(dataSize)
        , Data(std::move(data))
        , SizeMask(DataSize - 1) {
        Y_ABORT_UNLESS(std::has_single_bit(dataSize));
    }

    Y_FORCE_INLINE ui64 ItemMaskForHash(ui64 hash) const {
        return (ui64{1} << (hash & ItemMask));
    }

    Y_FORCE_INLINE ui64 IndexForHash(ui64 hash) const {
        return (hash >> SizeShift) & SizeMask;
    }

public:
    TArrayPower2BitsStorage(ui32 bitSize)
        : DataSize(bitSize / BitsInItem)
        , Data(new ui64[DataSize])
        , SizeMask(DataSize - 1) {
        Y_ABORT_UNLESS(bitSize >= BitsInItem);
        std::fill(Data.get(), Data.get() + DataSize, 0);
        Y_ABORT_UNLESS(std::has_single_bit(bitSize));
    }

    TString SerializeToString() const {
        return TString(reinterpret_cast<char*>(Data.get()), DataSize * sizeof(ui64));
    }

    TString SerializeDynBitMapCompatible() const;

    Y_FORCE_INLINE void operator()(const ui64 hash) {
        Data[IndexForHash(hash)] |= ItemMaskForHash(hash);
    }

    bool Get(const ui64 hash) const {
        return Data[IndexForHash(hash)] & ItemMaskForHash(hash);
    }

    ui32 CountSetBits() const {
        ui32 result = 0;
        for (size_t i = 0; i < DataSize; ++i) {
            result += std::popcount(Data[i]);
        }
        return result;
    }

    ui32 BitSize() const {
        return DataSize * BitsInItem;
    }

    TArrayPower2BitsStorage Fold(ui32 times) const;
};

}   // namespace NKikimr::NOlap::NIndexes
