#pragma once

#include <util/generic/string.h>
#include <util/system/yassert.h>

#include <algorithm>
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

    TArrayPower2BitsStorage(ui32 dataSize, std::unique_ptr<ui64[]> data)
        : DataSize(dataSize)
        , Data(std::move(data))
        , SizeMask(DataSize - 1)
    {
        Y_ABORT_UNLESS((dataSize & (dataSize - 1)) == 0);
    }

public:
    TArrayPower2BitsStorage(ui32 bitsSize)
        : DataSize(bitsSize / BitsInItem)
        , Data(new ui64[DataSize])
        , SizeMask(DataSize - 1)
    {
        std::fill(Data.get(), Data.get() + DataSize, 0);
        Y_ABORT_UNLESS((bitsSize & (bitsSize - 1)) == 0);
    }

    TString SerializeToString() {
        return TString(reinterpret_cast<char*>(Data.get()), DataSize * sizeof(ui64));
    }

    Y_FORCE_INLINE void operator()(const ui64 hash) {
        // 64 == 2 ** 6
        Data[(hash >> 6) & SizeMask] |= (static_cast<ui64>(1) << (hash & ItemMask));
    }

    ui32 Count() const {
        ui32 result = 0;
        for (size_t i = 0; i < DataSize; ++i) {
            result +=std::popcount(Data[i]);
        }
        return result;
    }

    TArrayPower2BitsStorage Fold(ui32 times) {
        Y_ABORT_UNLESS(DataSize % times == 0);
        auto newDataSize = DataSize / times;
        std::unique_ptr<ui64[]> newData(new ui64[newDataSize]);
        std::fill(newData.get(), newData.get() + newDataSize, 0);
        for (size_t i = 0; i < times; ++i) {
            auto start = newDataSize * i;
            for (size_t j = 0; j < newDataSize; ++j) {
                newData[j] |= Data[start + j];
            }
        }

        return TArrayPower2BitsStorage(newDataSize, std::move(newData));
    }
};

} // namespace NKikimr::NOlap::NIndexes
