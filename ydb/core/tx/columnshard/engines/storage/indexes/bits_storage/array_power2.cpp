#include "array_power2.h"

#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/ysaveload.h>

namespace NKikimr::NOlap::NIndexes {

TString TArrayPower2BitsStorage::SerializeDynBitMapCompatible() const {
    // TBitSetStorage requires serialization format of TDynBitMap, so reproduce it here
    TString result;
    result.reserve(DataSize * sizeof(ui64) + 2 * sizeof(ui64));
    TStringOutput out(result);

    ::Save(&out, ui8(sizeof(ui64)));
    ::Save(&out, ui64(BitSize()));
    ::SavePodArray(&out, Data.get(), DataSize);
    return result;
}

TArrayPower2BitsStorage TArrayPower2BitsStorage::Fold(ui32 times) const {
    Y_ABORT_UNLESS(std::has_single_bit(times));
    Y_ABORT_UNLESS(times < DataSize);
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

} // namespace NKikimr::NOlap::NIndexes
