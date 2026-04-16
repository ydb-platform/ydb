#include "array_power2.h"

namespace NKikimr::NOlap::NIndexes {

TArrayPower2BitsStorage TArrayPower2BitsStorage::Fold(ui32 times) const {
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

} // namespace NKikimr::NOlap::NIndexes
