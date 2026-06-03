#include "accessor.h"

#include <ydb/core/formats/arrow/size_calcer.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor {

std::optional<ui64> TCompactKVArray::DoGetRawSize() const {
    return NArrow::GetArrayDataSize(Array);
}

TMinMax TCompactKVArray::DoGetMinMaxScalars() const {
    return TMinMax::Compute(Array);
}

ui32 TCompactKVArray::DoGetValueRawBytes() const {
    return NArrow::GetArrayDataSize(Array);
}

void TCompactKVArray::Reallocate() {
    Array = NArrow::ReallocateArray(Array);
}

}   // namespace NKikimr::NArrow::NAccessor
