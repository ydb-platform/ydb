#include "common.h"

#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NArrow::NAccessor {

TColumnConstructionContext& TColumnConstructionContext::SetFilter(const std::shared_ptr<TColumnFilter>& val) {
    if (!val || val->IsTotalAllowFilter()) {
        Filter = nullptr;
    } else {
        Filter = val;
    }
    return *this;
}

std::optional<TColumnConstructionContext> TColumnConstructionContext::Slice(const ui32 offset, const ui32 count) const {
    std::optional<TColumnConstructionContext> result;
    const ui32 start = std::max<ui32>(offset, StartIndex.value_or(0));
    const ui32 finish = std::min<ui32>(offset + count, StartIndex.value_or(0) + RecordsCount.value_or(offset + count));
    if (finish <= start) {
        result = std::nullopt;
    } else {
        result = TColumnConstructionContext().SetStartIndex(start - offset).SetRecordsCount(finish - start, count);
    }
    if (result && Filter) {
        result->SetFilter(std::make_shared<TColumnFilter>(Filter->Slice(offset, count)));
    }
    return result;
}

}   // namespace NKikimr::NArrow::NAccessor
