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
    if (StartIndex && RecordsCount) {
        const ui32 start = std::max<ui32>(offset, *StartIndex);
        const ui32 finish = std::min<ui32>(offset + count, *StartIndex + *RecordsCount);
        if (finish <= start) {
            result = std::nullopt;
        } else {
            result = TColumnConstructionContext().SetStartIndex(start - offset).SetRecordsCount(finish - start, count);
        }
    } else if (StartIndex && !RecordsCount) {
        result = TColumnConstructionContext().SetStartIndex(std::max<ui32>(offset, *StartIndex) - offset);
    } else if (!StartIndex && RecordsCount) {
        result = TColumnConstructionContext().SetRecordsCount(std::min<ui32>(count, *RecordsCount), count);
    } else {
        result = TColumnConstructionContext();
    }
    if (result && Filter) {
        result->SetFilter(std::make_shared<TColumnFilter>(Filter->Slice(offset, count)));
    }
    return result;
}

}   // namespace NKikimr::NArrow::NAccessor
