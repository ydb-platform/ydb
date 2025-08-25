#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/system/types.h>

#include <memory>
#include <optional>

namespace NKikimr::NArrow {
class TColumnFilter;
}

namespace NKikimr::NArrow::NAccessor {

class TColumnConstructionContext {
private:
    YDB_READONLY_DEF(std::optional<ui32>, StartIndex);
    YDB_ACCESSOR_DEF(std::optional<ui32>, RecordsCount);
    YDB_READONLY_DEF(std::shared_ptr<TColumnFilter>, Filter);

public:
    TColumnConstructionContext& SetRecordsCount(const ui32 recordsCount, const ui32 defValue) {
        if (recordsCount == defValue) {
            RecordsCount.reset();
        } else {
            RecordsCount = recordsCount;
        }
        return *this;
    }

    TColumnConstructionContext& SetStartIndex(const ui32 startIndex) {
        if (startIndex) {
            StartIndex = startIndex;
        } else {
            StartIndex.reset();
        }
        return *this;
    }

    TColumnConstructionContext& SetFilter(const std::shared_ptr<TColumnFilter>& val);

    std::optional<TColumnConstructionContext> Slice(const ui32 offset, const ui32 count) const;

    TColumnConstructionContext() = default;
};

}   // namespace NKikimr::NArrow::NAccessor
