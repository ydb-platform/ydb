#pragma once
#include "position.h"
#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NArrow::NMerger {

class TBatchIterator {
private:
    bool ControlPointFlag;
    TRWSortableBatchPosition KeyColumns;
    TRWSortableBatchPosition VersionColumns;
    i64 RecordsCount;
    int ReverseSortKff;

    std::shared_ptr<NArrow::TColumnFilter> Filter;
    std::shared_ptr<NArrow::TColumnFilter::TIterator> FilterIterator;

    i32 GetFirstPosition() const {
        if (ReverseSortKff > 0) {
            return 0;
        } else {
            return RecordsCount - 1;
        }
    }

public:
    NJson::TJsonValue DebugJson() const;

    const std::shared_ptr<NArrow::TColumnFilter>& GetFilter() const {
        return Filter;
    }

    bool IsControlPoint() const {
        return ControlPointFlag;
    }

    const TRWSortableBatchPosition& GetKeyColumns() const {
        return KeyColumns;
    }

    const TRWSortableBatchPosition& GetVersionColumns() const {
        return VersionColumns;
    }

    TBatchIterator(TRWSortableBatchPosition&& keyColumns)
        : ControlPointFlag(true)
        , KeyColumns(std::move(keyColumns))
    {

    }

    template <class TDataContainer>
    TBatchIterator(std::shared_ptr<TDataContainer> batch, std::shared_ptr<NArrow::TColumnFilter> filter,
        const std::vector<std::string>& keyColumns, const std::vector<std::string>& dataColumns, const bool reverseSort, const std::vector<std::string>& versionColumnNames)
        : ControlPointFlag(false)
        , KeyColumns(batch, 0, keyColumns, dataColumns, reverseSort)
        , VersionColumns(batch, 0, versionColumnNames, {}, false)
        , RecordsCount(batch->num_rows())
        , ReverseSortKff(reverseSort ? -1 : 1)
        , Filter(filter) {
        Y_ABORT_UNLESS(KeyColumns.InitPosition(GetFirstPosition()));
        Y_ABORT_UNLESS(VersionColumns.InitPosition(GetFirstPosition()));
        if (Filter) {
            FilterIterator = std::make_shared<NArrow::TColumnFilter::TIterator>(Filter->GetIterator(reverseSort, RecordsCount));
        }
    }

    bool CheckNextBatch(const TBatchIterator& nextIterator) {
        return KeyColumns.Compare(nextIterator.KeyColumns) == std::partial_ordering::less;
    }

    bool IsReverse() const {
        return ReverseSortKff < 0;
    }

    bool IsDeleted() const {
        if (!FilterIterator) {
            return false;
        }
        return !FilterIterator->GetCurrentAcceptance();
    }

    TSortableBatchPosition::TFoundPosition SkipToLower(const TSortableBatchPosition& pos);

    bool Next();

    bool operator<(const TBatchIterator& item) const;
};

}
