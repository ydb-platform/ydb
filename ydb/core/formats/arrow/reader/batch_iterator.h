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
    YDB_READONLY(ui64, SourceId, 0);

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
    TBatchIterator(std::shared_ptr<TDataContainer> batch, std::shared_ptr<NArrow::TColumnFilter> filter, const arrow::Schema& keySchema,
        const arrow::Schema& dataSchema, const bool reverseSort, const std::vector<std::string>& versionColumnNames, const ui64 sourceId)
        : ControlPointFlag(false)
        , KeyColumns(batch, 0, keySchema.field_names(), dataSchema.field_names(), reverseSort)
        , VersionColumns(batch, 0, versionColumnNames, {}, false)
        , RecordsCount(batch->num_rows())
        , ReverseSortKff(reverseSort ? -1 : 1)
        , SourceId(sourceId)
        , Filter(filter) {
        AFL_VERIFY(KeyColumns.IsSameSortingSchema(keySchema))("batch", KeyColumns.DebugJson())("schema", keySchema.ToString());
        AFL_VERIFY(KeyColumns.IsSameDataSchema(dataSchema))("batch", KeyColumns.DebugJson())("schema", dataSchema.ToString());
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
