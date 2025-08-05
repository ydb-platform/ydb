#pragma once
#include "position.h"
#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NArrow::NMerger {

class TIterationOrder {
private:
    YDB_READONLY_DEF(bool, IsReversed);
    YDB_READONLY_DEF(ui64, Start);

public:
    TIterationOrder(const bool reverse, const ui64 start)
        : IsReversed(reverse)
        , Start(start) {
    }

    static TIterationOrder Forward(const ui64 start) {
        return TIterationOrder(false, start);
    }

    static TIterationOrder Reversed(const ui64 start) {
        return TIterationOrder(true, start);
    }
};

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

    i32 GetPosition(const ui64 position) const {
        AFL_VERIFY((i64)position < RecordsCount);
        if (ReverseSortKff > 0) {
            return position;
        } else {
            return RecordsCount - position - 1;
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
        const arrow::Schema& dataSchema, const std::vector<std::string>& versionColumnNames, const ui64 sourceId,
        const TIterationOrder& order = TIterationOrder::Forward(0))
        : ControlPointFlag(false)
        , KeyColumns(batch, 0, keySchema.field_names(), dataSchema.field_names(), order.GetIsReversed())
        , VersionColumns(batch, 0, versionColumnNames, {}, false)
        , RecordsCount(batch->num_rows())
        , ReverseSortKff(order.GetIsReversed() ? -1 : 1)
        , SourceId(sourceId)
        , Filter(filter) {
        AFL_VERIFY(KeyColumns.IsSameSortingSchema(keySchema))("batch", KeyColumns.DebugJson())("schema", keySchema.ToString());
        AFL_VERIFY(KeyColumns.IsSameDataSchema(dataSchema))("batch", KeyColumns.DebugJson())("schema", dataSchema.ToString());
        Y_ABORT_UNLESS(KeyColumns.InitPosition(GetPosition(order.GetStart())));
        Y_ABORT_UNLESS(VersionColumns.InitPosition(GetPosition(order.GetStart())));
        if (Filter) {
            FilterIterator =
                std::make_shared<NArrow::TColumnFilter::TIterator>(Filter->GetIterator(order.GetIsReversed(), RecordsCount, order.GetStart()));
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
