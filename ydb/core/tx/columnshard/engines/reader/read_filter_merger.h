#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/formats/arrow/reader/read_filter_merger.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/hash.h>
#include <util/string/join.h>
#include <set>

namespace NKikimr::NOlap::NIndexedReader {

class TRecordBatchBuilder;

class TMergePartialStream {
private:
#ifndef NDEBUG
    std::optional<TSortableBatchPosition> CurrentKeyColumns;
#endif
    class TBatchIterator {
    private:
        bool ControlPointFlag;
        TSortableBatchPosition KeyColumns;
        TSortableBatchPosition VersionColumns;
        i64 RecordsCount;
        int ReverseSortKff;
        YDB_OPT(ui32, PoolId);

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

        bool IsControlPoint() const {
            return ControlPointFlag;
        }

        const TSortableBatchPosition& GetKeyColumns() const {
            return KeyColumns;
        }

        const TSortableBatchPosition& GetVersionColumns() const {
            return VersionColumns;
        }

        TBatchIterator(const TSortableBatchPosition& keyColumns)
            : ControlPointFlag(true)
            , KeyColumns(keyColumns)
        {

        }

        TBatchIterator(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter,
            const std::vector<std::string>& keyColumns, const std::vector<std::string>& dataColumns, const bool reverseSort, const std::optional<ui32> poolId)
            : ControlPointFlag(false)
            , KeyColumns(batch, 0, keyColumns, dataColumns, reverseSort)
            , VersionColumns(batch, 0, TIndexInfo::GetSpecialColumnNames(), {}, false)
            , RecordsCount(batch->num_rows())
            , ReverseSortKff(reverseSort ? -1 : 1)
            , PoolId(poolId)
            , Filter(filter)
        {
            Y_VERIFY(KeyColumns.InitPosition(GetFirstPosition()));
            Y_VERIFY(VersionColumns.InitPosition(GetFirstPosition()));
            if (Filter) {
                FilterIterator = std::make_shared<NArrow::TColumnFilter::TIterator>(Filter->GetIterator(reverseSort, RecordsCount));
            }
        }

        bool CheckNextBatch(const TBatchIterator& nextIterator) {
            return KeyColumns.Compare(nextIterator.KeyColumns) == std::partial_ordering::less;
        }

        bool IsDeleted() const {
            if (!FilterIterator) {
                return false;
            }
            return !FilterIterator->GetCurrentAcceptance();
        }

        bool Next() {
            const bool result = KeyColumns.NextPosition(ReverseSortKff) && VersionColumns.NextPosition(ReverseSortKff);
            if (FilterIterator) {
                Y_VERIFY(result == FilterIterator->Next(1));
            }
            return result;
        }

        bool operator<(const TBatchIterator& item) const {
            const std::partial_ordering result = KeyColumns.Compare(item.KeyColumns);
            if (result == std::partial_ordering::equivalent) {
                if (IsControlPoint() && item.IsControlPoint()) {
                    return false;
                } else if (IsControlPoint()) {
                    return false;
                } else if (item.IsControlPoint()) {
                    return true;
                }
                //don't need inverse through we need maximal version at first (reverse analytic not included in VersionColumns)
                return VersionColumns.Compare(item.VersionColumns) == std::partial_ordering::less;
            } else {
                //inverse logic through we use max heap, but need minimal element if not reverse (reverse analytic included in KeyColumns)
                return result == std::partial_ordering::greater;
            }
        }
    };

    class TIteratorData {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Batch);
        YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);
    public:
        TIteratorData(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter)
            : Batch(batch)
            , Filter(filter)
        {

        }
    };

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
#ifndef NDEBUG
        if (CurrentKeyColumns) {
            result["current"] = CurrentKeyColumns->DebugJson();
        }
#endif
        for (auto&& i : SortHeap) {
            result["heap"].AppendValue(i.DebugJson());
        }
        return result;
    }

    bool NextInHeap(const bool needPop) {
        if (SortHeap.empty()) {
            return false;
        }
        if (needPop) {
            std::pop_heap(SortHeap.begin(), SortHeap.end());
        }
        if (SortHeap.back().Next()) {
            std::push_heap(SortHeap.begin(), SortHeap.end());
        } else if (!SortHeap.back().HasPoolId()) {
            SortHeap.pop_back();
        } else {
            auto it = BatchPools.find(SortHeap.back().GetPoolIdUnsafe());
            Y_VERIFY(it->second.size());
            if (it->second.size() == 1) {
                BatchPools.erase(it);
                SortHeap.pop_back();
            } else {
                it->second.pop_front();
                TBatchIterator oldIterator = std::move(SortHeap.back());
                SortHeap.pop_back();
                AddNewToHeap(SortHeap.back().GetPoolIdUnsafe(), it->second.front().GetBatch(), it->second.front().GetFilter(), false);
                oldIterator.CheckNextBatch(SortHeap.back());
                std::push_heap(SortHeap.begin(), SortHeap.end());
            }
        }
        return SortHeap.size();
    }

    THashMap<ui32, std::deque<TIteratorData>> BatchPools;
    std::vector<TBatchIterator> SortHeap;
    std::shared_ptr<arrow::Schema> SortSchema;
    std::shared_ptr<arrow::Schema> DataSchema;
    const bool Reverse;
    ui32 ControlPoints = 0;

    std::optional<TSortableBatchPosition> DrainCurrentPosition();

    void AddNewToHeap(const std::optional<ui32> poolId, std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter, const bool restoreHeap);
    void CheckSequenceInDebug(const TSortableBatchPosition& nextKeyColumnsPosition);
public:
    TMergePartialStream(std::shared_ptr<arrow::Schema> sortSchema, std::shared_ptr<arrow::Schema> dataSchema, const bool reverse)
        : SortSchema(sortSchema)
        , DataSchema(dataSchema)
        , Reverse(reverse) {
        Y_VERIFY(SortSchema);
        Y_VERIFY(SortSchema->num_fields());
        Y_VERIFY(!DataSchema || DataSchema->num_fields());
    }

    bool IsValid() const {
        return SortHeap.size();
    }

    bool HasRecordsInPool(const ui32 poolId) const {
        auto it = BatchPools.find(poolId);
        if (it == BatchPools.end()) {
            return false;
        }
        return it->second.size();
    }

    void PutControlPoint(std::shared_ptr<TSortableBatchPosition> point);

    void RemoveControlPoint();

    bool ControlPointEnriched() const {
        return SortHeap.size() && SortHeap.front().IsControlPoint();
    }

    void AddPoolSource(const std::optional<ui32> poolId, std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter);

    bool IsEmpty() const {
        return SortHeap.empty();
    }

    bool DrainAll(TRecordBatchBuilder& builder);
    bool DrainCurrentTo(TRecordBatchBuilder& builder, const TSortableBatchPosition& readTo, const bool includeFinish);
};

class TRecordBatchBuilder {
private:
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::Field>>, Fields);
    YDB_READONLY(ui32, RecordsCount, 0);

    bool IsSameFieldsSequence(const std::vector<std::shared_ptr<arrow::Field>>& f1, const std::vector<std::shared_ptr<arrow::Field>>& f2) {
        if (f1.size() != f2.size()) {
            return false;
        }
        for (ui32 i = 0; i < f1.size(); ++i) {
            if (!f1[i]->Equals(f2[i])) {
                return false;
            }
        }
        return true;
    }

public:
    ui32 GetBuildersCount() const {
        return Builders.size();
    }

    TString GetColumnNames() const {
        TStringBuilder result;
        for (auto&& f : Fields) {
            result << f->name() << ",";
        }
        return result;
    }

    TRecordBatchBuilder(const std::vector<std::shared_ptr<arrow::Field>>& fields)
        : Fields(fields) {
        Y_VERIFY(Fields.size());
        for (auto&& f : fields) {
            Builders.emplace_back(NArrow::MakeBuilder(f));
        }
    }

    std::shared_ptr<arrow::RecordBatch> Finalize() {
        auto schema = std::make_shared<arrow::Schema>(Fields);
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (auto&& i : Builders) {
            columns.emplace_back(NArrow::TStatusValidator::GetValid(i->Finish()));
        }
        return arrow::RecordBatch::Make(schema, columns.front()->length(), columns);
    }

    void AddRecord(const TSortableBatchPosition& position);
};

}
