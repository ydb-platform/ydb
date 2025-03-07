#pragma once
#include "position.h"
#include "heap.h"
#include "batch_iterator.h"
#include "heap.h"
#include "position.h"

#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NArrow::NMerger {

class IMergeResultBuilder {
public:
    virtual void AddRecord(const TBatchIterator& cursor) = 0;
    virtual void SkipRecord(const TBatchIterator& cursor) = 0;
    virtual void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& schema) const = 0;
    virtual bool IsBufferExhausted() const = 0;

    virtual ~IMergeResultBuilder() = default;
};

class TMergePartialStream {
private:
#ifndef NDEBUG
    std::optional<TCursor> CurrentKeyColumns;
#endif
    bool PossibleSameVersionFlag = true;

    std::shared_ptr<arrow::Schema> SortSchema;
    std::shared_ptr<arrow::Schema> DataSchema;
    const bool Reverse;
    const std::vector<std::string> VersionColumnNames;
    std::optional<TCursor> MaxVersion;
    ui32 ControlPoints = 0;

    TSortingHeap<TBatchIterator> SortHeap;

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
#ifndef NDEBUG
        if (CurrentKeyColumns) {
            result["current"] = CurrentKeyColumns->DebugJson();
        }
#endif
        result.InsertValue("heap", SortHeap.DebugJson());
        return result;
    }

    void DrainCurrentPosition(IMergeResultBuilder* builder, std::shared_ptr<TSortableScanData>* resultScanData, ui64* resultPosition);

    void CheckSequenceInDebug(const TRWSortableBatchPosition& nextKeyColumnsPosition);
    bool DrainCurrentTo(IMergeResultBuilder& builder, const TSortableBatchPosition& readTo, const bool includeFinish,
        std::optional<TCursor>* lastResultPosition = nullptr);

public:
    TMergePartialStream(std::shared_ptr<arrow::Schema> sortSchema, std::shared_ptr<arrow::Schema> dataSchema, const bool reverse,
        const std::vector<std::string>& versionColumnNames, const std::optional<TCursor>& maxVersion)
        : SortSchema(sortSchema)
        , DataSchema(dataSchema)
        , Reverse(reverse)
        , VersionColumnNames(versionColumnNames)
        , MaxVersion(maxVersion) {
        Y_ABORT_UNLESS(SortSchema);
        Y_ABORT_UNLESS(SortSchema->num_fields());
        Y_ABORT_UNLESS(!DataSchema || DataSchema->num_fields());
    }

    void PutControlPoint(const TSortableBatchPosition& point, const bool deepCopy);
    void SkipToBound(const TSortableBatchPosition& pos, const bool lower);

    void SetPossibleSameVersion(const bool value) {
        PossibleSameVersionFlag = value;
    }

    bool IsValid() const {
        return SortHeap.Size();
    }

    ui32 GetSourcesCount() const {
        return SortHeap.Size();
    }

    TString DebugString() const {
        return TStringBuilder() << "sort_heap=" << SortHeap.DebugJson();
    }

    void RemoveControlPoint();

    bool ControlPointEnriched() const {
        return SortHeap.Size() && SortHeap.Current().IsControlPoint();
    }

    template <class TDataContainer>
    void AddSource(const std::shared_ptr<TDataContainer>& batch,
        const std::shared_ptr<NArrow::TColumnFilter>& filter, const TIterationOrder& order, const std::optional<ui64> sourceIdExt = std::nullopt) {
        AFL_VERIFY(order.GetIsReversed() == Reverse);
        const ui64 sourceId = sourceIdExt.value_or(SortHeap.Size());
        if (!batch || (i64)batch->num_rows() == (i64)order.GetStart()) {
            return;
        }
        AFL_VERIFY((i64)order.GetStart() < (i64)batch->num_rows())("start", order.GetStart())("num_rows", batch->num_rows());
        //        Y_DEBUG_ABORT_UNLESS(NArrow::IsSorted(batch, SortSchema));
        const bool isDenyFilter = filter && filter->IsTotalDenyFilter();
        auto filterImpl = (!filter || filter->IsTotalAllowFilter()) ? nullptr : filter;
        static const arrow::Schema emptySchema = arrow::Schema(arrow::FieldVector());
        TBatchIterator iterator(batch, filterImpl, *SortSchema, (!isDenyFilter && DataSchema) ? *DataSchema : emptySchema, VersionColumnNames,
            sourceId, TIterationOrder(Reverse, order.GetStart()));
        if (MaxVersion) {
            MaxVersion->ValidateSchema(*iterator.GetVersionColumns().GetSorting());
        }
        SortHeap.Push(std::move(iterator));
    }

    bool IsEmpty() const {
        return !SortHeap.Size();
    }

    void DrainAll(IMergeResultBuilder& builder);
    std::shared_ptr<arrow::Table> SingleSourceDrain(const TSortableBatchPosition& readTo, const bool includeFinish, std::optional<TCursor>* lastResultPosition = nullptr);
    bool DrainToControlPoint(IMergeResultBuilder& builder, const bool includeFinish, std::optional<TCursor>* lastResultPosition = nullptr);
    std::vector<std::shared_ptr<arrow::RecordBatch>> DrainAllParts(const TIntervalPositions& positions,
        const std::vector<std::shared_ptr<arrow::Field>>& resultFields);
};

}   // namespace NKikimr::NArrow::NMerger
