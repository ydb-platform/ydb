#pragma once
#include "batch_iterator.h"
#include "heap.h"
#include "position.h"

#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NArrow::NMerger {

template <typename T>
concept MergeResultBuilder = requires(const T& constT, T& mutT, const std::shared_ptr<arrow::Schema>& schema, const TBatchIterator& cursor) {
    { constT.IsBufferExhausted() } -> std::same_as<bool>;
    { constT.ValidateDataSchema(schema) } -> std::same_as<void>;
    { mutT.AddRecord(cursor) } -> std::same_as<void>;
    { mutT.SkipRecord(cursor) } -> std::same_as<void>;
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

    template <MergeResultBuilder TBuilder>
    [[nodiscard]] bool DrainCurrentPosition(TBuilder* builder, std::shared_ptr<TSortableScanData>* resultScanData, ui64* resultPosition) {
        Y_ABORT_UNLESS(SortHeap.Size());
        Y_ABORT_UNLESS(!SortHeap.Current().IsControlPoint());
        CheckSequenceInDebug(SortHeap.Current().GetKeyColumns());

        const ui64 startPosition = SortHeap.Current().GetKeyColumns().GetPosition();
        const TSortableScanData* startSorting = SortHeap.Current().GetKeyColumns().GetSorting().get();
        const TSortableScanData* startVersion = SortHeap.Current().GetVersionColumns().GetSorting().get();

        if (MaxVersion) {
            bool skippedPk = false;
            while (SortHeap.Size() && !SortHeap.Current().IsControlPoint() &&
                   SortHeap.Current().GetVersionColumns().Compare(*MaxVersion) == std::partial_ordering::greater && !skippedPk) {
                if (builder) {
                    builder->SkipRecord(SortHeap.Current());
                }
                SortHeap.Next();
                if (SortHeap.Empty() ||
                    SortHeap.Current().GetKeyColumns().Compare(*startSorting, startPosition) != std::partial_ordering::equivalent) {
                    skippedPk = true;
                }
            }
            if (!SortHeap.Size() || SortHeap.Current().IsControlPoint() || skippedPk) {
                SortHeap.CleanFinished();
                return false;
            }
        }

        bool foundResult = false;
        if (!SortHeap.Current().IsDeleted()) {
            foundResult = true;
            //        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("key_add", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
            if (builder) {
                builder->AddRecord(SortHeap.Current());
            }
            if (resultScanData && resultPosition) {
                *resultScanData = SortHeap.Current().GetKeyColumns().GetSorting();
                *resultPosition = SortHeap.Current().GetKeyColumns().GetPosition();
            }
        } else {
            //        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("key_skip", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
            if (builder) {
                builder->SkipRecord(SortHeap.Current());
            }
        }
        SortHeap.Next();

        while (
            SortHeap.Size() && (SortHeap.Current().GetKeyColumns().Compare(*startSorting, startPosition) == std::partial_ordering::equivalent)) {
            if (builder) {
                builder->SkipRecord(SortHeap.Current());
            }
            //            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("key_skip1", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
            auto& anotherIterator = SortHeap.Current();
            if (PossibleSameVersionFlag) {
                AFL_VERIFY(anotherIterator.GetVersionColumns().Compare(*startVersion, startPosition) != std::partial_ordering::greater)
                ("r", startVersion->BuildCursor(startPosition).DebugJson())("a", anotherIterator.GetVersionColumns().DebugJson())(
                    "key", startSorting->BuildCursor(startPosition).DebugJson());
            } else {
                AFL_VERIFY(anotherIterator.GetVersionColumns().Compare(*startVersion, startPosition) == std::partial_ordering::less)
                ("r", startVersion->BuildCursor(startPosition).DebugJson())("a", anotherIterator.GetVersionColumns().DebugJson())(
                    "key", startSorting->BuildCursor(startPosition).DebugJson());
            }
            SortHeap.Next();
        }
        SortHeap.CleanFinished();
        return foundResult;
    }

    void CheckSequenceInDebug(const TRWSortableBatchPosition& nextKeyColumnsPosition);

    template <MergeResultBuilder TBuilder>
    bool DrainCurrentTo(TBuilder& builder, const TSortableBatchPosition& readTo, const bool includeFinish,
        std::optional<TCursor>* lastResultPosition = nullptr) {
        PutControlPoint(readTo, false);
        return DrainToControlPoint(builder, includeFinish, lastResultPosition);
    }

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
    void AddSource(const std::shared_ptr<TDataContainer>& batch, const std::shared_ptr<NArrow::TColumnFilter>& filter,
        const TIterationOrder& order, const std::optional<ui64> sourceIdExt = std::nullopt) {
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

    template <MergeResultBuilder TBuilder>
    void DrainAll(TBuilder& builder) {
        builder.ValidateDataSchema(DataSchema);
        while (SortHeap.Size()) {
            Y_UNUSED(DrainCurrentPosition(&builder, nullptr, nullptr));
        }
    }
    std::shared_ptr<arrow::Table> SingleSourceDrain(
        const TSortableBatchPosition& readTo, const bool includeFinish, std::optional<TCursor>* lastResultPosition = nullptr);
    std::vector<std::shared_ptr<arrow::RecordBatch>> DrainAllParts(
        const TIntervalPositions& positions, const std::vector<std::shared_ptr<arrow::Field>>& resultFields);

    template <MergeResultBuilder TBuilder>
    bool DrainToControlPoint(TBuilder& builder, const bool includeFinish, std::optional<TCursor>* lastResultPosition = nullptr) {
        AFL_VERIFY(ControlPoints == 1);
        builder.ValidateDataSchema(DataSchema);
        bool cpReachedFlag = false;
        std::shared_ptr<TSortableScanData> resultScanData;
        ui64 resultPosition;
        while (SortHeap.Size() && !cpReachedFlag && !builder.IsBufferExhausted()) {
            if (SortHeap.Current().IsControlPoint()) {
                auto keyColumns = SortHeap.Current().GetKeyColumns().BuildSortingCursor();
                RemoveControlPoint();
                cpReachedFlag = true;
                if (SortHeap.Empty() || !includeFinish ||
                    SortHeap.Current().GetKeyColumns().Compare(keyColumns) == std::partial_ordering::greater) {
                    if (lastResultPosition && resultScanData) {
                        *lastResultPosition = resultScanData->BuildCursor(resultPosition);
                    }
                    return true;
                }
            }
            Y_UNUSED(DrainCurrentPosition(&builder, &resultScanData, &resultPosition));
        }
        if (lastResultPosition && resultScanData) {
            *lastResultPosition = resultScanData->BuildCursor(resultPosition);
        }
        return cpReachedFlag;
    }
};

}   // namespace NKikimr::NArrow::NMerger
