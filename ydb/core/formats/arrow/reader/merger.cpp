#include "merger.h"
#include "result_builder.h"
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NArrow::NMerger {

void TMergePartialStream::PutControlPoint(const TSortableBatchPosition& point) {
    AFL_VERIFY(point.IsSameSortingSchema(SortSchema))("point", point.DebugJson())("schema", SortSchema->ToString());
    Y_ABORT_UNLESS(point.IsReverseSort() == Reverse);
    Y_ABORT_UNLESS(++ControlPoints == 1);

    SortHeap.Push(TBatchIterator(point.BuildRWPosition()));
}

void TMergePartialStream::RemoveControlPoint() {
    Y_ABORT_UNLESS(ControlPoints == 1);
    Y_ABORT_UNLESS(ControlPointEnriched());
    Y_ABORT_UNLESS(-- ControlPoints == 0);
    Y_ABORT_UNLESS(SortHeap.Current().IsControlPoint());
    SortHeap.RemoveTop();
}

void TMergePartialStream::CheckSequenceInDebug(const TRWSortableBatchPosition& nextKeyColumnsPosition) {
#ifndef NDEBUG
    if (CurrentKeyColumns) {
        const bool linearExecutionCorrectness = nextKeyColumnsPosition.Compare(*CurrentKeyColumns) == std::partial_ordering::greater;
        if (!linearExecutionCorrectness) {
            const bool newSegmentScan = nextKeyColumnsPosition.GetPosition() == 0;
            AFL_VERIFY(newSegmentScan && nextKeyColumnsPosition.Compare(*CurrentKeyColumns) == std::partial_ordering::less)
                ("merge_debug", DebugJson())("current_ext", nextKeyColumnsPosition.DebugJson())("newSegmentScan", newSegmentScan);
        }
    }
    CurrentKeyColumns = nextKeyColumnsPosition.BuildSortingCursor();
#else
    Y_UNUSED(nextKeyColumnsPosition);
#endif
}

bool TMergePartialStream::DrainToControlPoint(TRecordBatchBuilder& builder, const bool includeFinish, std::optional<TCursor>* lastResultPosition) {
    AFL_VERIFY(ControlPoints == 1);
    Y_ABORT_UNLESS((ui32)DataSchema->num_fields() == builder.GetBuildersCount());
    builder.ValidateDataSchema(DataSchema);
    bool cpReachedFlag = false;
    std::shared_ptr<TSortableScanData> resultScanData;
    ui64 resultPosition;
    while (SortHeap.Size() && !cpReachedFlag && !builder.IsBufferExhausted()) {
        if (SortHeap.Current().IsControlPoint()) {
            auto keyColumns = SortHeap.Current().GetKeyColumns().BuildSortingCursor();
            RemoveControlPoint();
            cpReachedFlag = true;
            if (SortHeap.Empty() || !includeFinish || SortHeap.Current().GetKeyColumns().Compare(keyColumns) == std::partial_ordering::greater) {
                if (lastResultPosition && resultScanData) {
                    *lastResultPosition = resultScanData->BuildCursor(resultPosition);
                }
                return true;
            }
        }

        DrainCurrentPosition(&builder, &resultScanData, &resultPosition);
    }
    if (lastResultPosition && resultScanData) {
        *lastResultPosition = resultScanData->BuildCursor(resultPosition);
    }
    return cpReachedFlag;
}

bool TMergePartialStream::DrainCurrentTo(TRecordBatchBuilder& builder, const TSortableBatchPosition& readTo, const bool includeFinish, std::optional<TCursor>* lastResultPosition) {
    PutControlPoint(readTo);
    return DrainToControlPoint(builder, includeFinish, lastResultPosition);
}

std::shared_ptr<arrow::Table> TMergePartialStream::SingleSourceDrain(const TSortableBatchPosition& readTo, const bool includeFinish, std::optional<TCursor>* lastResultPosition) {
    std::shared_ptr<arrow::Table> result;
    if (SortHeap.Empty()) {
        return result;
    }
    const ui32 startPos = SortHeap.Current().GetKeyColumns().GetPosition();
    const TSortableBatchPosition::TFoundPosition pos = SortHeap.MutableCurrent().SkipToLower(readTo);
    bool finished = false;
    const i32 delta = Reverse ? startPos - pos.GetPosition() : pos.GetPosition() - startPos;
    if (delta == 0 && pos.IsGreater()) {
        return nullptr;
    }
    bool include = false;
    AFL_VERIFY(delta >= 0);
    if (pos.IsEqual()) {
        if (includeFinish) {
            finished = !SortHeap.MutableCurrent().Next();
            include = true;
        } else {
            finished = false;
        }
    } else if (pos.IsGreater()) {
        finished = false;
    } else {
        finished = true;
        include = true;
    }
    const ui32 resultSize = delta + (include ? 1 : 0);
    if (Reverse) {
        result = SortHeap.Current().GetKeyColumns().SliceData(pos.GetPosition() + (include ? 0 : 1), resultSize);
        if (lastResultPosition && resultSize) {
            auto keys = SortHeap.Current().GetKeyColumns().SliceKeys(pos.GetPosition() + (include ? 0 : 1), resultSize);
            *lastResultPosition = TCursor(keys, 0, SortSchema->field_names());
        }
        if (SortHeap.Current().GetFilter()) {
            SortHeap.Current().GetFilter()->Apply(result, pos.GetPosition() + (include ? 0 : 1), resultSize);
        }
    } else {
        result = SortHeap.Current().GetKeyColumns().SliceData(startPos, resultSize);
        if (lastResultPosition && resultSize) {
            auto keys = SortHeap.Current().GetKeyColumns().SliceKeys(startPos, resultSize);
            *lastResultPosition = TCursor(keys, keys->num_rows() - 1, SortSchema->field_names());
        }
        if (SortHeap.Current().GetFilter()) {
            SortHeap.Current().GetFilter()->Apply(result, startPos, resultSize);
        }
    }
    if (!result || !result->num_rows()) {
        if (lastResultPosition) {
            *lastResultPosition = {};
        }
    }
#ifndef NDEBUG
    NArrow::TStatusValidator::Validate(result->ValidateFull());
#endif

    if (Reverse) {
        result = NArrow::ReverseRecords(result);
    }

    if (finished) {
        SortHeap.RemoveTop();
    } else {
        SortHeap.UpdateTop();
    }
    if (SortHeap.Empty()) {
        AFL_DEBUG(NKikimrServices::ARROW_HELPER)("pos", readTo.DebugJson().GetStringRobust())("heap", "EMPTY");
    } else {
        AFL_DEBUG(NKikimrServices::ARROW_HELPER)("pos", readTo.DebugJson().GetStringRobust())("heap", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
    }
    return result;
}

void TMergePartialStream::DrainAll(TRecordBatchBuilder& builder) {
    Y_ABORT_UNLESS((ui32)DataSchema->num_fields() == builder.GetBuildersCount());
    while (SortHeap.Size()) {
        DrainCurrentPosition(&builder, nullptr, nullptr);
    }
}

void TMergePartialStream::DrainCurrentPosition(TRecordBatchBuilder* builder, std::shared_ptr<TSortableScanData>* resultScanData, ui64* resultPosition) {
    Y_ABORT_UNLESS(SortHeap.Size());
    Y_ABORT_UNLESS(!SortHeap.Current().IsControlPoint());
    if (!SortHeap.Current().IsDeleted()) {
        if (builder) {
            builder->AddRecord(SortHeap.Current().GetKeyColumns());
        }
        if (resultScanData && resultPosition) {
            *resultScanData = SortHeap.Current().GetKeyColumns().GetSorting();
            *resultPosition = SortHeap.Current().GetKeyColumns().GetPosition();
        }
    }
    CheckSequenceInDebug(SortHeap.Current().GetKeyColumns());
    const ui64 startPosition = SortHeap.Current().GetKeyColumns().GetPosition();
    const TSortableScanData* startSorting = SortHeap.Current().GetKeyColumns().GetSorting().get();
    const TSortableScanData* startVersion = SortHeap.Current().GetVersionColumns().GetSorting().get();
    bool isFirst = true;
    while (SortHeap.Size() && (isFirst || SortHeap.Current().GetKeyColumns().Compare(*startSorting, startPosition) == std::partial_ordering::equivalent)) {
        if (!isFirst) {
            auto& anotherIterator = SortHeap.Current();
            if (PossibleSameVersionFlag) {
                AFL_VERIFY(anotherIterator.GetVersionColumns().Compare(*startVersion, startPosition) != std::partial_ordering::greater)
                    ("r", startVersion->BuildCursor(startPosition).DebugJson())("a", anotherIterator.GetVersionColumns().DebugJson())
                    ("key", startSorting->BuildCursor(startPosition).DebugJson());
            } else {
                AFL_VERIFY(anotherIterator.GetVersionColumns().Compare(*startVersion, startPosition) == std::partial_ordering::less)
                    ("r", startVersion->BuildCursor(startPosition).DebugJson())("a", anotherIterator.GetVersionColumns().DebugJson())
                    ("key", startSorting->BuildCursor(startPosition).DebugJson());
            }
        }
        SortHeap.Next();
        isFirst = false;
    }
    SortHeap.CleanFinished();
}

std::vector<std::shared_ptr<arrow::RecordBatch>> TMergePartialStream::DrainAllParts(const std::map<TSortableBatchPosition, bool>& positions,
    const std::vector<std::shared_ptr<arrow::Field>>& resultFields)
{
    std::vector<std::shared_ptr<arrow::RecordBatch>> result;
    for (auto&& i : positions) {
        TRecordBatchBuilder indexesBuilder(resultFields);
        DrainCurrentTo(indexesBuilder, i.first, i.second);
        result.emplace_back(indexesBuilder.Finalize());
        if (result.back()->num_rows() == 0) {
            result.pop_back();
        }
    }
    TRecordBatchBuilder indexesBuilder(resultFields);
    DrainAll(indexesBuilder);
    result.emplace_back(indexesBuilder.Finalize());
    if (result.back()->num_rows() == 0) {
        result.pop_back();
    }
    return result;
}

void TMergePartialStream::SkipToLowerBound(const TSortableBatchPosition& pos, const bool include) {
    if (SortHeap.Empty()) {
        return;
    }
    AFL_DEBUG(NKikimrServices::ARROW_HELPER)("pos", pos.DebugJson().GetStringRobust())("heap", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
    while (!SortHeap.Empty()) {
        const auto cmpResult = SortHeap.Current().GetKeyColumns().Compare(pos);
        if (cmpResult == std::partial_ordering::greater) {
            break;
        }
        if (cmpResult == std::partial_ordering::equivalent && include) {
            break;
        }
        const TSortableBatchPosition::TFoundPosition skipPos = SortHeap.MutableCurrent().SkipToLower(pos);
        AFL_DEBUG(NKikimrServices::ARROW_HELPER)("pos", pos.DebugJson().GetStringRobust())("heap", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
        if (skipPos.IsEqual()) {
            if (!include && !SortHeap.MutableCurrent().Next()) {
                SortHeap.RemoveTop();
            } else {
                SortHeap.UpdateTop();
            }
        } else if (skipPos.IsLess()) {
            SortHeap.RemoveTop();
        } else {
            SortHeap.UpdateTop();
        }
    }
}

}
