#include "merger.h"
#include "result_builder.h"
#include <ydb/library/formats/arrow/permutations.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NArrow::NMerger {

void TMergePartialStream::PutControlPoint(const TSortableBatchPosition& point, const bool deepCopy) {
    AFL_VERIFY(point.IsSameSortingSchema(*SortSchema))("point", point.DebugJson())("schema", SortSchema->ToString());
    Y_ABORT_UNLESS(point.IsReverseSort() == Reverse);
    Y_ABORT_UNLESS(++ControlPoints == 1);

    SortHeap.Push(TBatchIterator(point.BuildRWPosition(false, deepCopy)));
}

void TMergePartialStream::RemoveControlPoint() {
    Y_ABORT_UNLESS(ControlPoints == 1);
    Y_ABORT_UNLESS(ControlPointEnriched());
    Y_ABORT_UNLESS(--ControlPoints == 0);
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
            SortHeap.Current().GetFilter()->Apply(result, TColumnFilter::TApplyContext(pos.GetPosition() + (include ? 0 : 1), resultSize));
        }
    } else {
        result = SortHeap.Current().GetKeyColumns().SliceData(startPos, resultSize);
        if (lastResultPosition && resultSize) {
            auto keys = SortHeap.Current().GetKeyColumns().SliceKeys(startPos, resultSize);
            *lastResultPosition = TCursor(keys, keys->num_rows() - 1, SortSchema->field_names());
        }
        if (SortHeap.Current().GetFilter()) {
            SortHeap.Current().GetFilter()->Apply(result, TColumnFilter::TApplyContext(startPos, resultSize));
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

std::vector<std::shared_ptr<arrow::RecordBatch>> TMergePartialStream::DrainAllParts(const TIntervalPositions& positions,
    const std::vector<std::shared_ptr<arrow::Field>>& resultFields)
{
    std::vector<std::shared_ptr<arrow::RecordBatch>> result;
    for (auto&& i : positions) {
        TRecordBatchBuilder indexesBuilder(resultFields);
        if (SortHeap.Empty() || i.GetPosition().Compare(SortHeap.Current().GetKeyColumns()) == std::partial_ordering::less) {
            continue;
        }
        DrainCurrentTo(indexesBuilder, i.GetPosition(), i.IsIncludedToLeftInterval());
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

void TMergePartialStream::SkipToBound(const TSortableBatchPosition& pos, const bool lower) {
    if (SortHeap.Empty()) {
        return;
    }
    AFL_DEBUG(NKikimrServices::ARROW_HELPER)("pos", pos.DebugJson().GetStringRobust())("heap", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
    while (!SortHeap.Empty()) {
        const auto cmpResult = SortHeap.Current().GetKeyColumns().Compare(pos);
        if (cmpResult == std::partial_ordering::greater) {
            break;
        }
        if (cmpResult == std::partial_ordering::equivalent && lower) {
            break;
        }
        const TSortableBatchPosition::TFoundPosition skipPos = SortHeap.MutableCurrent().SkipToLower(pos);
        AFL_DEBUG(NKikimrServices::ARROW_HELPER)("pos", pos.DebugJson().GetStringRobust())("heap", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
        if (skipPos.IsEqual()) {
            if (!lower && !SortHeap.MutableCurrent().Next()) {
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
