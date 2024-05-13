#include "merger.h"
#include "result_builder.h"
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NArrow::NMerger {

void TMergePartialStream::PutControlPoint(std::shared_ptr<TSortableBatchPosition> point) {
    Y_ABORT_UNLESS(point);
    AFL_VERIFY(point->IsSameSortingSchema(SortSchema))("point", point->DebugJson())("schema", SortSchema->ToString());
    Y_ABORT_UNLESS(point->IsReverseSort() == Reverse);
    Y_ABORT_UNLESS(++ControlPoints == 1);

    SortHeap.Push(TBatchIterator(*point));
}

void TMergePartialStream::RemoveControlPoint() {
    Y_ABORT_UNLESS(ControlPoints == 1);
    Y_ABORT_UNLESS(ControlPointEnriched());
    Y_ABORT_UNLESS(-- ControlPoints == 0);
    Y_ABORT_UNLESS(SortHeap.Current().IsControlPoint());
    SortHeap.RemoveTop();
}

void TMergePartialStream::CheckSequenceInDebug(const TSortableBatchPosition& nextKeyColumnsPosition) {
#ifndef NDEBUG
    if (CurrentKeyColumns) {
        const bool linearExecutionCorrectness = CurrentKeyColumns->Compare(nextKeyColumnsPosition) == std::partial_ordering::less;
        if (!linearExecutionCorrectness) {
            const bool newSegmentScan = nextKeyColumnsPosition.GetPosition() == 0;
            AFL_VERIFY(newSegmentScan && nextKeyColumnsPosition.Compare(*CurrentKeyColumns) == std::partial_ordering::less)
                ("merge_debug", DebugJson())("current_ext", nextKeyColumnsPosition.DebugJson())("newSegmentScan", newSegmentScan);
        }
    }
    CurrentKeyColumns = nextKeyColumnsPosition;
#else
    Y_UNUSED(nextKeyColumnsPosition);
#endif
}

bool TMergePartialStream::DrainToControlPoint(TRecordBatchBuilder& builder, const bool includeFinish, std::optional<TSortableBatchPosition>* lastResultPosition) {
    AFL_VERIFY(ControlPoints == 1);
    Y_ABORT_UNLESS((ui32)DataSchema->num_fields() == builder.GetBuildersCount());
    builder.ValidateDataSchema(DataSchema);
    bool cpReachedFlag = false;
    while (SortHeap.Size() && !cpReachedFlag && !builder.IsBufferExhausted()) {
        if (SortHeap.Current().IsControlPoint()) {
            auto keyColumns = SortHeap.Current().GetKeyColumns();
            RemoveControlPoint();
            cpReachedFlag = true;
            if (SortHeap.Empty() || !includeFinish || SortHeap.Current().GetKeyColumns().Compare(keyColumns) == std::partial_ordering::greater) {
                return true;
            }
        }

        if (auto currentPosition = DrainCurrentPosition()) {
            CheckSequenceInDebug(*currentPosition);
            builder.AddRecord(*currentPosition);
            if (lastResultPosition) {
                *lastResultPosition = *currentPosition;
            }
        }
    }
    return cpReachedFlag;
}

bool TMergePartialStream::DrainCurrentTo(TRecordBatchBuilder& builder, const TSortableBatchPosition& readTo, const bool includeFinish, std::optional<TSortableBatchPosition>* lastResultPosition) {
    PutControlPoint(std::make_shared<TSortableBatchPosition>(readTo));
    return DrainToControlPoint(builder, includeFinish, lastResultPosition);
}

std::shared_ptr<arrow::Table> TMergePartialStream::SingleSourceDrain(const TSortableBatchPosition& readTo, const bool includeFinish, std::optional<TSortableBatchPosition>* lastResultPosition) {
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
            *lastResultPosition = TSortableBatchPosition(keys, 0, SortSchema->field_names(), {}, true);
        }
        if (SortHeap.Current().GetFilter()) {
            SortHeap.Current().GetFilter()->Apply(result, pos.GetPosition() + (include ? 0 : 1), resultSize);
        }
    } else {
        result = SortHeap.Current().GetKeyColumns().SliceData(startPos, resultSize);
        if (lastResultPosition && resultSize) {
            auto keys = SortHeap.Current().GetKeyColumns().SliceKeys(startPos, resultSize);
            *lastResultPosition = TSortableBatchPosition(keys, keys->num_rows() - 1, SortSchema->field_names(), {}, false);
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
        if (auto currentPosition = DrainCurrentPosition()) {
            CheckSequenceInDebug(*currentPosition);
            builder.AddRecord(*currentPosition);
        }
    }
}

std::optional<TSortableBatchPosition> TMergePartialStream::DrainCurrentPosition() {
    Y_ABORT_UNLESS(SortHeap.Size());
    Y_ABORT_UNLESS(!SortHeap.Current().IsControlPoint());
    TSortableBatchPosition result = SortHeap.Current().GetKeyColumns();
    TSortableBatchPosition resultVersion = SortHeap.Current().GetVersionColumns();
    bool isFirst = true;
    const bool deletedFlag = SortHeap.Current().IsDeleted();
    while (SortHeap.Size() && (isFirst || result.Compare(SortHeap.Current().GetKeyColumns()) == std::partial_ordering::equivalent)) {
        auto& anotherIterator = SortHeap.Current();
        if (!isFirst) {
            if (PossibleSameVersionFlag) {
                AFL_VERIFY(resultVersion.Compare(anotherIterator.GetVersionColumns()) != std::partial_ordering::less)("r", resultVersion.DebugJson())("a", anotherIterator.GetVersionColumns().DebugJson())
                    ("key", result.DebugJson());
            } else {
                AFL_VERIFY(resultVersion.Compare(anotherIterator.GetVersionColumns()) == std::partial_ordering::greater)("r", resultVersion.DebugJson())("a", anotherIterator.GetVersionColumns().DebugJson())
                    ("key", result.DebugJson());
            }
        }
        SortHeap.Next();
        isFirst = false;
    }
    if (deletedFlag) {
        return {};
    }
    return result;
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
