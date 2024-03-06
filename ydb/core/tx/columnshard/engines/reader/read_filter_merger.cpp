#include "read_filter_merger.h"
#include <ydb/core/formats/arrow/permutations.h>
#include <library/cpp/actors/core/log.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_vector.h>

namespace NKikimr::NOlap::NIndexedReader {

void TMergePartialStream::PutControlPoint(std::shared_ptr<TSortableBatchPosition> point) {
    Y_ABORT_UNLESS(point);
    AFL_VERIFY(point->IsSameSortingSchema(SortSchema))("point", point->DebugJson())("schema", SortSchema->ToString());
    Y_ABORT_UNLESS(point->IsReverseSort() == Reverse);
    Y_ABORT_UNLESS(++ControlPoints == 1);

    SortHeap.Push(TBatchIterator(*point));
}

void TMergePartialStream::AddSource(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter) {
    if (!batch || !batch->num_rows()) {
        return;
    }
    Y_DEBUG_ABORT_UNLESS(NArrow::IsSorted(batch, SortSchema));
    AddNewToHeap(batch, filter);
}

void TMergePartialStream::AddNewToHeap(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter) {
    if (!filter || filter->IsTotalAllowFilter()) {
        SortHeap.Push(TBatchIterator(batch, nullptr, SortSchema->field_names(), DataSchema ? DataSchema->field_names() : std::vector<std::string>(), Reverse));
    } else if (filter->IsTotalDenyFilter()) {
        return;
    } else {
        SortHeap.Push(TBatchIterator(batch, filter, SortSchema->field_names(), DataSchema ? DataSchema->field_names() : std::vector<std::string>(), Reverse));
    }
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

bool TMergePartialStream::DrainCurrentTo(TRecordBatchBuilder& builder, const TSortableBatchPosition& readTo, const bool includeFinish) {
    Y_ABORT_UNLESS((ui32)DataSchema->num_fields() == builder.GetBuildersCount());
    builder.ValidateDataSchema(DataSchema);
    PutControlPoint(std::make_shared<TSortableBatchPosition>(readTo));
    bool cpReachedFlag = false;
    while (SortHeap.Size() && !cpReachedFlag) {
        if (SortHeap.Current().IsControlPoint()) {
            RemoveControlPoint();
            cpReachedFlag = true;
            if (SortHeap.Empty() || !includeFinish || SortHeap.Current().GetKeyColumns().Compare(readTo) == std::partial_ordering::greater) {
                return true;
            }
        }

        if (auto currentPosition = DrainCurrentPosition()) {
            CheckSequenceInDebug(*currentPosition);
            builder.AddRecord(*currentPosition);
        }
    }
    return false;
}

std::shared_ptr<arrow::RecordBatch> TMergePartialStream::SingleSourceDrain(const TSortableBatchPosition& readTo, const bool includeFinish) {
    std::shared_ptr<arrow::RecordBatch> result;
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
    if (Reverse) {
        result = SortHeap.Current().GetKeyColumns().Slice(pos.GetPosition() + (include ? 0 : 1), delta + (include ? 1 : 0));
    } else {
        result = SortHeap.Current().GetKeyColumns().Slice(startPos, delta + (include ? 1 : 0));
    }
    AFL_VERIFY(result);
#ifndef NDEBUG
    NArrow::TStatusValidator::Validate(result->ValidateFull());
#endif
    if (Reverse && result->num_rows()) {
        auto permutation = NArrow::MakePermutation(result->num_rows(), true);
        AFL_VERIFY(permutation);
        result = NArrow::TStatusValidator::GetValid(arrow::compute::Take(result, permutation)).record_batch();
    }

    if (finished) {
        SortHeap.RemoveTop();
    } else {
        SortHeap.UpdateTop();
    }
    if (SortHeap.Empty()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("pos", readTo.DebugJson().GetStringRobust())("heap", "EMPTY");
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("pos", readTo.DebugJson().GetStringRobust())("heap", SortHeap.Current().GetKeyColumns().DebugJson().GetStringRobust());
    }
    return result;
}

bool TMergePartialStream::DrainAll(TRecordBatchBuilder& builder) {
    Y_ABORT_UNLESS((ui32)DataSchema->num_fields() == builder.GetBuildersCount());
    while (SortHeap.Size()) {
        if (auto currentPosition = DrainCurrentPosition()) {
            CheckSequenceInDebug(*currentPosition);
            builder.AddRecord(*currentPosition);
        }
    }
    return false;
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
        NIndexedReader::TRecordBatchBuilder indexesBuilder(resultFields);
        DrainCurrentTo(indexesBuilder, i.first, i.second);
        result.emplace_back(indexesBuilder.Finalize());
        if (result.back()->num_rows() == 0) {
            result.pop_back();
        }
    }
    NIndexedReader::TRecordBatchBuilder indexesBuilder(resultFields);
    DrainAll(indexesBuilder);
    result.emplace_back(indexesBuilder.Finalize());
    if (result.back()->num_rows() == 0) {
        result.pop_back();
    }
    return result;
}

NJson::TJsonValue TMergePartialStream::TBatchIterator::DebugJson() const {
    NJson::TJsonValue result;
    result["is_cp"] = IsControlPoint();
    result["key"] = KeyColumns.DebugJson();
    return result;
}

void TRecordBatchBuilder::ValidateDataSchema(const std::shared_ptr<arrow::Schema>& schema) {
    AFL_VERIFY(IsSameFieldsSequence(schema->fields(), Fields));
}

void TRecordBatchBuilder::AddRecord(const TSortableBatchPosition& position) {
    Y_DEBUG_ABORT_UNLESS(position.GetData().GetColumns().size() == Builders.size());
    Y_DEBUG_ABORT_UNLESS(IsSameFieldsSequence(position.GetData().GetFields(), Fields));
//    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "record_add_on_read")("record", position.DebugJson());
    for (ui32 i = 0; i < position.GetData().GetColumns().size(); ++i) {
        NArrow::Append(*Builders[i], *position.GetData().GetColumns()[i], position.GetPosition());
    }
    ++RecordsCount;
}

}
