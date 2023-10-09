#include "read_filter_merger.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap::NIndexedReader {

void TMergePartialStream::PutControlPoint(std::shared_ptr<TSortableBatchPosition> point) {
    Y_ABORT_UNLESS(point);
    Y_ABORT_UNLESS(point->IsSameSortingSchema(SortSchema));
    Y_ABORT_UNLESS(point->IsReverseSort() == Reverse);
    Y_ABORT_UNLESS(++ControlPoints == 1);

    SortHeap.emplace_back(TBatchIterator(*point));
    std::push_heap(SortHeap.begin(), SortHeap.end());
}

void TMergePartialStream::AddPoolSource(const std::optional<ui32> poolId, std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter) {
    if (!batch || !batch->num_rows()) {
        return;
    }
    Y_VERIFY_DEBUG(NArrow::IsSorted(batch, SortSchema));
    if (!poolId) {
        AddNewToHeap(poolId, batch, filter, true);
    } else {
        auto it = BatchPools.find(*poolId);
        if (it == BatchPools.end()) {
            it = BatchPools.emplace(*poolId, std::deque<TIteratorData>()).first;
        }
        it->second.emplace_back(batch, filter);
        if (it->second.size() == 1) {
            AddNewToHeap(poolId, batch, filter, true);
        }
    }
}

void TMergePartialStream::AddNewToHeap(const std::optional<ui32> poolId, std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter, const bool restoreHeap) {
    if (!filter || filter->IsTotalAllowFilter()) {
        SortHeap.emplace_back(TBatchIterator(batch, nullptr, SortSchema->field_names(), DataSchema ? DataSchema->field_names() : std::vector<std::string>(), Reverse, poolId));
    } else if (filter->IsTotalDenyFilter()) {
        return;
    } else {
        SortHeap.emplace_back(TBatchIterator(batch, filter, SortSchema->field_names(), DataSchema ? DataSchema->field_names() : std::vector<std::string>(), Reverse, poolId));
    }
    if (restoreHeap) {
        std::push_heap(SortHeap.begin(), SortHeap.end());
    }
}

void TMergePartialStream::RemoveControlPoint() {
    Y_ABORT_UNLESS(ControlPoints == 1);
    Y_ABORT_UNLESS(ControlPointEnriched());
    Y_ABORT_UNLESS(-- ControlPoints == 0);
    std::pop_heap(SortHeap.begin(), SortHeap.end());
    SortHeap.pop_back();
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
    PutControlPoint(std::make_shared<TSortableBatchPosition>(readTo));
    bool cpReachedFlag = false;
    while (SortHeap.size() && !cpReachedFlag) {
        if (SortHeap.front().IsControlPoint()) {
            RemoveControlPoint();
            cpReachedFlag = true;
            if (SortHeap.empty() || !includeFinish || SortHeap.front().GetKeyColumns().Compare(readTo) == std::partial_ordering::greater) {
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

bool TMergePartialStream::DrainAll(TRecordBatchBuilder& builder) {
    Y_ABORT_UNLESS((ui32)DataSchema->num_fields() == builder.GetBuildersCount());
    while (SortHeap.size()) {
        if (auto currentPosition = DrainCurrentPosition()) {
            CheckSequenceInDebug(*currentPosition);
            builder.AddRecord(*currentPosition);
        }
    }
    return false;
}

std::optional<TSortableBatchPosition> TMergePartialStream::DrainCurrentPosition() {
    Y_ABORT_UNLESS(SortHeap.size());
    Y_ABORT_UNLESS(!SortHeap.front().IsControlPoint());
    TSortableBatchPosition result = SortHeap.front().GetKeyColumns();
    TSortableBatchPosition resultVersion = SortHeap.front().GetVersionColumns();
    bool isFirst = true;
    const bool deletedFlag = SortHeap.front().IsDeleted();
    while (SortHeap.size() && (isFirst || result.Compare(SortHeap.front().GetKeyColumns()) == std::partial_ordering::equivalent)) {
        auto& anotherIterator = SortHeap.front();
        if (!isFirst) {
            AFL_VERIFY(resultVersion.Compare(anotherIterator.GetVersionColumns()) == std::partial_ordering::greater)("r", resultVersion.DebugJson())("a", anotherIterator.GetVersionColumns().DebugJson())
                ("key", result.DebugJson());
        }
        NextInHeap(true);
        isFirst = false;
    }
    if (deletedFlag) {
        return {};
    }
    return result;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> TMergePartialStream::DrainAllParts(const std::vector<TSortableBatchPosition>& positions,
    const std::vector<std::shared_ptr<arrow::Field>>& resultFields, const bool includePositions)
{
    std::vector<std::shared_ptr<arrow::RecordBatch>> result;
    for (auto&& i : positions) {
        NIndexedReader::TRecordBatchBuilder indexesBuilder(resultFields);
        DrainCurrentTo(indexesBuilder, i, includePositions);
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
    if (PoolId) {
        result["pool_id"] = *PoolId;
    } else {
        result["pool_id"] = "absent";
    }
    result["key"] = KeyColumns.DebugJson();
    return result;
}

void TRecordBatchBuilder::AddRecord(const TSortableBatchPosition& position) {
    Y_VERIFY_DEBUG(position.GetData().GetColumns().size() == Builders.size());
    Y_VERIFY_DEBUG(IsSameFieldsSequence(position.GetData().GetFields(), Fields));
//    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "record_add_on_read")("record", position.DebugJson());
    for (ui32 i = 0; i < position.GetData().GetColumns().size(); ++i) {
        NArrow::Append(*Builders[i], *position.GetData().GetColumns()[i], position.GetPosition());
    }
    ++RecordsCount;
}

}
