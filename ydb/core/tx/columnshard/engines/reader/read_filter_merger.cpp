#include "read_filter_merger.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap::NIndexedReader {

bool TSortableBatchPosition::IsSameSchema(const std::shared_ptr<arrow::Schema> schema) const {
    if (Fields.size() != (size_t)schema->num_fields()) {
        return false;
    }
    for (ui32 i = 0; i < Fields.size(); ++i) {
        if (Fields[i]->type() != schema->field(i)->type()) {
            return false;
        }
        if (Fields[i]->name() != schema->field(i)->name()) {
            return false;
        }
    }
    return true;
}

NJson::TJsonValue TSortableBatchPosition::DebugJson() const {
    NJson::TJsonValue result;
    result["reverse"] = ReverseSort;
    result["records_count"] = RecordsCount;
    result["position"] = Position;
    Y_VERIFY(Columns.size() == Fields.size());
    for (ui32 i = 0; i < Columns.size(); ++i) {
        auto& jsonColumn = result["columns"].AppendValue(NJson::JSON_MAP);
        jsonColumn["name"] = Fields[i]->name();
        jsonColumn["value"] = NArrow::DebugString(Columns[i], Position);
    }
    return result;
}

std::optional<ui64> TSortableBatchPosition::FindPosition(std::shared_ptr<arrow::RecordBatch> batch, const TSortableBatchPosition& forFound, const bool greater, const bool include) {
    if (!batch || !batch->num_rows()) {
        return {};
    }

    const auto checkEqualBorder = [batch, greater, include](const i64 position) ->std::optional<i64> {
        if (include) {
            return position;
        } else if (greater) {
            if (batch->num_rows() > position + 1) {
                return position + 1;
            } else {
                return {};
            }
        } else {
            if (position) {
                return position - 1;
            } else {
                return {};
            }
        }
    };

    i64 posStart = 0;
    i64 posFinish = batch->num_rows() - 1;
    TSortableBatchPosition position = forFound.BuildSame(batch, posStart);
    {
        position.InitPosition(posStart);
        auto cmp = position.Compare(forFound);
        if (cmp == std::partial_ordering::greater) {
            if (greater) {
                return posStart;
            } else {
                return {};
            }
        } else if (cmp == std::partial_ordering::equivalent) {
            return checkEqualBorder(posStart);
        }
    }
    {
        position.InitPosition(posFinish);
        auto cmp = position.Compare(forFound);
        if (cmp == std::partial_ordering::less) {
            if (greater) {
                return {};
            } else {
                return posFinish;
            }
        } else if (cmp == std::partial_ordering::equivalent) {
            return checkEqualBorder(posFinish);
        }
    }
    while (posFinish > posStart + 1) {
        Y_VERIFY(position.InitPosition(0.5 * (posStart + posFinish)));
        const auto comparision = position.Compare(forFound);
        if (comparision == std::partial_ordering::less) {
            posStart = position.Position;
        } else if (comparision == std::partial_ordering::greater) {
            posFinish = position.Position;
        } else {
            return checkEqualBorder(position.Position);
        }
    }
    Y_VERIFY(posFinish != posStart);
    if (greater) {
        return posFinish;
    } else {
        return posStart;
    }
}

void TMergePartialStream::PutControlPoint(std::shared_ptr<TSortableBatchPosition> point) {
    Y_VERIFY(point);
    Y_VERIFY(point->IsSameSchema(SortSchema));
    Y_VERIFY(++ControlPoints == 1);

    SortHeap.emplace_back(TBatchIterator(*point));
    std::push_heap(SortHeap.begin(), SortHeap.end());
}

void TMergePartialStream::AddPoolSource(const std::optional<ui32> poolId, std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NArrow::TColumnFilter> filter) {
    if (!batch || !batch->num_rows() || (filter && filter->IsTotalDenyFilter())) {
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
        SortHeap.emplace_back(TBatchIterator(batch, nullptr, SortSchema->field_names(), Reverse, poolId));
    } else if (filter->IsTotalDenyFilter()) {
        return;
    } else {
        SortHeap.emplace_back(TBatchIterator(batch, filter, SortSchema->field_names(), Reverse, poolId));
    }
    if (restoreHeap) {
        std::push_heap(SortHeap.begin(), SortHeap.end());
    }
}

void TMergePartialStream::RemoveControlPoint() {
    Y_VERIFY(ControlPoints == 1);
    Y_VERIFY(ControlPointEnriched());
    Y_VERIFY(-- ControlPoints == 0);
    std::pop_heap(SortHeap.begin(), SortHeap.end());
    SortHeap.pop_back();
}

bool TMergePartialStream::DrainCurrent() {
    if (SortHeap.empty()) {
        return false;
    }
    while (SortHeap.size()) {
        auto currentPosition = DrainCurrentPosition();
        if (currentPosition.IsControlPoint()) {
            return false;
        }
        if (currentPosition.IsDeleted()) {
            continue;
        }
        if (CurrentKeyColumns) {
            Y_VERIFY(CurrentKeyColumns->Compare(currentPosition.GetKeyColumns()) != std::partial_ordering::greater);
        }
        CurrentKeyColumns = currentPosition.GetKeyColumns();
        return true;
    }
    return false;
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

}
