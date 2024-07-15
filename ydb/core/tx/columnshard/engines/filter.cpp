#include "filter.h"
#include "defs.h"
#include "scheme/abstract/index_info.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/custom_registry.h>
#include <ydb/core/formats/arrow/program.h>

namespace NKikimr::NOlap {

template <class TArrayView>
class TChunkedArrayIterator {
private:
    const arrow::ArrayVector* Chunks;
    const typename TArrayView::value_type* RawView;
    arrow::ArrayVector::const_iterator CurrentChunkIt;
    ui32 CurrentChunkPosition = 0;
public:
    TChunkedArrayIterator(const std::shared_ptr<arrow::ChunkedArray>& chunks)
    {
        AFL_VERIFY(!!chunks);
        Chunks = &chunks->chunks();
        AFL_VERIFY(Chunks->size());
        CurrentChunkIt = Chunks->begin();
        Y_ABORT_UNLESS(chunks->type()->id() == arrow::TypeTraits<typename TArrayView::TypeClass>::type_singleton()->id());
        if (IsValid()) {
            RawView = std::static_pointer_cast<TArrayView>(*CurrentChunkIt)->raw_values();
        }
    }

    bool IsValid() const {
        return CurrentChunkIt != Chunks->end() && CurrentChunkPosition < (*CurrentChunkIt)->length();
    }

    typename TArrayView::value_type GetValue() const {
        AFL_VERIFY_DEBUG(IsValid());
        return RawView[CurrentChunkPosition];
    }

    bool Next() {
        AFL_VERIFY_DEBUG(IsValid());
        ++CurrentChunkPosition;
        while (CurrentChunkIt != Chunks->end() && (*CurrentChunkIt)->length() == CurrentChunkPosition) {
            if (++CurrentChunkIt != Chunks->end()) {
                CurrentChunkPosition = 0;
                RawView = std::static_pointer_cast<TArrayView>(*CurrentChunkIt)->raw_values();
            }
        }
        return CurrentChunkIt != Chunks->end();
    }
};

class TTableSnapshotGetter {
private:
    const TSnapshot Snapshot;
    mutable TChunkedArrayIterator<arrow::UInt64Array> Steps;
    mutable TChunkedArrayIterator<arrow::UInt64Array> Ids;
    mutable i64 CurrentIdx = -1;
public:
    TTableSnapshotGetter(const std::shared_ptr<arrow::ChunkedArray>& steps, const std::shared_ptr<arrow::ChunkedArray>& ids, const TSnapshot& snapshot)
        : Snapshot(snapshot)
        , Steps(steps)
        , Ids(ids)
    {
        Y_ABORT_UNLESS(steps->length() == ids->length());
    }

    bool operator[](const ui32 idx) const {
        AFL_VERIFY(CurrentIdx + 1 == idx)("current_idx", CurrentIdx)("idx", idx);
        CurrentIdx = idx;
        const bool result = std::less_equal<TSnapshot>()(TSnapshot(Steps.GetValue(), Ids.GetValue()), Snapshot);
        const bool sNext = Steps.Next();
        const bool idNext = Ids.Next();
        AFL_VERIFY(sNext == idNext);
        if (!idNext) {
            CurrentIdx = -2;
        }
        return result;
    }
};

template <class TGetter, class TData>
NArrow::TColumnFilter MakeSnapshotFilterImpl(const std::shared_ptr<TData>& batch, const TSnapshot& snapshot) {
    Y_ABORT_UNLESS(batch);
    auto steps = batch->GetColumnByName(IIndexInfo::SPEC_COL_PLAN_STEP);
    auto ids = batch->GetColumnByName(IIndexInfo::SPEC_COL_TX_ID);
    NArrow::TColumnFilter result = NArrow::TColumnFilter::BuildAllowFilter();
    TGetter getter(steps, ids, snapshot);
    result.Reset(steps->length(), std::move(getter));
    return result;
}

NArrow::TColumnFilter MakeSnapshotFilter(const std::shared_ptr<arrow::Table>& batch,
    const TSnapshot& snapshot) {
    return MakeSnapshotFilterImpl<TTableSnapshotGetter>(batch, snapshot);
}

class TSnapshotGetter {
private:
    const arrow::UInt64Array::value_type* RawSteps;
    const arrow::UInt64Array::value_type* RawIds;
    const TSnapshot Snapshot;
public:
    TSnapshotGetter(std::shared_ptr<arrow::Array> steps, std::shared_ptr<arrow::Array> ids, const TSnapshot& snapshot)
        : Snapshot(snapshot)
    {
        Y_ABORT_UNLESS(steps);
        Y_ABORT_UNLESS(ids);
        Y_ABORT_UNLESS(steps->length() == ids->length());
        Y_ABORT_UNLESS(steps->type() == arrow::uint64());
        Y_ABORT_UNLESS(ids->type() == arrow::uint64());
        RawSteps = std::static_pointer_cast<arrow::UInt64Array>(steps)->raw_values();
        RawIds = std::static_pointer_cast<arrow::UInt64Array>(ids)->raw_values();
    }

    bool operator[](const ui32 idx) const {
        return std::less_equal<TSnapshot>()(TSnapshot(RawSteps[idx], RawIds[idx]), Snapshot);
    }
};

NArrow::TColumnFilter MakeSnapshotFilter(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     const TSnapshot& snapshot) {
    return MakeSnapshotFilterImpl<TSnapshotGetter>(batch, snapshot);
}

}
