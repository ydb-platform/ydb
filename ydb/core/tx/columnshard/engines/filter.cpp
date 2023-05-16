#include "filter.h"
#include "defs.h"
#include "indexed_read_data.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/custom_registry.h>

namespace NKikimr::NOlap {

class TSnapshotGetter {
private:
    const arrow::UInt64Array::value_type* RawSteps;
    const arrow::UInt64Array::value_type* RawIds;
    const TSnapshot Snapshot;
public:
    TSnapshotGetter(std::shared_ptr<arrow::Array> steps, std::shared_ptr<arrow::Array> ids, const TSnapshot& snapshot)
        : Snapshot(snapshot)
    {
        Y_VERIFY(steps);
        Y_VERIFY(ids);
        Y_VERIFY(steps->length() == ids->length());
        Y_VERIFY(steps->type() == arrow::uint64());
        Y_VERIFY(ids->type() == arrow::uint64());
        RawSteps = std::static_pointer_cast<arrow::UInt64Array>(steps)->raw_values();
        RawIds = std::static_pointer_cast<arrow::UInt64Array>(ids)->raw_values();
    }

    bool operator[](const ui32 idx) const {
        return std::less_equal<TSnapshot>()(TSnapshot(RawSteps[idx], RawIds[idx]), Snapshot);
    }
};

NArrow::TColumnFilter MakeSnapshotFilter(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     const std::shared_ptr<arrow::Schema>& snapSchema,
                                     const TSnapshot& snapshot) {
    Y_VERIFY(batch);
    Y_VERIFY(snapSchema);
    Y_VERIFY(snapSchema->num_fields() == 2);
    auto steps = batch->GetColumnByName(snapSchema->fields()[0]->name());
    auto ids = batch->GetColumnByName(snapSchema->fields()[1]->name());
    NArrow::TColumnFilter result;
    TSnapshotGetter getter(steps, ids, snapshot);
    result.Reset(steps->length(), std::move(getter));
    return result;
}

NArrow::TColumnFilter FilterPortion(const std::shared_ptr<arrow::RecordBatch>& portion, const TReadMetadata& readMetadata) {
    Y_VERIFY(portion);
    NArrow::TColumnFilter result = readMetadata.GetPKRangesFilter().BuildFilter(portion);
    if (readMetadata.GetSnapshot().GetPlanStep()) {
        auto snapSchema = TIndexInfo::ArrowSchemaSnapshot();
        result = result.And(MakeSnapshotFilter(portion, snapSchema, readMetadata.GetSnapshot()));
    }

    return result;
}

NArrow::TColumnFilter FilterNotIndexed(const std::shared_ptr<arrow::RecordBatch>& batch, const TReadMetadata& readMetadata) {
    return readMetadata.GetPKRangesFilter().BuildFilter(batch);
}

NArrow::TColumnFilter EarlyFilter(const std::shared_ptr<arrow::RecordBatch>& batch, std::shared_ptr<NSsa::TProgram> ssa) {
    return ssa->MakeEarlyFilter(batch, NArrow::GetCustomExecContext());
}

}
