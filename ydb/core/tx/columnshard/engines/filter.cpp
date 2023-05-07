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
    const ui64 PlanStep;
    const ui64 TxId;
public:
    TSnapshotGetter(std::shared_ptr<arrow::Array> steps, std::shared_ptr<arrow::Array> ids, const ui64 planStep, const ui64 txId)
        : PlanStep(planStep)
        , TxId(txId)
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
        return SnapLessOrEqual(RawSteps[idx], RawIds[idx], PlanStep, TxId);
    }
};

NArrow::TColumnFilter MakeSnapshotFilter(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     const std::shared_ptr<arrow::Schema>& snapSchema,
                                     ui64 planStep, ui64 txId) {
    Y_VERIFY(batch);
    Y_VERIFY(snapSchema);
    Y_VERIFY(snapSchema->num_fields() == 2);
    auto steps = batch->GetColumnByName(snapSchema->fields()[0]->name());
    auto ids = batch->GetColumnByName(snapSchema->fields()[1]->name());
    NArrow::TColumnFilter result;
    TSnapshotGetter getter(steps, ids, planStep, txId);
    result.Reset(steps->length(), std::move(getter));
    return result;
}

NArrow::TColumnFilter MakeReplaceFilter(const std::shared_ptr<arrow::RecordBatch>& batch,
                                    THashSet<NArrow::TReplaceKey>& keys) {
    NArrow::TColumnFilter bits;
    bits.Reset(batch->num_rows());

    auto columns = std::make_shared<NArrow::TArrayVec>(batch->columns());

    for (int i = 0; i < batch->num_rows(); ++i) {
        NArrow::TReplaceKey key(columns, i);
        bits.Add(keys.emplace(key).second);
    }
    return bits;
}

NArrow::TColumnFilter MakeReplaceFilterLastWins(const std::shared_ptr<arrow::RecordBatch>& batch,
                                            THashSet<NArrow::TReplaceKey>& keys) {
    if (!batch->num_rows()) {
        return {};
    }

    NArrow::TColumnFilter result;
    result.Reset(batch->num_rows());

    auto columns = std::make_shared<NArrow::TArrayVec>(batch->columns());

    for (int i = batch->num_rows() - 1; i >= 0; --i) {
        NArrow::TReplaceKey key(columns, i);
        result.Add(keys.emplace(key).second);
    }

    return result;
}

NArrow::TColumnFilter FilterPortion(const std::shared_ptr<arrow::RecordBatch>& portion, const TReadMetadata& readMetadata) {
    Y_VERIFY(portion);
    NArrow::TColumnFilter result = readMetadata.GetPKRangesFilter().BuildFilter(portion);
    if (readMetadata.PlanStep) {
        auto snapSchema = TIndexInfo::ArrowSchemaSnapshot();
        result.And(MakeSnapshotFilter(portion, snapSchema, readMetadata.PlanStep, readMetadata.TxId));
    }

    return result;
}

NArrow::TColumnFilter FilterNotIndexed(const std::shared_ptr<arrow::RecordBatch>& batch, const TReadMetadata& readMetadata) {
    return readMetadata.GetPKRangesFilter().BuildFilter(batch);
}

NArrow::TColumnFilter EarlyFilter(const std::shared_ptr<arrow::RecordBatch>& batch, std::shared_ptr<NSsa::TProgram> ssa) {
    return ssa->MakeEarlyFilter(batch, NArrow::GetCustomExecContext());
}

void ReplaceDupKeys(std::shared_ptr<arrow::RecordBatch>& batch,
                    const std::shared_ptr<arrow::Schema>& replaceSchema, bool lastWins) {
    THashSet<NArrow::TReplaceKey> replaces;

    auto keyBatch = NArrow::ExtractColumns(batch, replaceSchema);

    NArrow::TColumnFilter bits;
    if (lastWins) {
        bits = MakeReplaceFilterLastWins(keyBatch, replaces);
    } else {
        bits = MakeReplaceFilter(keyBatch, replaces);
    }
    Y_VERIFY(bits.Apply(batch));
}

}
