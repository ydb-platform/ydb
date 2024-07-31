#pragma once
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>

namespace NKikimr::NOlap {

class TBatchSerializedSlice: public TGeneralSerializedSlice {
private:
    using TBase = TGeneralSerializedSlice;
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Batch);

public:
    TBatchSerializedSlice(const std::shared_ptr<arrow::RecordBatch>& batch, NArrow::NSplitter::ISchemaDetailInfo::TPtr schema,
        std::shared_ptr<NColumnShard::TSplitterCounters> counters, const NSplitter::TSplitSettings& settings);

    explicit TBatchSerializedSlice(NArrow::NSplitter::TVectorView<TBatchSerializedSlice>&& objects) {
        Y_ABORT_UNLESS(objects.size());
        std::swap(*this, objects.front());
        for (ui32 i = 1; i < objects.size(); ++i) {
            MergeSlice(std::move(objects[i]));
        }
    }
    void MergeSlice(TBatchSerializedSlice&& slice) {
        Batch = NArrow::CombineBatches({ Batch, slice.Batch });
        TBase::MergeSlice(std::move(slice));
    }

    static std::vector<TBatchSerializedSlice> BuildSimpleSlices(const std::shared_ptr<arrow::RecordBatch>& batch,
        const NSplitter::TSplitSettings& settings, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters,
        const NArrow::NSplitter::ISchemaDetailInfo::TPtr& schemaInfo);
};
}
