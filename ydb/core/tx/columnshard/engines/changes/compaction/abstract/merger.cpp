#include "merger.h"

namespace NKikimr::NOlap::NCompaction {

void IColumnMerger::Start(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) {
    AFL_VERIFY(!Started);
    Started = true;
    for (auto&& i : input) {
        if (!i) {
            continue;
        }
        AFL_VERIFY(i->GetDataType()->id() == Context.GetResultField()->type()->id())("input", i->GetDataType()->ToString())(
                                                 "result", Context.GetResultField()->ToString());
    }
    return DoStart(input, mergeContext);
}

 TMergingChunkContext::TMergingChunkContext(const std::shared_ptr<arrow::RecordBatch>& pkAndAddresses) {
    auto columnPortionIdx = pkAndAddresses->GetColumnByName(IColumnMerger::PortionIdFieldName);
    auto columnPortionRecordIdx = pkAndAddresses->GetColumnByName(IColumnMerger::PortionRecordIndexFieldName);
    Y_ABORT_UNLESS(columnPortionIdx && columnPortionRecordIdx);
    Y_ABORT_UNLESS(columnPortionIdx->type_id() == arrow::UInt16Type::type_id);
    Y_ABORT_UNLESS(columnPortionRecordIdx->type_id() == arrow::UInt32Type::type_id);
    IdxArray = static_pointer_cast<arrow::UInt16Array>(columnPortionIdx);
    RecordIdxArray = static_pointer_cast<arrow::UInt32Array>(columnPortionRecordIdx);

    AFL_VERIFY(pkAndAddresses->num_rows() == IdxArray->length());
    AFL_VERIFY(pkAndAddresses->num_rows() == RecordIdxArray->length());
}

}   // namespace NKikimr::NOlap::NCompaction
