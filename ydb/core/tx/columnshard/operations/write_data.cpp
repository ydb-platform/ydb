#include "write_data.h"

#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {

void TArrowData::Serialize(NKikimrDataEvents::TOperationData& proto) const {
    Y_FAIL("Not implemented");
    Y_UNUSED(proto);
}

bool TArrowData::Parse(const NKikimrDataEvents::TOperationData& proto, const IPayloadData& payload) {
    IncomingData = payload.GetDataFromPayload(proto.GetArrowData().GetPayloadIndex());

    std::vector<ui32> columns;
    for (auto&& columnId : proto.GetColumnIds()) {
        columns.emplace_back(columnId);
    }
    BatchSchema = std::make_shared<NOlap::TFilteredSnapshotSchema>(IndexSchema, columns);
    return BatchSchema->GetColumnsCount() == columns.size() && !IncomingData.empty() && IncomingData.size() <= NColumnShard::TLimits::GetMaxBlobSize();
}

std::shared_ptr<arrow::RecordBatch> TArrowData::GetArrowBatch() const {
    TString err;
    return IndexSchema->PrepareForInsert(IncomingData, BatchSchema->GetSchema(), err);
}

void TProtoArrowData::Serialize(NKikimrDataEvents::TOperationData& proto) const {
    Y_FAIL("Not implemented");
    Y_UNUSED(proto);
}

bool TProtoArrowData::ParseFromProto(const NKikimrTxColumnShard::TEvWrite& proto) {
    IncomingData = proto.GetData();
    if (proto.HasMeta()) {
        const auto& incomingDataScheme = proto.GetMeta().GetSchema();
        if (incomingDataScheme.empty() || proto.GetMeta().GetFormat() != NKikimrTxColumnShard::FORMAT_ARROW) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "invalid_data_format");
            return false;
        }
        ArrowSchema = NArrow::DeserializeSchema(incomingDataScheme);
        if (!ArrowSchema) {
            return false;
        }
    }
    return !IncomingData.empty() && IncomingData.size() <= NColumnShard::TLimits::GetMaxBlobSize();
}

std::shared_ptr<arrow::RecordBatch> TProtoArrowData::GetArrowBatch() const {
    TString err;
    return IndexSchema->PrepareForInsert(IncomingData, ArrowSchema, err);
}

}
