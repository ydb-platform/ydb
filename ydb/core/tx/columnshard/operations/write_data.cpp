#include "write_data.h"

#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {

bool TArrowData::Parse(const NKikimrDataEvents::TEvWrite_TOperation& proto, const NEvWrite::IPayloadData& payload) {
    if(proto.GetPayloadFormat() != NKikimrDataEvents::FORMAT_ARROW)
    {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "invalid_payload_format")("payload_format", (ui64)proto.GetPayloadFormat());
        return false;
    }
    IncomingData = payload.GetDataFromPayload(proto.GetPayloadIndex());

    std::vector<ui32> columns;
    for (auto&& columnId : proto.GetColumnIds()) {
        columns.emplace_back(columnId);
    }
    BatchSchema = std::make_shared<NOlap::TFilteredSnapshotSchema>(IndexSchema, columns);
    OriginalDataSize = IncomingData.size();
    return BatchSchema->GetColumnsCount() == columns.size() && !IncomingData.empty();
}

std::shared_ptr<arrow::RecordBatch> TArrowData::ExtractBatch() {
    Y_ABORT_UNLESS(!!IncomingData);
    auto result = IndexSchema->PrepareForInsert(IncomingData, BatchSchema->GetSchema());
    IncomingData = "";
    return result;
}

ui64 TArrowData::GetSchemaVersion() const {
    return IndexSchema->GetVersion();
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
    OriginalDataSize = IncomingData.size();
    return !IncomingData.empty() && IncomingData.size() <= NColumnShard::TLimits::GetMaxBlobSize();
}

std::shared_ptr<arrow::RecordBatch> TProtoArrowData::ExtractBatch() {
    Y_ABORT_UNLESS(!!IncomingData);
    auto result = IndexSchema->PrepareForInsert(IncomingData, ArrowSchema);
    IncomingData = "";
    return result;
}

ui64 TProtoArrowData::GetSchemaVersion() const {
    return IndexSchema->GetVersion();
}

}
