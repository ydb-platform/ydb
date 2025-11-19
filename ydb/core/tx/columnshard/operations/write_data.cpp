#include "write_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/columnshard/defs.h>

namespace NKikimr::NColumnShard {

bool TArrowData::Parse(const NKikimrDataEvents::TEvWrite::TOperation& proto, const NEvWrite::IPayloadReader& payload) {
    if (proto.GetPayloadFormat() != NKikimrDataEvents::FORMAT_ARROW) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "invalid_payload_format")("payload_format", (ui64)proto.GetPayloadFormat());
        return false;
    }
    IncomingData = payload.GetDataFromPayload(proto.GetPayloadIndex());
    if (proto.HasType()) {
        auto type = TEnumOperator<NEvWrite::EModificationType>::DeserializeFromProto(proto.GetType());
        if (!type) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "invalid_modification_type")("proto", proto.DebugString());
            return false;
        }
        ModificationType = *type;
    }

    if (proto.HasPayloadSchema()) {
        Operation = proto;
    } else {
        std::vector<ui32> columns;
        for (auto&& columnId : proto.GetColumnIds()) {
            columns.emplace_back(columnId);
        }
        if (columns.empty()) {
            BatchSchema = IndexSchema;
        } else {
            BatchSchema = std::make_shared<NOlap::TFilteredSnapshotSchema>(IndexSchema, columns);
        }
        if (BatchSchema->GetColumnsCount() != columns.size()) {
            return false;
        }
    }
    OriginalDataSize = IncomingData.size();
    return !!IncomingData;
}

TConclusion<std::shared_ptr<arrow::RecordBatch>> TArrowData::ExtractBatch() {
    Y_ABORT_UNLESS(!!IncomingData);
    std::shared_ptr<arrow::RecordBatch> result;
    if (Operation.HasPayloadSchema()) {
        auto payloadSchema = NArrow::DeserializeSchema(Operation.GetPayloadSchema());
        result = NArrow::DeserializeBatch(IncomingData, payloadSchema);
    } else {
        result = NArrow::DeserializeBatch(IncomingData, std::make_shared<arrow::Schema>(BatchSchema->GetSchema()->fields()));
    }

    TString emptyString;
    std::swap(IncomingData, emptyString);
    return result;
}

ui64 TArrowData::GetSchemaVersion() const {
    return IndexSchema->GetVersion();
}

bool TProtoArrowData::ParseFromProto(const NKikimrTxColumnShard::TEvWrite& proto) {
    IncomingData = proto.GetData();
    if (proto.HasModificationType()) {
        ModificationType = TEnumOperator<NEvWrite::EModificationType>::DeserializeFromProto(proto.GetModificationType());
    }
    if (proto.HasMeta()) {
        const auto& incomingDataScheme = proto.GetMeta().GetSchema();
        if (incomingDataScheme.empty() || proto.GetMeta().GetFormat() != NKikimrTxColumnShard::FORMAT_ARROW) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "invalid_data_format");
            return false;
        }
        ArrowSchema = NArrow::DeserializeSchema(incomingDataScheme);
        if (!ArrowSchema) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "cannot_deserialize_data");
            return false;
        }
    }
    OriginalDataSize = IncomingData.size();
    if (IncomingData.empty()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "empty_data");
        return false;
    }
    if (NColumnShard::TLimits::GetMaxBlobSize() < IncomingData.size() && !AppDataVerified().FeatureFlags.GetEnableWritePortionsOnInsert()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "too_big_blob");
        return false;
    }
    return true;
}

TConclusion<std::shared_ptr<arrow::RecordBatch>> TProtoArrowData::ExtractBatch() {
    Y_ABORT_UNLESS(!!IncomingData);
    auto result = NArrow::DeserializeBatch(IncomingData, ArrowSchema);
    IncomingData = "";
    return result;
}

ui64 TProtoArrowData::GetSchemaVersion() const {
    return IndexSchema->GetVersion();
}

}   // namespace NKikimr::NColumnShard
