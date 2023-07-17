#include "write_data.h"
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NEvWrite {

TWriteData::TWriteData(const TWriteMeta& writeMeta, IDataContainer::TPtr data)
    : WriteMeta(writeMeta)
    , Data(data)
{}

bool TArrowData::ParseFromProto(const NKikimrTxColumnShard::TEvWrite& proto) {
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

std::shared_ptr<arrow::RecordBatch> TArrowData::GetArrowBatch() const {
    auto batch = NArrow::DeserializeBatch(IncomingData, ArrowSchema);
    if (!batch) {
        return nullptr;
    }

    if (batch->num_rows() == 0) {
        return nullptr;
    }
    return batch;
}

}
