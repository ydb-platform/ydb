#include "meta.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap {

void TPortionMeta::FillBatchInfo(const std::shared_ptr<arrow::RecordBatch> batch, const TIndexInfo& indexInfo) {
    {
        auto keyBatch = NArrow::ExtractColumns(batch, indexInfo.GetReplaceKey());
        std::vector<bool> bits(batch->num_rows(), false);
        bits[0] = true;
        bits[batch->num_rows() - 1] = true; // it could be 0 if batch has one row

        auto filter = NArrow::TColumnFilter(std::move(bits)).BuildArrowFilter(batch->num_rows());
        auto res = arrow::compute::Filter(keyBatch, filter);
        Y_VERIFY(res.ok());

        ReplaceKeyEdges = res->record_batch();
        Y_VERIFY(ReplaceKeyEdges->num_rows() == 1 || ReplaceKeyEdges->num_rows() == 2);
    }

    auto edgesBatch = NArrow::ExtractColumns(ReplaceKeyEdges, indexInfo.GetIndexKey());
    IndexKeyStart = NArrow::TReplaceKey::FromBatch(edgesBatch, 0);
    IndexKeyEnd = NArrow::TReplaceKey::FromBatch(edgesBatch, edgesBatch->num_rows() - 1);
}

bool TPortionMeta::DeserializeFromProto(const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo) {
    const bool compositeIndexKey = indexInfo.IsCompositeIndexKey();
    if (Produced != TPortionMeta::EProduced::UNSPECIFIED) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DeserializeFromProto")("error", "parsing duplication");
        return true;
    }
    TierName = portionMeta.GetTierName();
    if (portionMeta.GetIsInserted()) {
        Produced = TPortionMeta::EProduced::INSERTED;
    } else if (portionMeta.GetIsCompacted()) {
        Produced = TPortionMeta::EProduced::COMPACTED;
    } else if (portionMeta.GetIsSplitCompacted()) {
        Produced = TPortionMeta::EProduced::SPLIT_COMPACTED;
    } else if (portionMeta.GetIsEvicted()) {
        Produced = TPortionMeta::EProduced::EVICTED;
    } else {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DeserializeFromProto")("error", "incorrect portion meta")("meta", portionMeta.DebugString());
        return false;
    }

    if (portionMeta.HasPrimaryKeyBorders()) {
        ReplaceKeyEdges = NArrow::DeserializeBatch(portionMeta.GetPrimaryKeyBorders(), indexInfo.GetReplaceKey());
        Y_VERIFY(ReplaceKeyEdges);
        Y_VERIFY_DEBUG(ReplaceKeyEdges->ValidateFull().ok());
        Y_VERIFY(ReplaceKeyEdges->num_rows() == 1 || ReplaceKeyEdges->num_rows() == 2);

        if (compositeIndexKey) {
            auto edgesBatch = NArrow::ExtractColumns(ReplaceKeyEdges, indexInfo.GetIndexKey());
            Y_VERIFY(edgesBatch);
            IndexKeyStart = NArrow::TReplaceKey::FromBatch(edgesBatch, 0);
            IndexKeyEnd = NArrow::TReplaceKey::FromBatch(edgesBatch, edgesBatch->num_rows() - 1);
        }
    }
    return true;
}

std::optional<NKikimrTxColumnShard::TIndexPortionMeta> TPortionMeta::SerializeToProto(const ui32 columnId, const ui32 chunk) const {
    if (columnId != FirstPkColumn || chunk != 0) {
        return {};
    }

    NKikimrTxColumnShard::TIndexPortionMeta portionMeta;
    portionMeta.SetTierName(TierName);

    switch (Produced) {
        case TPortionMeta::EProduced::UNSPECIFIED:
            Y_VERIFY(false);
        case TPortionMeta::EProduced::INSERTED:
            portionMeta.SetIsInserted(true);
            break;
        case TPortionMeta::EProduced::COMPACTED:
            portionMeta.SetIsCompacted(true);
            break;
        case TPortionMeta::EProduced::SPLIT_COMPACTED:
            portionMeta.SetIsSplitCompacted(true);
            break;
        case TPortionMeta::EProduced::EVICTED:
            portionMeta.SetIsEvicted(true);
            break;
        case TPortionMeta::EProduced::INACTIVE:
            Y_FAIL("Unexpected inactive case");
            //portionMeta->SetInactive(true);
            break;
    }

    if (const auto& keyEdgesBatch = ReplaceKeyEdges) {
        Y_VERIFY(keyEdgesBatch);
        Y_VERIFY_DEBUG(keyEdgesBatch->ValidateFull().ok());
        Y_VERIFY(keyEdgesBatch->num_rows() == 1 || keyEdgesBatch->num_rows() == 2);
        portionMeta.SetPrimaryKeyBorders(NArrow::SerializeBatchNoCompression(keyEdgesBatch));
    }
    return portionMeta;
}

TString TPortionMeta::DebugString() const {
    TStringBuilder sb;
    sb << "(produced=" << Produced << ";";
    if (TierName) {
        sb << "tier_name=" << TierName << ";";
    }
    sb << ")";
    return sb;
}

TString TPortionAddress::DebugString() const {
    return TStringBuilder() << "(granule_id=" << GranuleId << ";portion_id=" << PortionId << ")";
}

}
