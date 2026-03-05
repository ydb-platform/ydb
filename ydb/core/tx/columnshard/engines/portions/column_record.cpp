#include "column_record.h"

#include <ydb/core/formats/arrow/accessor/common/additional_data.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

TConclusionStatus TChunkMeta::DeserializeFromProto(const NKikimrTxColumnShard::TIndexColumnMeta& proto) {
    if (proto.HasNumRows()) {
        RecordsCount = proto.GetNumRows();
    }
    if (proto.HasRawBytes()) {
        RawBytes = proto.GetRawBytes();
    }
    if (proto.HasAdditionalAccessorData()) {
        const auto& add = proto.GetAdditionalAccessorData();
        if (add.Accessor_case() == NKikimrTxColumnShard::TAdditionalAccessorData::kDictionaryAccessorData) {
            const auto& acc = add.GetDictionaryAccessorData();
            AdditionalAccessorData = std::make_shared<NArrow::NAccessor::TDictionaryAccessorData>(
                acc.GetVariantsBlobSize(), acc.GetRecordsBlobSize());
        } else {
            AdditionalAccessorData.reset();
        }
    } else {
        AdditionalAccessorData.reset();
    }
    return TConclusionStatus::Success();
}

TChunkMeta::TChunkMeta(const TColumnChunkLoadContextV1& context) {
    DeserializeFromProto(context.GetMetaProto()).Validate();
}

TChunkMeta::TChunkMeta(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column)
    : TBase(column) {
}

NKikimrTxColumnShard::TIndexColumnMeta TChunkMeta::SerializeToProto() const {
    NKikimrTxColumnShard::TIndexColumnMeta meta;
    meta.SetNumRows(RecordsCount);
    meta.SetRawBytes(RawBytes);
    if (AdditionalAccessorData) {
        struct TVisitor : NArrow::NAccessor::IAdditionalAccessorDataVisitor {
            NKikimrTxColumnShard::TAdditionalAccessorData* Proto = nullptr;
            void VisitDictionary(ui32 variantsBlobSize, ui32 recordsBlobSize) override {
                if (Proto) {
                    auto* acc = Proto->MutableDictionaryAccessorData();
                    acc->SetVariantsBlobSize(variantsBlobSize);
                    acc->SetRecordsBlobSize(recordsBlobSize);
                }
            }
        } visitor;
        visitor.Proto = meta.MutableAdditionalAccessorData();
        AdditionalAccessorData->Accept(visitor);
    }
    return meta;
}

TColumnRecord::TColumnRecord(const TColumnChunkLoadContextV1& loadContext)
    : Meta(loadContext)
    , ColumnId(loadContext.GetAddress().GetColumnId())
    , Chunk(loadContext.GetAddress().GetChunk())
    , BlobRange(loadContext.GetBlobRange()) {
}

TColumnRecord::TColumnRecord(const TChunkAddress& address, const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column)
    : Meta(column)
    , ColumnId(address.GetColumnId())
    , Chunk(address.GetChunk()) {
}

NKikimrColumnShardDataSharingProto::TColumnRecord TColumnRecord::SerializeToProto() const {
    NKikimrColumnShardDataSharingProto::TColumnRecord result;
    result.SetColumnId(ColumnId);
    result.SetChunkIdx(Chunk);
    *result.MutableMeta() = Meta.SerializeToProto();
    *result.MutableBlobRange() = BlobRange.SerializeToProto();
    return result;
}

NKikimr::TConclusionStatus TColumnRecord::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TColumnRecord& proto) {
    ColumnId = proto.GetColumnId();
    Chunk = proto.GetChunkIdx();
    {
        auto parse = Meta.DeserializeFromProto(proto.GetMeta());
        if (!parse) {
            return parse;
        }
    }
    {
        auto parsed = TBlobRangeLink16::BuildFromProto(proto.GetBlobRange());
        if (!parsed) {
            return parsed;
        }
        BlobRange = parsed.DetachResult();
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap
