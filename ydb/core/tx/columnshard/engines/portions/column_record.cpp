#include "column_record.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

TConclusionStatus TChunkMeta::DeserializeFromProto(const TChunkAddress& address, const NKikimrTxColumnShard::TIndexColumnMeta& proto, const TSimpleColumnInfo& columnInfo) {
    auto field = columnInfo.GetArrowField();
    if (proto.HasNumRows()) {
        NumRows = proto.GetNumRows();
    }
    if (proto.HasRawBytes()) {
        RawBytes = proto.GetRawBytes();
    }
    if (proto.HasMaxValue()) {
        AFL_VERIFY(field)("field_id", address.GetColumnId())("field_name", columnInfo.GetColumnName());
        Max = ConstantToScalar(proto.GetMaxValue(), field->type());
    }
    return TConclusionStatus::Success();
}

TChunkMeta::TChunkMeta(const TColumnChunkLoadContext& context, const TSimpleColumnInfo& columnInfo) {
    DeserializeFromProto(context.GetAddress(), context.GetMetaProto(), columnInfo).Validate();
}

TChunkMeta::TChunkMeta(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column, const TSimpleColumnInfo& columnInfo)
    : TBase(column, columnInfo.GetNeedMinMax(), columnInfo.GetIsSorted())
{
}

NKikimrTxColumnShard::TIndexColumnMeta TChunkMeta::SerializeToProto() const {
    NKikimrTxColumnShard::TIndexColumnMeta meta;
    meta.SetNumRows(NumRows);
    meta.SetRawBytes(RawBytes);
    if (HasMax()) {
        ScalarToConstant(*Max, *meta.MutableMaxValue());
        ScalarToConstant(*Max, *meta.MutableMinValue());
    }
    return meta;
}

TColumnRecord::TColumnRecord(const TBlobRangeLink16::TLinkId blobLinkId, const TColumnChunkLoadContext& loadContext, const TSimpleColumnInfo& columnInfo)
    : Meta(loadContext, columnInfo)
    , ColumnId(loadContext.GetAddress().GetColumnId())
    , Chunk(loadContext.GetAddress().GetChunk())
    , BlobRange(loadContext.GetBlobRange().BuildLink(blobLinkId))
{
}

TColumnRecord::TColumnRecord(
    const TChunkAddress& address, const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column, const TSimpleColumnInfo& columnInfo)
    : Meta(column, columnInfo)
    , ColumnId(address.GetColumnId())
    , Chunk(address.GetChunk())
{
}

NKikimrColumnShardDataSharingProto::TColumnRecord TColumnRecord::SerializeToProto() const {
    NKikimrColumnShardDataSharingProto::TColumnRecord result;
    result.SetColumnId(ColumnId);
    result.SetChunkIdx(Chunk);
    *result.MutableMeta() = Meta.SerializeToProto();
    *result.MutableBlobRange() = BlobRange.SerializeToProto();
    return result;
}

NKikimr::TConclusionStatus TColumnRecord::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TColumnRecord& proto, const TSimpleColumnInfo& columnInfo) {
    ColumnId = proto.GetColumnId();
    Chunk = proto.GetChunkIdx();
    {
        auto parse = Meta.DeserializeFromProto(GetAddress(), proto.GetMeta(), columnInfo);
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

}
