#include "column_record.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

TConclusionStatus TChunkMeta::DeserializeFromProto(const TChunkAddress& address, const NKikimrTxColumnShard::TIndexColumnMeta& proto, const TIndexInfo& indexInfo) {
    auto field = indexInfo.ArrowColumnFieldOptional(address.GetColumnId());
    if (proto.HasNumRows()) {
        NumRows = proto.GetNumRows();
    }
    if (proto.HasRawBytes()) {
        RawBytes = proto.GetRawBytes();
    }
    if (proto.HasMaxValue()) {
        AFL_VERIFY(field)("field_id", address.GetColumnId())("field_name", indexInfo.GetColumnName(address.GetColumnId()));
        Max = ConstantToScalar(proto.GetMaxValue(), field->type());
    }
    return TConclusionStatus::Success();
}

TChunkMeta::TChunkMeta(const TColumnChunkLoadContext& context, const TIndexInfo& indexInfo) {
    AFL_VERIFY(DeserializeFromProto(context.GetAddress(), context.GetMetaProto(), indexInfo));
}

TChunkMeta::TChunkMeta(const std::shared_ptr<arrow::Array>& column, const ui32 columnId, const TIndexInfo& indexInfo)
    : TBase(column, indexInfo.GetMinMaxIdxColumns().contains(columnId), indexInfo.IsSortedColumn(columnId))
{
}

NKikimrTxColumnShard::TIndexColumnMeta TChunkMeta::SerializeToProto() const {
    NKikimrTxColumnShard::TIndexColumnMeta meta;
    if (NumRows) {
        meta.SetNumRows(*NumRows);
    }
    if (RawBytes) {
        meta.SetRawBytes(*RawBytes);
    }
    if (HasMax()) {
        ScalarToConstant(*Max, *meta.MutableMaxValue());
        ScalarToConstant(*Max, *meta.MutableMinValue());
    }
    return meta;
}

TColumnRecord::TColumnRecord(const TBlobRangeLink16::TLinkId blobLinkId, const TColumnChunkLoadContext& loadContext, const TIndexInfo& info)
    : Meta(loadContext, info)
    , ColumnId(loadContext.GetAddress().GetColumnId())
    , Chunk(loadContext.GetAddress().GetChunk())
    , BlobRange(loadContext.GetBlobRange().BuildLink(blobLinkId))
{
}

TColumnRecord::TColumnRecord(const TChunkAddress& address, const std::shared_ptr<arrow::Array>& column, const TIndexInfo& info)
    : Meta(column, address.GetColumnId(), info)
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

NKikimr::TConclusionStatus TColumnRecord::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TColumnRecord& proto, const TIndexInfo& indexInfo) {
    ColumnId = proto.GetColumnId();
    Chunk = proto.GetChunkIdx();
    {
        auto parse = Meta.DeserializeFromProto(GetAddress(), proto.GetMeta(), indexInfo);
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
