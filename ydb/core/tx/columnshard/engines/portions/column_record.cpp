#include "column_record.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

TChunkMeta::TChunkMeta(const TColumnChunkLoadContext& context, const TIndexInfo& indexInfo) {
    auto field = indexInfo.ArrowColumnField(context.GetAddress().GetColumnId());
    if (context.GetMetaProto().HasNumRows()) {
        NumRows = context.GetMetaProto().GetNumRows();
    }
    if (context.GetMetaProto().HasRawBytes()) {
        RawBytes = context.GetMetaProto().GetRawBytes();
    }
    if (context.GetMetaProto().HasMinValue()) {
        Min = ConstantToScalar(context.GetMetaProto().GetMinValue(), field->type());
    }
    if (context.GetMetaProto().HasMaxValue()) {
        Max = ConstantToScalar(context.GetMetaProto().GetMaxValue(), field->type());
    }
}

TChunkMeta::TChunkMeta(const std::shared_ptr<arrow::Array>& column, const ui32 columnId, const TIndexInfo& indexInfo) {
    Y_VERIFY(column);
    Y_VERIFY(column->length());
    NumRows = column->length();
    RawBytes = NArrow::GetArrayDataSize(column);

    if (indexInfo.GetMinMaxIdxColumns().contains(columnId)) {
        std::pair<i32, i32> minMaxPos = {0, (column->length() - 1)};
        if (!indexInfo.IsSortedColumn(columnId)) {
            minMaxPos = NArrow::FindMinMaxPosition(column);
            Y_VERIFY(minMaxPos.first >= 0);
            Y_VERIFY(minMaxPos.second >= 0);
        }

        Min = NArrow::GetScalar(column, minMaxPos.first);
        Max = NArrow::GetScalar(column, minMaxPos.second);

        Y_VERIFY(Min);
        Y_VERIFY(Max);
    }
}

NKikimrTxColumnShard::TIndexColumnMeta TChunkMeta::SerializeToProto() const {
    NKikimrTxColumnShard::TIndexColumnMeta meta;
    if (NumRows) {
        meta.SetNumRows(*NumRows);
    }
    if (RawBytes) {
        meta.SetRawBytes(*RawBytes);
    }
    if (HasMinMax()) {
        ScalarToConstant(*Min, *meta.MutableMinValue());
        ScalarToConstant(*Max, *meta.MutableMaxValue());
    }
    return meta;
}

TColumnRecord::TColumnRecord(const TColumnChunkLoadContext& loadContext, const TIndexInfo& info)
    : Meta(loadContext, info)
    , ColumnId(loadContext.GetAddress().GetColumnId())
    , Chunk(loadContext.GetAddress().GetChunk())
    , BlobRange(loadContext.GetBlobRange())
{
}

TColumnRecord::TColumnRecord(const TChunkAddress& address, const std::shared_ptr<arrow::Array>& column, const TIndexInfo& info)
    : Meta(column, address.GetColumnId(), info)
    , ColumnId(address.GetColumnId())
    , Chunk(address.GetChunk())
{
}

}
