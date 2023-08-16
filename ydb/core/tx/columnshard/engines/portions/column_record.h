#pragma once

#include "common.h"
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/library/accessor/accessor.h>
#include <util/string/builder.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {
class TColumnChunkLoadContext;
struct TIndexInfo;

struct TChunkMeta {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, Min);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, Max);
    YDB_READONLY(ui32, NumRows, 0);
    YDB_READONLY(ui32, RawBytes, 0);
public:
    ui64 GetMetadataSize() const {
        return sizeof(NumRows) + sizeof(RawBytes) + 8 * 3 * 2;
    }

    bool HasMinMax() const noexcept {
        return Min.get() && Max.get();
    }

    NKikimrTxColumnShard::TIndexColumnMeta SerializeToProto() const;

    TChunkMeta(const TColumnChunkLoadContext& context, const TIndexInfo& indexInfo);

    TChunkMeta(const std::shared_ptr<arrow::Array>& column, const ui32 columnId, const TIndexInfo& indexInfo);
};

struct TColumnRecord {
private:
    TChunkMeta Meta;
public:
    ui32 ColumnId = 0;
    ui16 Chunk = 0;
    TBlobRange BlobRange;

    const TChunkMeta& GetMeta() const {
        return Meta;
    }

    TChunkAddress GetAddress() const {
        return TChunkAddress(ColumnId, Chunk);
    }

    bool IsEqualTest(const TColumnRecord& item) const {
        return ColumnId == item.ColumnId && Chunk == item.Chunk;
    }

    std::optional<ui32> GetChunkRowsCount() const {
        return Meta.GetNumRows();
    }

    bool Valid() const {
        return ColumnId && ValidBlob();
    }

    TString SerializedBlobId() const {
        return BlobRange.BlobId.SerializeBinary();
    }

    TString DebugString() const {
        return TStringBuilder()
            << "column_id:" << ColumnId << ";"
            << "blob_range:" << BlobRange << ";"
            ;
    }

    bool ValidBlob() const {
        return BlobRange.BlobId.IsValid() && BlobRange.Size;
    }

    TColumnRecord(const TChunkAddress& address, const std::shared_ptr<arrow::Array>& column, const TIndexInfo& info);

    TColumnRecord(const TColumnChunkLoadContext& loadContext, const TIndexInfo& info);

    friend IOutputStream& operator << (IOutputStream& out, const TColumnRecord& rec) {
        out << '{';
        if (rec.Chunk) {
            out << 'n' << rec.Chunk;
        }
        out << ',' << (i32)rec.ColumnId;
        out << ',' << rec.BlobRange.ToString();
        out << '}';
        return out;
    }
};

}
