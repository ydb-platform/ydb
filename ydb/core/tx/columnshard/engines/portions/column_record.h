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
    YDB_READONLY_DEF(std::optional<ui32>, NumRows);
    YDB_READONLY_DEF(std::optional<ui32>, RawBytes);
    TChunkMeta() = default;
public:
    ui64 GetMetadataSize() const {
        return sizeof(ui32) + sizeof(ui32) + 8 * 3 * 2;
    }

    bool HasMinMax() const noexcept {
        return Min.get() && Max.get();
    }

    NKikimrTxColumnShard::TIndexColumnMeta SerializeToProto() const;

    class TTestInstanceBuilder {
    public:
        static TChunkMeta Build(const ui64 numRows, const ui64 rawBytes) {
            TChunkMeta result;
            result.NumRows = numRows;
            result.RawBytes = rawBytes;
            return result;
        }
    };

    TChunkMeta(const TColumnChunkLoadContext& context, const TIndexInfo& indexInfo);

    TChunkMeta(const std::shared_ptr<arrow::Array>& column, const ui32 columnId, const TIndexInfo& indexInfo);
};

struct TColumnRecord {
private:
    TChunkMeta Meta;
    TColumnRecord(TChunkMeta&& meta)
        : Meta(std::move(meta))
    {

    }
public:
    ui32 ColumnId = 0;
    ui16 Chunk = 0;
    TBlobRange BlobRange;

    class TTestInstanceBuilder {
    public:
        static TColumnRecord Build(const ui32 columnId, const ui16 chunkId, const ui64 offset, const ui64 size, const ui64 numRows, const ui64 rawBytes) {
            TColumnRecord result(TChunkMeta::TTestInstanceBuilder::Build(numRows, rawBytes));
            result.ColumnId = columnId;
            result.Chunk = chunkId;
            result.BlobRange.Offset = offset;
            result.BlobRange.Size = size;
            return result;
        }
    };

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
