#pragma once

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NOlap {

struct TColumnRecord {
    ui32 ColumnId = 0;
    ui16 Chunk;     // Number of blob for column ColumnName in Portion
    TBlobRange BlobRange;
    TString Metadata;

    bool IsEqualTest(const TColumnRecord& item) const {
        return ColumnId == item.ColumnId && Chunk == item.Chunk;
    }

    std::optional<ui32> GetChunkRowsCount() const {
        return {};
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

    static TColumnRecord Make(ui32 columnId, ui16 chunk = 0) {
        TColumnRecord row;
        row.ColumnId = columnId;
        row.Chunk = chunk;
        return row;
    }

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
