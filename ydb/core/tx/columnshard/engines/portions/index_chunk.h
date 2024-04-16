#pragma once

#include "common.h"

#include <ydb/core/protos/tx_columnshard.pb.h>

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/splitter/stats.h>
#include <ydb/core/tx/columnshard/splitter/chunks.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <util/string/builder.h>

namespace NKikimrColumnShardDataSharingProto {
class TIndexChunk;
}

namespace NKikimr::NOlap {
struct TIndexInfo;

class TIndexChunk {
private:
    YDB_READONLY(ui32, IndexId, 0);
    YDB_READONLY(ui32, ChunkIdx, 0);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY(ui32, RawBytes, 0);
    YDB_READONLY_DEF(TBlobRange, BlobRange);

    TIndexChunk() = default;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TIndexChunk& proto);
public:
    TIndexChunk(const ui32 indexId, const ui32 chunkIdx, const ui32 recordsCount, const ui64 rawBytes, const TBlobRange& blobRange)
        : IndexId(indexId)
        , ChunkIdx(chunkIdx)
        , RecordsCount(recordsCount)
        , RawBytes(rawBytes)
        , BlobRange(blobRange) {

    }

    void RegisterBlobId(const TUnifiedBlobId& blobId) {
//        AFL_VERIFY(!BlobRange.BlobId.GetTabletId())("original", BlobRange.BlobId.ToStringNew())("new", blobId.ToStringNew());
        BlobRange.BlobId = blobId;
    }

    static TConclusion<TIndexChunk> BuildFromProto(const NKikimrColumnShardDataSharingProto::TIndexChunk& proto) {
        TIndexChunk result;
        auto parse = result.DeserializeFromProto(proto);
        if (!parse) {
            return parse;
        }
        return result;
    }

    NKikimrColumnShardDataSharingProto::TIndexChunk SerializeToProto() const;

};

}
