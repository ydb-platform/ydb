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
    std::variant<TBlobRangeLink16, TString> Data;

    TIndexChunk() = default;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TIndexChunk& proto);
public:
    ui64 GetDataSize() const;

    bool HasBlobRange() const {
        return std::holds_alternative<TBlobRangeLink16>(Data);
    }

    const TBlobRangeLink16* GetBlobRangeOptional() const {
        return std::get_if<TBlobRangeLink16>(&Data);
    }

    const TBlobRangeLink16& GetBlobRangeVerified() const {
        const auto* result = std::get_if<TBlobRangeLink16>(&Data);
        AFL_VERIFY(result);
        return *result;
    }

    bool HasBlobData() const {
        return std::holds_alternative<TString>(Data);
    }

    const TString* GetBlobDataOptional() const {
        return std::get_if<TString>(&Data);
    }

    const TString& GetBlobDataVerified() const {
        const auto* result = std::get_if<TString>(&Data);
        AFL_VERIFY(result);
        return *result;
    }

    TChunkAddress GetAddress() const {
        return TChunkAddress(IndexId, ChunkIdx);
    }

    ui32 GetEntityId() const {
        return IndexId;
    }

    TIndexChunk(const ui32 indexId, const ui32 chunkIdx, const ui32 recordsCount, const ui64 rawBytes, const TBlobRangeLink16& blobRange)
        : IndexId(indexId)
        , ChunkIdx(chunkIdx)
        , RecordsCount(recordsCount)
        , RawBytes(rawBytes)
        , Data(blobRange) {

    }

    TIndexChunk(const ui32 indexId, const ui32 chunkIdx, const ui32 recordsCount, const ui64 rawBytes, const TString& blobData)
        : IndexId(indexId)
        , ChunkIdx(chunkIdx)
        , RecordsCount(recordsCount)
        , RawBytes(rawBytes)
        , Data(blobData) {
    }

    void RegisterBlobIdx(const TBlobRangeLink16::TLinkId blobLinkId) {
        auto* result = std::get_if<TBlobRangeLink16>(&Data);
        AFL_VERIFY(result);
        result->BlobIdx = blobLinkId;
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
