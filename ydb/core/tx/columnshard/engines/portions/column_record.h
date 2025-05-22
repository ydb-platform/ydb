#pragma once

#include "common.h"

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>
#include <ydb/core/tx/columnshard/splitter/chunk_meta.h>
#include <ydb/core/tx/columnshard/splitter/chunks.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/splitter/stats.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <util/string/builder.h>

namespace NKikimrColumnShardDataSharingProto {
class TColumnRecord;
}

namespace NKikimr::NOlap {
class TColumnChunkLoadContextV1;
struct TIndexInfo;
class TColumnRecord;

struct TChunkMeta: public TSimpleChunkMeta {
private:
    using TBase = TSimpleChunkMeta;
    TChunkMeta() = default;
    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrTxColumnShard::TIndexColumnMeta& proto);
    friend class TColumnRecord;

public:
    TChunkMeta(TSimpleChunkMeta&& baseMeta)
        : TBase(baseMeta) {
    }

    [[nodiscard]] static TConclusion<TChunkMeta> BuildFromProto(const NKikimrTxColumnShard::TIndexColumnMeta& proto) {
        TChunkMeta result;
        auto parse = result.DeserializeFromProto(proto);
        if (!parse) {
            return parse;
        }
        return result;
    }

    NKikimrTxColumnShard::TIndexColumnMeta SerializeToProto() const;

    class TTestInstanceBuilder {
    public:
        static TChunkMeta Build(const ui64 recordsCount, const ui64 rawBytes) {
            TChunkMeta result;
            result.RecordsCount = recordsCount;
            result.RawBytes = rawBytes;
            return result;
        }
    };

    TChunkMeta(const TColumnChunkLoadContextV1& context);

    TChunkMeta(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column);
};

class TColumnRecord {
private:
    TChunkMeta Meta;
    TColumnRecord(TChunkMeta&& meta)
        : Meta(std::move(meta)) {
    }

    TColumnRecord() = default;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TColumnRecord& proto);

public:
    ui32 ColumnId = 0;
    ui16 Chunk = 0;
    TBlobRangeLink16 BlobRange;

    TChunkMeta& MutableMeta() {
        return Meta;
    }

    ui32 GetEntityId() const {
        return ColumnId;
    }

    void ResetBlobRange() {
        BlobRange = TBlobRangeLink16();
    }

    void RegisterBlobIdx(const ui16 blobIdx) {
        AFL_VERIFY(!BlobRange.BlobIdx)("original", BlobRange.BlobIdx)("new", blobIdx);
        BlobRange.BlobIdx = blobIdx;
    }

    TColumnRecord(const TChunkAddress& address, const TBlobRangeLink16& range, TChunkMeta&& meta)
        : Meta(std::move(meta))
        , ColumnId(address.GetColumnId())
        , Chunk(address.GetChunk())
        , BlobRange(range) {
    }

    class TTestInstanceBuilder {
    public:
        static TColumnRecord Build(const ui32 columnId, const ui16 chunkId, const ui64 offset, const ui64 size, const ui64 recordsCount, const ui64 rawBytes) {
            TColumnRecord result(TChunkMeta::TTestInstanceBuilder::Build(recordsCount, rawBytes));
            result.ColumnId = columnId;
            result.Chunk = chunkId;
            result.BlobRange.Offset = offset;
            result.BlobRange.Size = size;
            return result;
        }
    };

    ui32 GetColumnId() const {
        return ColumnId;
    }
    ui16 GetChunkIdx() const {
        return Chunk;
    }
    const TBlobRangeLink16& GetBlobRange() const {
        return BlobRange;
    }

    NKikimrTxColumnShard::TColumnChunkInfo SerializeToDBProto() const {
        NKikimrTxColumnShard::TColumnChunkInfo result;
        result.SetSSColumnId(GetEntityId());
        result.SetChunkIdx(GetChunkIdx());
        *result.MutableChunkMetadata() = Meta.SerializeToProto();
        *result.MutableBlobRangeLink() = BlobRange.SerializeToProto();
        return result;
    }
    NKikimrColumnShardDataSharingProto::TColumnRecord SerializeToProto() const;
    static TConclusion<TColumnRecord> BuildFromProto(const NKikimrColumnShardDataSharingProto::TColumnRecord& proto) {
        TColumnRecord result;
        auto parse = result.DeserializeFromProto(proto);
        if (!parse) {
            return parse;
        }
        return result;
    }

    NArrow::NSplitter::TColumnSerializationStat GetSerializationStat(const std::string& columnName) const {
        NArrow::NSplitter::TColumnSerializationStat result(ColumnId, columnName);
        result.Merge(GetSerializationStat());
        return result;
    }

    NArrow::NSplitter::TSimpleSerializationStat GetSerializationStat() const {
        return NArrow::NSplitter::TSimpleSerializationStat(BlobRange.Size, Meta.GetRecordsCount(), Meta.GetRawBytes());
    }

    const TChunkMeta& GetMeta() const {
        return Meta;
    }

    TChunkAddress GetAddress() const {
        return TChunkAddress(ColumnId, Chunk);
    }

    bool IsEqualTest(const TColumnRecord& item) const {
        return ColumnId == item.ColumnId && Chunk == item.Chunk;
    }

    TString DebugString() const {
        return TStringBuilder() << "column_id:" << ColumnId << ";"
                                << "chunk_idx:" << Chunk << ";"
                                << "blob_range:" << BlobRange.ToString() << ";";
    }

    TColumnRecord(const TChunkAddress& address, const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column);
    TColumnRecord(const TColumnChunkLoadContextV1& loadContext);

    friend IOutputStream& operator<<(IOutputStream& out, const TColumnRecord& rec) {
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

}   // namespace NKikimr::NOlap
