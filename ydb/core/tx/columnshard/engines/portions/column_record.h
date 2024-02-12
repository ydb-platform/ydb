#pragma once

#include "common.h"
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/splitter/stats.h>
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/library/accessor/accessor.h>
#include <util/string/builder.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {
class TColumnChunkLoadContext;
struct TIndexInfo;

class TIndexChunk {
private:
    YDB_READONLY(ui32, IndexId, 0);
    YDB_READONLY(ui32, ChunkIdx, 0);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY(ui32, RawBytes, 0);
    YDB_READONLY_DEF(TBlobRange, BlobRange);

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

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo::TIndexChunk& proto) {
        IndexId = proto.GetIndexId();
        ChunkIdx = proto.GetChunkIdx();
        RecordsCount = proto.GetRecordsCount();
        RawBytes = proto.GetRawBytes();
        {
            auto parsed = TBlobRange::BuildFromProto(proto.GetBlobRange());
            if (!parsed) {
                return parsed;
            }
            BlobRange = parsed.DetachResult();
        }
        return TConclusionStatus::Success();
    }

    NKikimrColumnShardDataSharingProto::TPortionInfo::TIndexChunk SerializeToProto() const {
        NKikimrColumnShardDataSharingProto::TPortionInfo::TIndexChunk result;
        proto.SetIndexId(IndexId);
        proto.SetChunkIdx(ChunkIdx);
        proto.SetRecordsCount(RecordsCount);
        proto.SetRawBytes(RawBytes);
        *result.MutabletBlobRange() = BlobRange.SerializeToProto();
        return result;
    }

};

struct TChunkMeta: public TSimpleChunkMeta {
private:
    using TBase = TSimpleChunkMeta;
    TChunkMeta() = default;
public:
    TChunkMeta(TSimpleChunkMeta&& baseMeta)
        : TBase(baseMeta)
    {

    }

    NKikimrTxColumnShard::TIndexColumnMeta SerializeToProto() const;
    TConclusionStatus DeserializeFromProto(const TChunkAddress& address, const NKikimrTxColumnShard::TIndexColumnMeta& proto, const TIndexInfo& indexInfo);

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


    void RegisterBlobId(const TUnifiedBlobId& blobId) {
//        AFL_VERIFY(!BlobRange.BlobId.GetTabletId())("original", BlobRange.BlobId.ToStringNew())("new", blobId.ToStringNew());
        BlobRange.BlobId = blobId;
    }

    TColumnRecord(const TChunkAddress& address, const TBlobRange& range, TChunkMeta&& meta)
        : Meta(std::move(meta))
        , ColumnId(address.GetColumnId())
        , Chunk(address.GetChunk())
        , BlobRange(range)
    {

    }

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

    ui32 GetColumnId() const { 
        return ColumnId;
    }
    ui16 GetChunkIdx() const {
        return Chunk;
    }

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo::TColumnRecord& proto, const TIndexInfo& indexInfo) {
        ColumnId = proto.GetColumnId();
        Chunk = proto.GetChunk();
        {
            auto parse = Meta.DeserializeFromProto(GetAddress(), proto.GetMeta(), indexInfo);
            if (!parse) {
                return parse;
            }
        }
        {
            auto parsed = TBlobRange::BuildFromProto(proto.GetBlobRange());
            if (!parsed) {
                return parsed;
            }
            BlobRange = parsed.DetachResult();
        }
        return TConclusionStatus::Success();
    }

    NKikimrColumnShardDataSharingProto::TPortionInfo::TColumnRecord SerializeToProto() const {
        NKikimrColumnShardDataSharingProto::TPortionInfo::TColumnRecord result;
        result.SetColumnId(ColumnId);
        result.SetChunk(Chunk);
        *result.MutabletMeta() = Meta.SerializeToProto();
        *result.MutabletBlobRange() = BlobRange.SerializeToProto();
        return result;
    }

    TColumnSerializationStat GetSerializationStat(const std::string& columnName) const {
        TColumnSerializationStat result(ColumnId, columnName);
        result.Merge(GetSerializationStat());
        return result;
    }

    TSimpleSerializationStat GetSerializationStat() const {
        return TSimpleSerializationStat(BlobRange.Size, Meta.GetNumRowsVerified(), Meta.GetRawBytesVerified());
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

class TSimpleOrderedColumnChunk: public IPortionColumnChunk {
private:
    using TBase = IPortionColumnChunk;
    const TColumnRecord ColumnRecord;
    YDB_READONLY_DEF(TString, Data);
protected:
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "column_id=" << GetColumnId() << ";chunk=" << GetChunkIdx() << ";data_size=" << Data.size() << ";";
    }

    virtual const TString& DoGetData() const override {
        return Data;
    }
    virtual ui32 DoGetRecordsCountImpl() const override {
        return ColumnRecord.GetMeta().GetNumRowsVerified();
    }
    virtual std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplitImpl(const TColumnSaver& /*saver*/, const std::shared_ptr<NColumnShard::TSplitterCounters>& /*counters*/,
                                                                                const std::vector<ui64>& /*splitSizes*/) const override {
        Y_ABORT_UNLESS(false);
        return {};
    }
    virtual TSimpleChunkMeta DoBuildSimpleChunkMeta() const override {
        return ColumnRecord.GetMeta();
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetFirstScalar() const override {
        return nullptr;
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetLastScalar() const override {
        return nullptr;
    }
public:
    TSimpleOrderedColumnChunk(const TColumnRecord& cRecord, const TString& data)
        : TBase(cRecord.ColumnId, cRecord.Chunk)
        , ColumnRecord(cRecord)
        , Data(data) {
    }
};

}
