#pragma once

#include "common.h"

#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/splitter/stats.h>
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/columnshard/splitter/chunk_meta.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <util/string/builder.h>

namespace NKikimrColumnShardDataSharingProto {
class TColumnRecord;
}

namespace NKikimr::NOlap {
class TColumnChunkLoadContext;
struct TIndexInfo;
class TColumnRecord;

struct TChunkMeta: public TSimpleChunkMeta {
private:
    using TBase = TSimpleChunkMeta;
    TChunkMeta() = default;
    [[nodiscard]] TConclusionStatus DeserializeFromProto(const TChunkAddress& address, const NKikimrTxColumnShard::TIndexColumnMeta& proto, const TSimpleColumnInfo& columnInfo);
    friend class TColumnRecord;
public:
    TChunkMeta(TSimpleChunkMeta&& baseMeta)
        : TBase(baseMeta)
    {

    }

    [[nodiscard]] static TConclusion<TChunkMeta> BuildFromProto(const TChunkAddress& address, const NKikimrTxColumnShard::TIndexColumnMeta& proto, const TSimpleColumnInfo& columnInfo) {
        TChunkMeta result;
        auto parse = result.DeserializeFromProto(address, proto, columnInfo);
        if (!parse) {
            return parse;
        }
        return result;
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

    TChunkMeta(const TColumnChunkLoadContext& context, const TSimpleColumnInfo& columnInfo);

    TChunkMeta(const std::shared_ptr<arrow::Array>& column, const TSimpleColumnInfo& columnInfo);
};

class TColumnRecord {
private:
    TChunkMeta Meta;
    TColumnRecord(TChunkMeta&& meta)
        : Meta(std::move(meta))
    {

    }

    TColumnRecord() = default;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TColumnRecord& proto, const TSimpleColumnInfo& columnInfo);
public:
    ui32 ColumnId = 0;
    ui16 Chunk = 0;
    TBlobRangeLink16 BlobRange;

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
    const TBlobRangeLink16& GetBlobRange() const {
        return BlobRange;
    }

    NKikimrColumnShardDataSharingProto::TColumnRecord SerializeToProto() const;
    static TConclusion<TColumnRecord> BuildFromProto(const NKikimrColumnShardDataSharingProto::TColumnRecord& proto, const TSimpleColumnInfo& columnInfo) {
        TColumnRecord result;
        auto parse = result.DeserializeFromProto(proto, columnInfo);
        if (!parse) {
            return parse;
        }
        return result;
    }

    TColumnSerializationStat GetSerializationStat(const std::string& columnName) const {
        TColumnSerializationStat result(ColumnId, columnName);
        result.Merge(GetSerializationStat());
        return result;
    }

    TSimpleSerializationStat GetSerializationStat() const {
        return TSimpleSerializationStat(BlobRange.Size, Meta.GetNumRows(), Meta.GetRawBytes());
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
        return ColumnId && BlobRange.IsValid();
    }

    TString DebugString() const {
        return TStringBuilder()
            << "column_id:" << ColumnId << ";"
            << "chunk_idx:" << Chunk << ";"
            << "blob_range:" << BlobRange.ToString() << ";"
            ;
    }

    TColumnRecord(const TChunkAddress& address, const std::shared_ptr<arrow::Array>& column, const TSimpleColumnInfo& columnInfo);

    TColumnRecord(const TBlobRangeLink16::TLinkId blobLinkId, const TColumnChunkLoadContext& loadContext, const TSimpleColumnInfo& columnInfo);

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
        TStringBuilder sb;
        sb << "column_id=" << GetColumnId() << ";data_size=" << Data.size() << ";";
        if (GetChunkIdxOptional()) {
            sb << "chunk=" << GetChunkIdxVerified() << ";";
        } else {
            sb << "chunk=NO_INITIALIZED;";
        }
        return sb;
    }

    virtual const TString& DoGetData() const override {
        return Data;
    }
    virtual ui64 DoGetRawBytesImpl() const override {
        return ColumnRecord.GetMeta().GetRawBytes();
    }
    virtual ui32 DoGetRecordsCountImpl() const override {
        return ColumnRecord.GetMeta().GetNumRows();
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
