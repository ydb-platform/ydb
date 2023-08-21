#pragma once
#include "simple.h"
#include <ydb/core/tx/columnshard/counters/splitter.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

class TSplitSettings {
private:
    static const inline i64 DefaultMaxBlobSize = 8 * 1024 * 1024;
    static const inline i64 DefaultMinBlobSize = 4 * 1024 * 1024;
    static const inline i64 DefaultMinRecordsCount = 10000;
    static const inline i64 DefaultMaxPortionSize = 4 * DefaultMaxBlobSize;
    YDB_ACCESSOR(i64, MaxBlobSize, DefaultMaxBlobSize);
    YDB_ACCESSOR(i64, MinBlobSize, DefaultMinBlobSize);
    YDB_ACCESSOR(i64, MinRecordsCount, DefaultMinRecordsCount);
    YDB_ACCESSOR(i64, MaxPortionSize, DefaultMaxPortionSize);
public:
};

class TSplittedColumn;

class TSplittedColumnChunk {
private:
    TSaverSplittedChunk Data;
    std::vector<ui64> SplitSizes;
    YDB_READONLY(ui32, ColumnId, 0);
public:
    void AddSplit(const ui64 size) {
        SplitSizes.emplace_back(size);
    }
    std::vector<TSplittedColumnChunk> InternalSplit(const TColumnSaver& saver, std::shared_ptr<NColumnShard::TSplitterCounters> counters);

    i64 GetSize() const {
        return Data.GetSerializedChunk().size();
    }

    const TSaverSplittedChunk& GetData() const {
        return Data;
    }

    TSplittedColumnChunk() = default;

    TSplittedColumnChunk(const ui32 columnId, const TSaverSplittedChunk& data)
        : Data(data)
        , ColumnId(columnId)
    {

    }
};

class TSplittedColumn {
private:
    YDB_READONLY(ui64, Size, 0);
    YDB_READONLY_DEF(TString, ColumnName);
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Field>, Field);
    YDB_READONLY_DEF(std::vector<TSplittedColumnChunk>, Chunks);
public:
    void Merge(TSplittedColumn&& c) {
        Size += c.Size;
        Y_VERIFY(ColumnName == c.ColumnName);
        Y_VERIFY(ColumnId == c.ColumnId);
        for (auto&& i : c.Chunks) {
            Chunks.emplace_back(std::move(i));
        }
    }

    void SetBlobs(const std::vector<TSaverSplittedChunk>& data) {
        Y_VERIFY(Chunks.empty());
        for (auto&& i : data) {
            Y_VERIFY(i.IsCompatibleColumn(Field));
            Size += i.GetSerializedChunk().size();
            Chunks.emplace_back(ColumnId, i);
        }
    }

    TSplittedColumn(std::shared_ptr<arrow::Field> f, const ui32 id)
        : ColumnName(TString(f->name().data(), f->name().size()))
        , ColumnId(id)
        , Field(f)
    {

    }

    bool operator<(const TSplittedColumn& item) const {
        return Size > item.Size;
    }
};

class TSplittedBlob {
private:
    YDB_READONLY(i64, Size, 0);
    YDB_READONLY_DEF(std::vector<TSplittedColumnChunk>, Chunks);
public:
    void Take(const TSplittedColumnChunk& chunk) {
        Chunks.emplace_back(chunk);
        Size += chunk.GetSize();
    }
    bool operator<(const TSplittedBlob& item) const {
        return Size > item.Size;
    }
};

class TSimpleOrderedColumnChunk {
private:
    TChunkAddress ChunkAddress;
    YDB_READONLY_DEF(TString, Data);
    YDB_READONLY(ui64, Offset, 0);
public:
    ui64 GetSize() const {
        return Data.size();
    }

    const TChunkAddress& GetChunkAddress() const {
        return ChunkAddress;
    }

    TSimpleOrderedColumnChunk(const TChunkAddress& chunkAddress, const ui64 offset, const TString& data)
        : ChunkAddress(chunkAddress)
        , Data(std::move(data))
        , Offset(offset) {

    }

    TString DebugString() const;
};

class TOrderedColumnChunk: public TSimpleOrderedColumnChunk {
private:
    using TBase = TSimpleOrderedColumnChunk;
    std::shared_ptr<arrow::Array> Column;
public:
    std::shared_ptr<arrow::Array> GetColumn() const {
        return Column;
    }

    ui32 GetRecordsCount() const {
        return Column->length();
    }

    TOrderedColumnChunk(const TChunkAddress& chunkAddress, const ui64 offset, const TString& data, std::shared_ptr<arrow::Array> column)
        : TBase(chunkAddress, offset, data)
        , Column(column)
    {
        Y_VERIFY(Column);

    }

    TString DebugString() const;
};

}
