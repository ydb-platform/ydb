#pragma once
#include "simple.h"
#include <ydb/core/tx/columnshard/counters/splitter.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

class TSplitSettings {
public:
    static const inline i64 MaxBlobSize = 8 * 1024 * 1024;
    static const inline i64 MaxBlobSizeWithGap = 7 * 1024 * 1024;
    static const inline i64 MinBlobSize = 4 * 1024 * 1024;
    static const inline i64 MinRecordsCount = 10000;
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

    ui64 GetSize() const {
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
    YDB_READONLY(ui64, Size, 0);
    YDB_READONLY_DEF(std::vector<TSplittedColumnChunk>, Chunks);
public:
    bool Take(const TSplittedColumnChunk& chunk) {
        if (Size + chunk.GetSize() < TSplitSettings::MaxBlobSize) {
            Chunks.emplace_back(chunk);
            Size += chunk.GetSize();
            return true;
        }
        return false;
    }
    bool operator<(const TSplittedBlob& item) const {
        return Size > item.Size;
    }
};

class TOrderedColumnChunk {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY_DEF(TString, Data);
public:
    TOrderedColumnChunk(const ui32 columnId, const ui32 recordsCount, const TString& data)
        : ColumnId(columnId)
        , RecordsCount(recordsCount)
        , Data(std::move(data))
    {

    }

    TString DebugString() const {
        return TStringBuilder() << "column_id=" << ColumnId << ";data_size=" << Data.size() << ";records_count=" << RecordsCount << ";";
    }
};

}
