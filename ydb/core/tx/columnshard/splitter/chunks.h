#pragma once
#include "chunk_meta.h"
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/counters/splitter.h>
#include <ydb/core/tx/columnshard/engines/portions/common.h>

namespace NKikimr::NOlap {

class IPortionColumnChunk {
public:
    using TPtr = std::shared_ptr<IPortionColumnChunk>;
protected:
    ui32 ColumnId = 0;
    ui16 ChunkIdx = 0;
    virtual std::vector<IPortionColumnChunk::TPtr> DoInternalSplit(const TColumnSaver& saver, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const std::vector<ui64>& splitSizes) const = 0;
    virtual ui64 DoGetPackedSize() const {
        return GetData().size();
    }
    virtual const TString& DoGetData() const = 0;
    virtual ui32 DoGetRecordsCount() const = 0;
    virtual TString DoDebugString() const = 0;
    virtual TSimpleChunkMeta DoBuildSimpleChunkMeta() const = 0;
    virtual std::shared_ptr<arrow::Scalar> DoGetFirstScalar() const = 0;
    virtual std::shared_ptr<arrow::Scalar> DoGetLastScalar() const = 0;

public:
    IPortionColumnChunk(const ui32 columnId)
        : ColumnId(columnId)
    {

    }
    virtual ~IPortionColumnChunk() = default;

    TChunkAddress GetChunkAddress() const {
        return TChunkAddress(ColumnId, ChunkIdx);
    }

    std::shared_ptr<arrow::Scalar> GetFirstScalar() const {
        auto result = DoGetFirstScalar();
        Y_ABORT_UNLESS(result);
        return result;
    }
    std::shared_ptr<arrow::Scalar> GetLastScalar() const {
        auto result = DoGetLastScalar();
        Y_ABORT_UNLESS(result);
        return result;
    }

    TSimpleChunkMeta BuildSimpleChunkMeta() const {
        return DoBuildSimpleChunkMeta();
    }

    ui32 GetColumnId() const {
        return ColumnId;
    }

    ui16 GetChunkIdx() const {
        return ChunkIdx;
    }

    void SetChunkIdx(const ui16 value) {
        ChunkIdx = value;
    }

    TString DebugString() const {
        return DoDebugString();
    }

    ui32 GetRecordsCount() const {
        return DoGetRecordsCount();
    }

    const TString& GetData() const {
        return DoGetData();
    }

    ui64 GetPackedSize() const {
        return DoGetPackedSize();
    }

    std::vector<IPortionColumnChunk::TPtr> InternalSplit(const TColumnSaver& saver, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const std::vector<ui64>& splitSizes) const;
};
}
