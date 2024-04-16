#pragma once
#include "chunk_meta.h"
#include <ydb/core/tx/columnshard/engines/portions/common.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/saver.h>
#include <ydb/core/tx/columnshard/common/blob.h>

namespace NKikimr::NColumnShard {
class TSplitterCounters;
}

namespace NKikimr::NOlap {

class TPortionInfo;

class IPortionDataChunk {
private:
    YDB_READONLY(ui32, EntityId, 0);

    std::optional<ui32> ChunkIdx;

protected:
    ui64 DoGetPackedSize() const {
        return GetData().size();
    }
    virtual const TString& DoGetData() const = 0;
    virtual TString DoDebugString() const = 0;
    virtual std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplit(const TColumnSaver& saver, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const std::vector<ui64>& splitSizes) const = 0;
    virtual bool DoIsSplittable() const = 0;
    virtual std::optional<ui32> DoGetRecordsCount() const = 0;
    virtual std::shared_ptr<arrow::Scalar> DoGetFirstScalar() const = 0;
    virtual std::shared_ptr<arrow::Scalar> DoGetLastScalar() const = 0;
    virtual void DoAddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionInfo& portionInfo) const = 0;
public:
    IPortionDataChunk(const ui32 entityId, const std::optional<ui16>& chunkIdx = {})
        : EntityId(entityId)
        , ChunkIdx(chunkIdx) {
    }

    virtual ~IPortionDataChunk() = default;

    TString DebugString() const {
        return DoDebugString();
    }

    const TString& GetData() const {
        return DoGetData();
    }

    ui64 GetPackedSize() const {
        return DoGetPackedSize();
    }

    std::optional<ui32> GetRecordsCount() const {
        return DoGetRecordsCount();
    }

    ui32 GetRecordsCountVerified() const {
        auto result = DoGetRecordsCount();
        AFL_VERIFY(result);
        return *result;
    }

    std::vector<std::shared_ptr<IPortionDataChunk>> InternalSplit(const TColumnSaver& saver, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const std::vector<ui64>& splitSizes) const {
        return DoInternalSplit(saver, counters, splitSizes);
    }

    bool IsSplittable() const {
        return DoIsSplittable();
    }

    ui16 GetChunkIdx() const {
        AFL_VERIFY(!!ChunkIdx);
        return *ChunkIdx;
    }

    void SetChunkIdx(const ui16 value) {
        ChunkIdx = value;
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

    TChunkAddress GetChunkAddress() const {
        return TChunkAddress(GetEntityId(), GetChunkIdx());
    }

    void AddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionInfo& portionInfo) const {
        AFL_VERIFY(!bRange.IsValid());
        return DoAddIntoPortionBeforeBlob(bRange, portionInfo);
    }
};

}
