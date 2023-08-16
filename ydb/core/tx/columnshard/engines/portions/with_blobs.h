#pragma once
#include "portion_info.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NOlap {

class TPortionInfoWithBlobs {
public:
    class TBlobInfo {
    private:
        YDB_READONLY(ui64, Size, 0);
        YDB_READONLY_DEF(std::vector<TOrderedColumnChunk>, Chunks);
        const ui32 StartRecordsIndex;
        mutable std::optional<TString> ResultBlob;
    public:
        explicit TBlobInfo(const ui32 predictedCount, TPortionInfoWithBlobs& owner)
            : StartRecordsIndex(owner.GetPortionInfo().Records.size())
        {
            Chunks.reserve(predictedCount);
        }

        const TString& GetBlob() const {
            if (!ResultBlob) {
                TString result;
                result.reserve(Size);
                for (auto&& i : Chunks) {
                    result.append(i.GetData());
                }
                ResultBlob = std::move(result);
            }
            return *ResultBlob;
        }

        const TColumnRecord& AddChunk(TPortionInfoWithBlobs& owner, TOrderedColumnChunk&& chunk, const TIndexInfo& info);

        void RegisterBlobId(TPortionInfoWithBlobs& owner, const TUnifiedBlobId& blobId);
    };
private:
    std::map<ui32, ui32> ColumnChunkIds;
    TPortionInfo PortionInfo;
    YDB_READONLY_DEF(std::vector<TBlobInfo>, Blobs);
    mutable std::optional<std::shared_ptr<arrow::RecordBatch>> CachedBatch;
public:
    std::shared_ptr<arrow::RecordBatch> GetBatch(const ISnapshotSchema& data, const ISnapshotSchema& result) const {
        if (!CachedBatch) {
            THashMap<ui32, ui32> chunkIds;
            THashMap<std::pair<ui32, ui32>, TString> blobsByColumnChunk;
            for (auto&& b : Blobs) {
                for (auto&& i : b.GetChunks()) {
                    Y_VERIFY(blobsByColumnChunk.emplace(std::make_pair(i.GetColumnId(), chunkIds[i.GetColumnId()]++), i.GetData()).second);
                }
            }
            THashMap<TBlobRange, TString> blobs;
            for (auto&& i : PortionInfo.Records) {
                auto it = blobsByColumnChunk.find(std::make_pair(i.ColumnId, i.Chunk));
                Y_VERIFY(it != blobsByColumnChunk.end());
                blobs[i.BlobRange] = it->second;
            }
            CachedBatch = PortionInfo.AssembleInBatch(data, result, blobs);
        }
        return *CachedBatch;
    }

    const TString& GetBlobByRangeVerified(const ui32 columnId, const ui32 chunkId) const {
        ui32 columnChunk = 0;
        for (auto&& b : Blobs) {
            for (auto&& i : b.GetChunks()) {
                if (i.GetColumnId() == columnId) {
                    if (columnChunk == chunkId) {
                        return i.GetData();
                    }
                    ++columnChunk;
                }
            }
        }
        Y_VERIFY(false);
    }

    ui64 GetBlobFullSizeVerified(const ui32 columnId, const ui32 chunkId) const {
        ui32 columnChunk = 0;
        for (auto&& b : Blobs) {
            for (auto&& i : b.GetChunks()) {
                if (i.GetColumnId() == columnId) {
                    if (columnChunk == chunkId) {
                        return b.GetSize();
                    }
                    ++columnChunk;
                }
            }
        }
        Y_VERIFY(false);
    }

    TString DebugString() const {
        return TStringBuilder() << PortionInfo.DebugString() << "blobs_count=" << Blobs.size() << ";";
    }

    std::vector<TBlobInfo>& GetBlobs() {
        return Blobs;
    }

    const TPortionInfo& GetPortionInfo() const {
        return PortionInfo;
    }

    TPortionInfo& GetPortionInfo() {
        return PortionInfo;
    }

    void SetPortionInfo(const TPortionInfo& portionInfo) {
        PortionInfo = portionInfo;
    }

    explicit TPortionInfoWithBlobs(TPortionInfo&& portionInfo, const ui32 predictedBlobsCount)
        : PortionInfo(portionInfo) {
        Blobs.reserve(predictedBlobsCount);
    }

    explicit TPortionInfoWithBlobs(const TPortionInfo& portionInfo, const ui32 predictedBlobsCount)
        : PortionInfo(portionInfo) {
        Blobs.reserve(predictedBlobsCount);
    }

    TBlobInfo& StartBlob(const ui32 blobChunksCount) {
        Blobs.emplace_back(TBlobInfo(blobChunksCount, *this));
        return Blobs.back();
    }

    friend IOutputStream& operator << (IOutputStream& out, const TPortionInfoWithBlobs& info) {
        out << info.DebugString();
        return out;
    }
};

} // namespace NKikimr::NOlap
