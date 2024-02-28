#pragma once
#include "portion_info.h"
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/splitter/blob_info.h>
#include <ydb/core/tx/columnshard/splitter/chunks.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TVersionedIndex;

class TPortionInfoWithBlobs {
public:
    class TBlobInfo {
    private:
        using TBlobChunks = std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>>;
        YDB_READONLY(ui64, Size, 0);
        YDB_READONLY_DEF(TBlobChunks, Chunks);
        YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, Operator);
        std::vector<std::shared_ptr<IPortionDataChunk>> ChunksOrdered;
        mutable std::optional<TString> ResultBlob;
        void AddChunk(TPortionInfoWithBlobs& owner, const std::shared_ptr<IPortionDataChunk>& chunk);
        void RestoreChunk(const TPortionInfoWithBlobs& owner, const std::shared_ptr<IPortionDataChunk>& chunk);

    public:
        TBlobInfo(const std::shared_ptr<IBlobsStorageOperator>& bOperator)
            : Operator(bOperator)
        {

        }

        class TBuilder {
        private:
            TBlobInfo* OwnerBlob;
            TPortionInfoWithBlobs* OwnerPortion;
        public:
            TBuilder(TBlobInfo& blob, TPortionInfoWithBlobs& portion)
                : OwnerBlob(&blob)
                , OwnerPortion(&portion) {
            }
            ui64 GetSize() const {
                return OwnerBlob->GetSize();
            }

            void AddChunk(const std::shared_ptr<IPortionDataChunk>& chunk) {
                return OwnerBlob->AddChunk(*OwnerPortion, chunk);
            }
            void RestoreChunk(const std::shared_ptr<IPortionColumnChunk>& chunk) {
                OwnerBlob->RestoreChunk(*OwnerPortion, chunk);
            }
        };

        const TString& GetBlob() const {
            if (!ResultBlob) {
                TString result;
                result.reserve(Size);
                for (auto&& i : ChunksOrdered) {
                    result.append(i->GetData());
                }
                ResultBlob = std::move(result);
            }
            return *ResultBlob;
        }

        void RegisterBlobId(TPortionInfoWithBlobs& owner, const TUnifiedBlobId& blobId);
        void ExtractEntityChunks(const ui32 entityId, std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>>& resultMap);
    };
private:
    TPortionInfo PortionInfo;
    YDB_READONLY_DEF(std::vector<TBlobInfo>, Blobs);
    mutable std::optional<std::shared_ptr<arrow::RecordBatch>> CachedBatch;

    explicit TPortionInfoWithBlobs(TPortionInfo&& portionInfo, std::optional<std::shared_ptr<arrow::RecordBatch>> batch = {})
        : PortionInfo(std::move(portionInfo))
        , CachedBatch(batch) {
    }

    explicit TPortionInfoWithBlobs(const TPortionInfo& portionInfo, std::optional<std::shared_ptr<arrow::RecordBatch>> batch = {})
        : PortionInfo(portionInfo)
        , CachedBatch(batch) {
    }

    void SetPortionInfo(const TPortionInfo& portionInfo) {
        PortionInfo = portionInfo;
    }

    TBlobInfo::TBuilder StartBlob(const std::shared_ptr<IBlobsStorageOperator>& bOperator) {
        Blobs.emplace_back(TBlobInfo(bOperator));
        return TBlobInfo::TBuilder(Blobs.back(), *this);
    }

public:
    static std::vector<TPortionInfoWithBlobs> RestorePortions(const std::vector<TPortionInfo>& portions, NBlobOperations::NRead::TCompositeReadBlobs& blobs,
        const TVersionedIndex& tables, const std::shared_ptr<IStoragesManager>& operators);
    static TPortionInfoWithBlobs RestorePortion(const TPortionInfo& portions, NBlobOperations::NRead::TCompositeReadBlobs& blobs,
        const TIndexInfo& indexInfo, const std::shared_ptr<IStoragesManager>& operators);

    std::shared_ptr<arrow::RecordBatch> GetBatch(const ISnapshotSchema::TPtr& data, const ISnapshotSchema& result, const std::set<std::string>& columnNames = {}) const;

    std::vector<std::shared_ptr<IPortionDataChunk>> GetEntityChunks(const ui32 entityId) const;

    bool ExtractColumnChunks(const ui32 columnId, std::vector<const TColumnRecord*>& records, std::vector<std::shared_ptr<IPortionDataChunk>>& chunks);

    ui64 GetSize() const {
        return PortionInfo.BlobsBytes();
    }

    static TPortionInfoWithBlobs BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
        std::shared_ptr<arrow::RecordBatch> batch, const ui64 granule, const TSnapshot& snapshot, const std::shared_ptr<IStoragesManager>& operators);

    std::optional<TPortionInfoWithBlobs> ChangeSaver(ISnapshotSchema::TPtr currentSchema, const TSaverContext& saverContext) const;

    const TString& GetBlobByRangeVerified(const ui32 columnId, const ui32 chunkId) const {
        for (auto&& b : Blobs) {
            auto it = b.GetChunks().find(TChunkAddress(columnId, chunkId));
            if (it == b.GetChunks().end()) {
                continue;
            } else {
                return it->second->GetData();
            }
        }
        Y_ABORT_UNLESS(false);
    }

    ui64 GetBlobFullSizeVerified(const ui32 columnId, const ui32 chunkId) const {
        for (auto&& b : Blobs) {
            auto it = b.GetChunks().find(TChunkAddress(columnId, chunkId));
            if (it == b.GetChunks().end()) {
                continue;
            } else {
                return b.GetSize();
            }
        }
        Y_ABORT_UNLESS(false);
    }

    std::vector<TBlobInfo>& GetBlobs() {
        return Blobs;
    }

    TString DebugString() const {
        return TStringBuilder() << PortionInfo.DebugString() << ";blobs_count=" << Blobs.size() << ";";
    }

    const TPortionInfo& GetPortionInfo() const {
        return PortionInfo;
    }

    TPortionInfo& GetPortionInfo() {
        return PortionInfo;
    }

    friend IOutputStream& operator << (IOutputStream& out, const TPortionInfoWithBlobs& info) {
        out << info.DebugString();
        return out;
    }
};

} // namespace NKikimr::NOlap
