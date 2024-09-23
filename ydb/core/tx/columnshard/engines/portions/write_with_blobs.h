#pragma once
#include "base_with_blobs.h"
#include "constructor.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/splitter/blob_info.h>

namespace NKikimr::NOlap {

class TWritePortionInfoWithBlobs: public TBasePortionInfoWithBlobs {
public:
    class TBlobInfo {
    private:
        using TBlobChunks = std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>>;
        YDB_READONLY(ui64, Size, 0);
        YDB_READONLY_DEF(TBlobChunks, Chunks);
        YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, Operator);
        std::vector<std::shared_ptr<IPortionDataChunk>> ChunksOrdered;
        mutable std::optional<TString> ResultBlob;
        void AddChunk(TWritePortionInfoWithBlobs& owner, const std::shared_ptr<IPortionDataChunk>& chunk);

    public:
        TBlobInfo(const std::shared_ptr<IBlobsStorageOperator>& bOperator)
            : Operator(bOperator)
        {

        }

        class TBuilder {
        private:
            TBlobInfo* OwnerBlob;
            TWritePortionInfoWithBlobs* OwnerPortion;
        public:
            TBuilder(TBlobInfo& blob, TWritePortionInfoWithBlobs& portion)
                : OwnerBlob(&blob)
                , OwnerPortion(&portion) {
            }
            ui64 GetSize() const {
                return OwnerBlob->GetSize();
            }

            void AddChunk(const std::shared_ptr<IPortionDataChunk>& chunk) {
                return OwnerBlob->AddChunk(*OwnerPortion, chunk);
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

        void RegisterBlobId(TWritePortionInfoWithBlobs& owner, const TUnifiedBlobId& blobId);
    };
private:
    std::optional<TPortionInfoConstructor> PortionConstructor;
    std::optional<TPortionInfo> PortionResult;
    YDB_READONLY_DEF(std::vector<TBlobInfo>, Blobs);

    explicit TWritePortionInfoWithBlobs(TPortionInfoConstructor&& portionConstructor)
        : PortionConstructor(std::move(portionConstructor)) {
    }

    TBlobInfo::TBuilder StartBlob(const std::shared_ptr<IBlobsStorageOperator>& bOperator) {
        Blobs.emplace_back(TBlobInfo(bOperator));
        return TBlobInfo::TBuilder(Blobs.back(), *this);
    }

public:
    std::vector<std::shared_ptr<IPortionDataChunk>> GetEntityChunks(const ui32 entityId) const;

    void FillStatistics(const TIndexInfo& index);

    static TWritePortionInfoWithBlobs BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
        const ui64 granule, const ui64 schemaVersion, const TSnapshot& snapshot, const std::shared_ptr<IStoragesManager>& operators);

    static TWritePortionInfoWithBlobs BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
        TPortionInfoConstructor&& constructor, const std::shared_ptr<IStoragesManager>& operators);

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
        return TStringBuilder() << "blobs_count=" << Blobs.size() << ";";
    }

    void FinalizePortionConstructor() {
        AFL_VERIFY(!!PortionConstructor);
        AFL_VERIFY(!PortionResult);
        PortionResult = PortionConstructor->Build(true);
        PortionConstructor.reset();
    }

    const TPortionInfo& GetPortionResult() const {
        AFL_VERIFY(!PortionConstructor);
        AFL_VERIFY(!!PortionResult);
        return *PortionResult;
    }

    TPortionInfoConstructor& GetPortionConstructor() {
        AFL_VERIFY(!!PortionConstructor);
        AFL_VERIFY(!PortionResult);
        return *PortionConstructor;
    }

};

} // namespace NKikimr::NOlap
