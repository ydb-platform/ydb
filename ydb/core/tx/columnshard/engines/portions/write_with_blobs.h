#pragma once
#include "base_with_blobs.h"
#include "constructor.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/splitter/blob_info.h>

namespace NKikimr::NOlap {

class TWritePortionInfoWithBlobsResult;

class TWritePortionInfoWithBlobsConstructor: public TBasePortionInfoWithBlobs {
public:
    class TBlobInfo {
    private:
        using TBlobChunks = std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>>;
        YDB_READONLY(ui64, Size, 0);
        YDB_READONLY_DEF(TBlobChunks, Chunks);
        YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, Operator);
        std::vector<std::shared_ptr<IPortionDataChunk>> ChunksOrdered;
        bool Finished = false;
        void AddChunk(TWritePortionInfoWithBlobsConstructor& owner, const std::shared_ptr<IPortionDataChunk>& chunk);
    public:
        TBlobInfo(const std::shared_ptr<IBlobsStorageOperator>& bOperator)
            : Operator(bOperator)
        {

        }

        class TBuilder {
        private:
            TBlobInfo* OwnerBlob;
            TWritePortionInfoWithBlobsConstructor* OwnerPortion;
        public:
            TBuilder(TBlobInfo& blob, TWritePortionInfoWithBlobsConstructor& portion)
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

        std::vector<TChunkAddress> ExtractChunks() {
            std::vector<TChunkAddress> result;
            result.reserve(Chunks.size());
            for (auto&& i : Chunks) {
                result.emplace_back(i.first);
            }
            return result;
        }

        TString ExtractBlob() {
            AFL_VERIFY(!Finished);
            Finished = true;
            TString result;
            result.reserve(Size);
            for (auto&& i : ChunksOrdered) {
                result.append(i->GetData());
            }
            ChunksOrdered.clear();
            return result;
        }
    };
private:
    std::optional<TPortionInfoConstructor> PortionConstructor;
    YDB_READONLY_DEF(std::vector<TBlobInfo>, Blobs);

    explicit TWritePortionInfoWithBlobsConstructor(TPortionInfoConstructor&& portionConstructor)
        : PortionConstructor(std::move(portionConstructor)) {
    }

    TBlobInfo::TBuilder StartBlob(const std::shared_ptr<IBlobsStorageOperator>& bOperator) {
        Blobs.emplace_back(TBlobInfo(bOperator));
        return TBlobInfo::TBuilder(Blobs.back(), *this);
    }
    friend class TWritePortionInfoWithBlobsResult;
public:
    std::vector<std::shared_ptr<IPortionDataChunk>> GetEntityChunks(const ui32 entityId) const;

    static TWritePortionInfoWithBlobsConstructor BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
        const THashMap<ui32, std::shared_ptr<IPortionDataChunk>>& inplaceChunks,
        const ui64 granule, const ui64 schemaVersion, const TSnapshot& snapshot, const std::shared_ptr<IStoragesManager>& operators);

    static TWritePortionInfoWithBlobsConstructor BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
        const THashMap<ui32, std::shared_ptr<IPortionDataChunk>>& inplaceChunks,
        TPortionInfoConstructor&& constructor, const std::shared_ptr<IStoragesManager>& operators);

    std::vector<TBlobInfo>& GetBlobs() {
        return Blobs;
    }

    TString DebugString() const {
        return TStringBuilder() << "blobs_count=" << Blobs.size() << ";";
    }

    TPortionInfoConstructor& GetPortionConstructor() {
        AFL_VERIFY(!!PortionConstructor);
        return *PortionConstructor;
    }

};

class TWritePortionInfoWithBlobsResult {
public:
    class TBlobInfo {
    private:
        using TBlobChunks = std::vector<TChunkAddress>;
        YDB_READONLY_DEF(TBlobChunks, Chunks);
        const TString ResultBlob;
        YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, Operator);

    public:
        ui64 GetSize() const {
            return ResultBlob.size();
        }

        TBlobInfo(const TString& blobData, TBlobChunks&& chunks, const std::shared_ptr<IBlobsStorageOperator>& stOperator)
            : Chunks(std::move(chunks))
            , ResultBlob(blobData)
            , Operator(stOperator)
        {

        }

        const TString& GetResultBlob() const {
            return ResultBlob;
        }

        void RegisterBlobId(TWritePortionInfoWithBlobsResult& owner, const TUnifiedBlobId& blobId) const;
    };
private:
    std::optional<TPortionInfoConstructor> PortionConstructor;
    std::optional<TPortionInfo> PortionResult;
    YDB_READONLY_DEF(std::vector<TBlobInfo>, Blobs);
public:
    TWritePortionInfoWithBlobsResult(TWritePortionInfoWithBlobsConstructor&& constructor)
        : PortionConstructor(std::move(constructor.PortionConstructor)) {
        for (auto&& i : constructor.Blobs) {
            Blobs.emplace_back(i.ExtractBlob(), i.ExtractChunks(), i.GetOperator());
        }
    }

    TString GetBlobByRangeVerified(const ui32 entityId, const ui32 chunkIdx) const;

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
