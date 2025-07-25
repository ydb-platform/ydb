#pragma once
#include "base_with_blobs.h"
#include "constructor_accessor.h"
#include "data_accessor.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/splitter/blob_info.h>

#include <ydb/library/accessor/accessor.h>

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
            : Operator(bOperator) {
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
    std::optional<TPortionAccessorConstructor> PortionConstructor;
    YDB_READONLY_DEF(std::vector<TBlobInfo>, Blobs);

    explicit TWritePortionInfoWithBlobsConstructor(TPortionAccessorConstructor&& portionConstructor)
        : PortionConstructor(std::move(portionConstructor)) {
        AFL_VERIFY(!PortionConstructor->HaveBlobsData());
    }

    TBlobInfo::TBuilder StartBlob(const std::shared_ptr<IBlobsStorageOperator>& bOperator) {
        Blobs.emplace_back(TBlobInfo(bOperator));
        return TBlobInfo::TBuilder(Blobs.back(), *this);
    }
    friend class TWritePortionInfoWithBlobsResult;

public:
    std::vector<std::shared_ptr<IPortionDataChunk>> GetEntityChunks(const ui32 entityId) const;

    static TWritePortionInfoWithBlobsConstructor BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
        const THashMap<ui32, std::shared_ptr<IPortionDataChunk>>& inplaceChunks, const TInternalPathId granule, const ui64 schemaVersion,
        const TSnapshot& snapshot, const std::shared_ptr<IStoragesManager>& operators, const EPortionType type);

    static TWritePortionInfoWithBlobsConstructor BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
        const THashMap<ui32, std::shared_ptr<IPortionDataChunk>>& inplaceChunks, TPortionAccessorConstructor&& constructor,
        const std::shared_ptr<IStoragesManager>& operators);

    std::vector<TBlobInfo>& GetBlobs() {
        return Blobs;
    }

    TString DebugString() const {
        return TStringBuilder() << "blobs_count=" << Blobs.size() << ";";
    }

    TPortionAccessorConstructor& GetPortionConstructor() {
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
        std::optional<TUnifiedBlobId> BlobId;
        const TString ResultBlob;
        YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, Operator);

    public:
        const TUnifiedBlobId& GetBlobIdVerified() const {
            AFL_VERIFY(BlobId);
            return *BlobId;
        }

        ui64 GetSize() const {
            return ResultBlob.size();
        }

        TBlobInfo(const TString& blobData, TBlobChunks&& chunks, const std::shared_ptr<IBlobsStorageOperator>& stOperator)
            : Chunks(std::move(chunks))
            , ResultBlob(blobData)
            , Operator(stOperator) {
        }

        const TString& GetResultBlob() const {
            return ResultBlob;
        }

        void RegisterBlobId(TWritePortionInfoWithBlobsResult& owner, const TUnifiedBlobId& blobId);
    };

private:
    std::optional<TPortionAccessorConstructor> PortionConstructor;
    std::optional<std::shared_ptr<TPortionDataAccessor>> PortionResult;
    YDB_READONLY_DEF(std::vector<TBlobInfo>, Blobs);

    TString GetBlobByAddressVerified(const ui32 entityId, const ui32 chunkIdx) const {
        for (auto&& i : Blobs) {
            for (auto&& b: i.GetChunks()) {
                if (b.GetEntityId() == entityId && b.GetChunkIdx() == chunkIdx) {
                    auto* recordInfo = GetPortionResult().GetRecordPointer(TChunkAddress(entityId, chunkIdx));
                    AFL_VERIFY(recordInfo);
                    return recordInfo->GetBlobRange().GetBlobData(i.GetResultBlob());
                }
            }
        }
        AFL_VERIFY(false);
        return "";
    }

public:
    TConclusion<std::shared_ptr<NArrow::TGeneralContainer>> RestoreBatch(
        const ISnapshotSchema& data, const ISnapshotSchema& resultSchema, const std::set<ui32>& seqColumns, const bool restoreAbsent) const {
        THashMap<TChunkAddress, TString> blobs;
        NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)(
            "portion_id", GetPortionResult().GetPortionInfo().GetPortionId());
        for (auto&& i : GetPortionResult().GetRecordsVerified()) {
            blobs[i.GetAddress()] = GetBlobByAddressVerified(i.ColumnId, i.Chunk);
            Y_ABORT_UNLESS(blobs[i.GetAddress()].size() == i.BlobRange.Size);
        }
        return GetPortionResult()
            .PrepareForAssemble(data, resultSchema, blobs, {}, restoreAbsent)
            .AssembleToGeneralContainer(seqColumns);
    }

    std::vector<TBlobInfo>& MutableBlobs() {
        return Blobs;
    }

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

    void RegisterFakeBlobIds() {
        ui32 idx = 0;
        for (auto&& i : Blobs) {
            i.RegisterBlobId(*this, TUnifiedBlobId(1, 1, 1, ++idx, 1, 1, i.GetSize()));
        }

    }

    void FinalizePortionConstructor() {
        AFL_VERIFY(!!PortionConstructor);
        AFL_VERIFY(!PortionResult);
        PortionResult = PortionConstructor->Build(true);
        PortionConstructor.reset();
    }

    const TPortionDataAccessor& GetPortionResult() const {
        AFL_VERIFY(!PortionConstructor);
        AFL_VERIFY(!!PortionResult);
        AFL_VERIFY(!!*PortionResult);
        return **PortionResult;
    }

    const std::shared_ptr<TPortionDataAccessor>& GetPortionResultPtr() const {
        AFL_VERIFY(!PortionConstructor);
        AFL_VERIFY(!!PortionResult);
        AFL_VERIFY(!!*PortionResult);
        return *PortionResult;
    }

    TPortionAccessorConstructor& GetPortionConstructor() {
        AFL_VERIFY(!!PortionConstructor);
        AFL_VERIFY(!PortionResult);
        return *PortionConstructor;
    }

    std::shared_ptr<TPortionAccessorConstructor> DetachPortionConstructor() {
        AFL_VERIFY(PortionConstructor);
        return std::make_shared<TPortionAccessorConstructor>(std::move(*PortionConstructor));
    }
};

}   // namespace NKikimr::NOlap
