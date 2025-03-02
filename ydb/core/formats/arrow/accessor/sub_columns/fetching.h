#pragma once

#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/header.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/common/blob.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class THeaderRestoreInfo {
private:
    YDB_ACCESSOR_DEF(std::optional<ui32>, HeaderSize);
    YDB_ACCESSOR_DEF(std::optional<ui32>, FullHeaderSize);
    std::optional<TSubColumnsHeader> Result;

    TString SavedBlob;
    std::optional<NOlap::TBlobRange> RequestedBlobRange;
    TString StorageId;
    NOlap::TBlobRange BlobRange;
    TChunkConstructionData ConstructionData;

public:
    TSubColumnsHeader&& ExtractResult() {
        AFL_VERIFY(!!Result);
        return std::move(*Result);
    }

    TString&& ExtractSavedBlob() {
        return std::move(SavedBlob);
    }

    bool ReadNextBlobRange(
        NOlap::NBlobOperations::NRead::TCompositeReadBlobs& blobs, const std::shared_ptr<NOlap::IBlobsReadingAction>& nextRead) {
        AFL_VERIFY(RequestedBlobRange);
        SavedBlob += blobs.Extract(StorageId, *RequestedBlobRange);
        RequestedBlobRange.reset();

        if (!HeaderSize) {
            HeaderSize.emplace(TConstructor::GetHeaderSize(SavedBlob).DetachResult());
        }

        if (!FullHeaderSize) {
            if (auto fullHeader = TConstructor::GetFullHeaderSize(SavedBlob); fullHeader.IsSuccess()) {
                FullHeaderSize.emplace(fullHeader.DetachResult());
            } else {
                RequestedBlobRange.emplace(BlobRange.BuildSubset(SavedBlob.size(), *HeaderSize - SavedBlob.size()));
                nextRead->AddRange(*RequestedBlobRange);
                return false;
            }
        }

        if (SavedBlob.size() < *FullHeaderSize) {
            RequestedBlobRange.emplace(BlobRange.BuildSubset(SavedBlob.size(), *FullHeaderSize - SavedBlob.size()));
            nextRead->AddRange(*RequestedBlobRange);
            return false;
        }

        Result.emplace(TSubColumnsHeader::ReadHeader(SavedBlob, ConstructionData).DetachResult());
        return true;
    }

    void StartReading(const std::shared_ptr<NOlap::IBlobsReadingAction>& nextRead) {
        AFL_VERIFY(!RequestedBlobRange);
        RequestedBlobRange.emplace(BlobRange.BuildSubset(0, std::min<ui32>(BlobRange.GetSize(), 4096)));
        nextRead->AddRange(*RequestedBlobRange);
    }

    THeaderRestoreInfo(const TString& storageId, const NOlap::TBlobRange& blobRange, TChunkConstructionData&& constructionData)
        : StorageId(storageId)
        , BlobRange(blobRange)
        , ConstructionData(std::move(constructionData)) {
    }
};

class THeaderFetchingLogic {
private:
    THashMap<NOlap::TBlobRange, THeaderRestoreInfo> Chunks;
    THashMap<NOlap::TBlobRange, TSubColumnsHeader> ReadyChunks;
    std::shared_ptr<NOlap::IBlobsStorageOperator> StorageOperator;

public:
    THeaderFetchingLogic(
        THashMap<NOlap::TBlobRange, TChunkConstructionData>&& chunkRanges, const std::shared_ptr<NOlap::IBlobsStorageOperator>& storageOperator)
        : StorageOperator(storageOperator) {
        AFL_VERIFY(!!StorageOperator);
        for (auto&& [range, data] : chunkRanges) {
            Chunks.emplace(range, THeaderRestoreInfo(StorageOperator->GetStorageId(), range, std::move(data)));
        }
    }

    void Start(NOlap::TReadActionsCollection& nextRead) {
        auto reading = StorageOperator->StartReadingAction(NOlap::NBlobOperations::EConsumer::SCAN);
        reading->SetIsBackgroundProcess(false);
        for (const auto& [range, _] : Chunks) {
            Chunks.FindPtr(range)->StartReading(reading);
        }
        nextRead.Add(reading);
    }

    void OnDataFetched(NOlap::NBlobOperations::NRead::TCompositeReadBlobs& blobs, NOlap::TReadActionsCollection& nextRead) {
        auto reading = StorageOperator->StartReadingAction(NOlap::NBlobOperations::EConsumer::SCAN);
        reading->SetIsBackgroundProcess(false);
        for (auto it = Chunks.begin(); it != Chunks.end();) {
            if (it->second.ReadNextBlobRange(blobs, reading)) {
                ReadyChunks.emplace(it->first, it->second.ExtractResult());
                Chunks.erase(it++);
            } else {
                ++it;
            }
        }
        nextRead.Add(reading);
    }

    bool IsDone() const {
        return Chunks.empty();
    }

    THashMap<NOlap::TBlobRange, TSubColumnsHeader>&& ExtractResults() {
        AFL_VERIFY(IsDone());
        return std::move(ReadyChunks);
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
