#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>

namespace NKikimr::NOlap::NIndexes {

class TRangeFetchingState {
private:
    std::optional<TString> StorageId;
    std::optional<TBlobRange> BlobRange;
    std::optional<TString> BlobData;

public:
    bool HasData() const {
        return !!BlobData;
    }

    const TBlobRange& GetRangeVerified() const {
        AFL_VERIFY(BlobRange);
        return *BlobRange;
    }

    const TString& GetBlobData() const {
        AFL_VERIFY(BlobData);
        return *BlobData;
    }

    void FillDataFrom(NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
        AFL_VERIFY(!BlobData);
        AFL_VERIFY(!!BlobRange);
        AFL_VERIFY(!!StorageId);
        BlobData = blobs.Extract(*StorageId, *BlobRange);
    }

    explicit TRangeFetchingState(const TString& storageId, const TBlobRange& range)
        : StorageId(storageId)
        , BlobRange(range) {
        AFL_VERIFY(BlobRange->IsValid());
    }
    explicit TRangeFetchingState(const TString& blobData)
        : BlobData(blobData) {
    }
};

class TIndexChunkFetching {
private:
    std::shared_ptr<IIndexHeader> Header;
    TIndexDataAddress Address;
    TChunkOriginalData OriginalData;
    TRangeFetchingState Result;

    static TRangeFetchingState BuildResult(const TString& storageId, const TChunkOriginalData& originalData,
        const std::shared_ptr<IIndexHeader>& header, const TIndexDataAddress& address) {
        if (!originalData.HasData()) {
            if (header) {
                auto addressRange = header->GetAddressRangeOptional(address, originalData.GetBlobRangeVerified());
                if (!addressRange) {
                    return TRangeFetchingState("");
                } else {
                    return TRangeFetchingState(storageId, *addressRange);
                }
            } else {
                auto addressRange =
                    originalData.GetBlobRangeVerified().BuildSubset(0, std::min<ui32>(originalData.GetBlobRangeVerified().GetSize(), 4096));
                return TRangeFetchingState(storageId, addressRange);
            }
        } else {
            const TString& data = originalData.GetDataVerified();
            AFL_VERIFY(header);
            auto addressRange = header->GetAddressRangeOptional(address, TBlobRange(TUnifiedBlobId(), 0, data.size()));
            if (!addressRange) {
                return TRangeFetchingState("");
            }
            AFL_VERIFY(addressRange->GetOffset() + addressRange->GetSize() <= data.size());
            auto finalAddressData = data.substr(addressRange->GetOffset(), addressRange->GetSize());
            return TRangeFetchingState(finalAddressData);
        }
    }

public:
    const std::shared_ptr<IIndexHeader>& GetHeader() const {
        AFL_VERIFY(Header);
        return Header;
    }

    void FetchFrom(
        const std::shared_ptr<IIndexMeta>& indexMeta, IBlobsReadingAction& nextReadAction, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
        Result.FillDataFrom(blobs);
        bool wasHeader = !!Header;
        if (!Header) {
            auto size = IIndexHeader::ReadHeaderSize(Result.GetBlobData(), true).DetachResult();
            if (size <= Result.GetBlobData().size()) {
                Header = indexMeta->BuildHeader(TChunkOriginalData(Result.GetBlobData())).DetachResult();
            } else {
                Result = TRangeFetchingState(nextReadAction.GetStorageId(), OriginalData.GetBlobRangeVerified().BuildSubset(0, size));
                nextReadAction.AddRange(Result.GetRangeVerified());
            }
        }
        if (!wasHeader && !!Header) {
            auto indexRange = Header->GetAddressRangeOptional(Address, OriginalData.GetBlobRangeVerified());
            if (!indexRange) {
                Result = TRangeFetchingState("");
            } else {
                Result = TRangeFetchingState(nextReadAction.GetStorageId(),
                    OriginalData.GetBlobRangeVerified().BuildSubset(indexRange->GetOffset(), indexRange->GetSize()));
                nextReadAction.AddRange(Result.GetRangeVerified());
            }
        }
    }

    const TChunkOriginalData& GetOriginalData() const {
        return OriginalData;
    }

    const TRangeFetchingState& GetResult() const {
        return Result;
    }

    TRangeFetchingState& MutableResult() {
        return Result;
    }

    TIndexChunkFetching(const TString& storageId, const TIndexDataAddress& address, const TChunkOriginalData& originalData,
        const std::shared_ptr<IIndexHeader>& header)
        : Header(header)
        , Address(address)
        , OriginalData(std::move(originalData))
        , Result(BuildResult(storageId, originalData, Header, address)) {
    }
};

class TIndexFetcherLogic: public NReader::NCommon::IKernelFetchLogic {
private:
    using TBase = NReader::NCommon::IKernelFetchLogic;
    const TIndexDataAddress DataAddress;
    const TString StorageId;
    std::shared_ptr<IIndexMeta> IndexMeta;
    std::vector<TIndexChunkFetching> Fetching;

    virtual void DoStart(TReadActionsCollection& nextRead, NReader::NCommon::TFetchingResultContext& /*context*/) override {
        TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(StorageId);
        for (auto&& i : Fetching) {
            if (!i.GetResult().HasData()) {
                reading->AddRange(i.GetResult().GetRangeVerified());
            }
        }
        nextRead.Add(reading);
    }

    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override {
        TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(StorageId);
        for (auto&& r : Fetching) {
            r.FetchFrom(IndexMeta, *reading, blobs);
        }
        nextRead.Add(reading);
    }
    virtual void DoOnDataCollected(NReader::NCommon::TFetchingResultContext& context) override {
        std::vector<TString> data;
        const bool hasIndex = context.GetIndexes().HasIndex(DataAddress.GetIndexId());

        for (auto&& i : Fetching) {
            data.emplace_back(i.GetResult().GetBlobData());
            if (!hasIndex) {
                context.GetIndexes().StartChunk(DataAddress.GetIndexId(), i.GetHeader());
            }
        }
        context.GetIndexes().AddData(DataAddress.GetIndexId(), DataAddress.GetCategory(), data);
    }

public:
    TIndexFetcherLogic(const TIndexDataAddress& address, std::vector<TIndexChunkFetching>&& fetching,
        const std::shared_ptr<IIndexMeta>& indexMeta, const std::shared_ptr<IStoragesManager>& storagesManager)
        : TBase(address.GetIndexId(), storagesManager)
        , DataAddress(address)
        , IndexMeta(indexMeta)
        , Fetching(std::move(fetching)) {
        AFL_VERIFY(IndexMeta);
        AFL_VERIFY(Fetching.size());
    }
};

}   // namespace NKikimr::NOlap::NIndexes
