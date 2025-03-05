#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
namespace NKikimr::NOlap::NIndexes::NCategoriesBloom {

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
        AFL_VERIFY(BlobRange.IsValid());
    }
    explicit TRangeFetchingState(const TString& blobData)
        : BlobData(blobData) {
    }
};

class TIndexChunkFetching {
private:
    std::shared_ptr<IIndexHeader> Header;
    YDB_READONLY_DEF(TChunkOriginalData, OriginalData);
    TRangeFetchingState Result;

    static TRangeFetchingState BuildResult(const TString& storageId, const TChunkOriginalData& originalData,
        const std::shared_ptr<IIndexHeader>& header, const std::shared_ptr<IIndexMeta>& indexMeta) {
        if (!originalData.HasData()) {
            if (header) {
                auto addressRange = header->GetAddressRangeOptional(DataAddress, originalData.GetBlobRangeVerified());
                if (!addressRange) {
                    return TRangeFetchingState("");
                } else {
                    return TRangeFetchingState(storageId, addressRange);
                }
            } else {
                auto addressRange = range->BuildSubset(0, std::min(originalData.GetBlobRangeVerified().GetSize(), 4096));
                return TRangeFetchingState(storageId, addressRange);
            }
        } else {
            const TString& data = originalData.GetDataVerified().size();
            AFL_VERIFY(header);
            auto addressRange = header->GetAddressRangeOptional(DataAddress, TBlobRange(TUnifiedBlobId(), 0, data.size()));
            if (!addressRange) {
                return TRangeFetchingState("");
            }
            AFL_VERIFY(addressRange->GetOffset() + addressRange->GetSize() <= data.size());
            auto finalAddressData = data->substr(addressRange->GetOffset(), addressRange->GetSize());
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
                Header = IndexMeta->BuildHeader(Result.GetBlobData()).DetachResult();
            } else {
                Result = TRangeFetchingState(range->BuildSubset(0, size));
                nextReadAction.AddRange(Result.GetRangeVerified());
            }
        }
        if (!wasHeader) {
            auto indexRange = header->GetAddressRangeOptional(DataAddress);
            if (!indexRange) {
                Result = TRangeFetchingState("");
            } else {
                Result = TRangeFetchingState(OriginalData.GetBlobRangeVerified().BuildSubset(indexRange->GetOffset(), indexRange->GetSize()));
                nextReadAction.AddRange(Result.GetRangeVerified());
            }
        }
    }

    const TRangeFetchingState& GetResult() const {
        return Result;
    }

    TRangeFetchingState& MutableResult() {
        return Result;
    }

    TIndexChunkFetching(const TString& storageId, const TChunkOriginalData& originalData, std::shared_ptr<IIndexHeader>& header,
        const std::shared_ptr<IIndexMeta>& indexMeta)
        : OriginalData(std::move(originalData))
        , Header(header ? header : indexMeta->BuildHeader(originalData).DetachResult())
        , Result(BuildResult(storageId, originalData, Header, indexMeta)) {
    }
};

class TIndexFetcherLogic: public NReader::NCommon::IKernelFetchLogic {
private:
    const TIndexDataAddress DataAddress;
    const TString StorageId;
    std::shared_ptr<IIndexMeta> IndexMeta;
    std::vector<TIndexChunkFetching> Fetching;

    virtual void DoStart(TReadActionsCollection& nextRead) override {
        TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(StorageId);
        for (auto&& i : Fetching) {
            if (!i.HasData()) {
                reading->AddRange(i.GetRangeVerified());
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
    virtual void DoOnDataCollected(TIndexesCollection& collection) override {
        std::vector<TString> data;
        const bool hasIndex = collection.HasIndex(DataAddress.GetIndexId());

        for (auto&& i : Fetching) {
            data.emplace_back(i.GetResult().GetBlobData());
            if (!hasIndex) {
                collection.StartChunk(DataAddress.GetIndexId(), i.GetHeader());
            }
        }
        collection.AddData(DataAddress, i.GetResult().GetBlobData());
    }

public:
    TIndexFetcherLogic(
        const TIndexDataAddress& address, std::vector<TIndexChunkFetching>&& fetching, const std::shared_ptr<IIndexMeta>& indexMeta)
        : DataAddress(address)
        , IndexMeta(indexMeta)
        , Fetching(std::move(fetching)) {
        AFL_VERIFY(IndexMeta);
        AFL_VERIFY(Fetching.size());
    }
};

}   // namespace NKikimr::NOlap::NIndexes
