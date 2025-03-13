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
    const TChunkOriginalData OriginalData;
    TRangeFetchingState Result;
    const ui32 RecordsCount;

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
            auto addressRange =
                header->GetAddressRangeOptional(address, TBlobRange(TUnifiedBlobId(0, 0, 0, 0, 0, 0, data.size()), 0, data.size()));
            if (!addressRange) {
                return TRangeFetchingState("");
            }
            AFL_VERIFY(addressRange->GetOffset() + addressRange->GetSize() <= data.size());
            auto finalAddressData = data.substr(addressRange->GetOffset(), addressRange->GetSize());
            return TRangeFetchingState(finalAddressData);
        }
    }

public:
    ui32 GetRecordsCount() const {
        return RecordsCount;
    }

    const std::shared_ptr<IIndexHeader>& GetHeader() const {
        AFL_VERIFY(Header);
        return Header;
    }

    void FetchFrom(
        const std::shared_ptr<IIndexMeta>& indexMeta, const TString& storageId, std::vector<TBlobRange>& nextReads, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
        Result.FillDataFrom(blobs);
        bool wasHeader = !!Header;
        if (!Header) {
            auto size = IIndexHeader::ReadHeaderSize(Result.GetBlobData(), true).DetachResult();
            if (size <= Result.GetBlobData().size()) {
                Header = indexMeta->BuildHeader(TChunkOriginalData(Result.GetBlobData())).DetachResult();
            } else {
                Result = TRangeFetchingState(storageId, OriginalData.GetBlobRangeVerified().BuildSubset(0, size));
                nextReads.emplace_back(Result.GetRangeVerified());
            }
        }
        if (!wasHeader && !!Header) {
            auto indexRange = Header->GetAddressRangeOptional(Address, OriginalData.GetBlobRangeVerified());
            if (!indexRange) {
                Result = TRangeFetchingState("");
            } else {
                Result = TRangeFetchingState(storageId, *indexRange);
                nextReads.emplace_back(Result.GetRangeVerified());
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
        const std::shared_ptr<IIndexHeader>& header, const ui32 recordsCount)
        : Header(header)
        , Address(address)
        , OriginalData(std::move(originalData))
        , Result(BuildResult(storageId, OriginalData, Header, address))
        , RecordsCount(recordsCount) {
    }
};

class TIndexFetcherLogic: public NReader::NCommon::IKernelFetchLogic {
private:
    using TBase = NReader::NCommon::IKernelFetchLogic;
    const NRequest::TOriginalDataAddress DataAddress;
    const TIndexDataAddress IndexAddress;
    TString StorageId;
    std::shared_ptr<IIndexMeta> IndexMeta;
    std::vector<TIndexChunkFetching> Fetching;

    virtual void DoStart(TReadActionsCollection& nextRead, NReader::NCommon::TFetchingResultContext& context) override;
    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override;
    virtual void DoOnDataCollected(NReader::NCommon::TFetchingResultContext& context) override;

public:
    TIndexFetcherLogic(const NRequest::TOriginalDataAddress& dataAddress, const TIndexDataAddress& indexAddress,
        const std::shared_ptr<IIndexMeta>& indexMeta, const std::shared_ptr<IStoragesManager>& storagesManager)
        : TBase(indexAddress.GetIndexId(), storagesManager)
        , DataAddress(dataAddress)
        , IndexAddress(indexAddress)
        , IndexMeta(indexMeta) {
        AFL_VERIFY(IndexMeta);
    }
};

}   // namespace NKikimr::NOlap::NIndexes
