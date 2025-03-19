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
    std::vector<TIndexDataAddress> IndexAddresses;
    const TChunkOriginalData OriginalData;

    class TFetchingState {
    private:
        std::optional<TRangeFetchingState> HeaderFetching;
        YDB_READONLY_DEF(std::vector<TRangeFetchingState>, ColumnsFetching);

    public:
        const TRangeFetchingState& GetHeaderFetching() const {
            AFL_VERIFY(!!HeaderFetching);
            return *HeaderFetching;
        }

        TRangeFetchingState& MutableHeaderFetching() {
            AFL_VERIFY(!!HeaderFetching);
            return *HeaderFetching;
        }

        bool NeedFetchHeader() const {
            return !!HeaderFetching;
        }

        std::vector<TBlobRange> GetRangesToFetch() const {
            std::vector<TBlobRange> result;
            if (!!HeaderFetching) {
                if (!HeaderFetching->HasData()) {
                    AFL_VERIFY(ColumnsFetching.empty());
                    result.emplace_back(HeaderFetching->GetRangeVerified());
                } else {
                    AFL_VERIFY(ColumnsFetching.size());
                }
            }
            for (auto&& i : ColumnsFetching) {
                if (!i.HasData()) {
                    result.emplace_back(i.GetRangeVerified());
                }
            }
            return result;
        }

        TFetchingState(const TRangeFetchingState& headerFetching)
            : HeaderFetching(headerFetching) {
        }
        TFetchingState(const std::vector<TRangeFetchingState>& colsFetching)
            : ColumnsFetching(colsFetching) {
        }
        void FillDataFrom(NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
            if (HeaderFetching) {
                HeaderFetching->FillDataFrom(blobs);
            }
            for (auto&& i : ColumnsFetching) {
                i.FillDataFrom(blobs);
            }
        }
    };

    const ui32 RecordsCount;

    static TFetchingState BuildFetchingState(const TString& storageId, const TChunkOriginalData& originalData,
        const std::shared_ptr<IIndexHeader>& header, const std::vector<TIndexDataAddress>& addresses) {
        std::vector<TRangeFetchingState> colsFetching;
        for (auto&& address : addresses) {
            if (!originalData.HasData()) {
                if (header) {
                    auto addressRange = header->GetAddressRangeOptional(address, originalData.GetBlobRangeVerified());
                    if (!addressRange) {
                        colsFetching.emplace_back(TRangeFetchingState(""));
                    } else {
                        colsFetching.emplace_back(TRangeFetchingState(storageId, *addressRange));
                    }
                } else {
                    auto addressRange =
                        originalData.GetBlobRangeVerified().BuildSubset(0, std::min<ui32>(originalData.GetBlobRangeVerified().GetSize(), 4096));
                    AFL_VERIFY(colsFetching.empty());
                    return TFetchingState(TRangeFetchingState(storageId, addressRange));
                }
            } else {
                const TString& data = originalData.GetDataVerified();
                AFL_VERIFY(header);
                auto addressRange =
                    header->GetAddressRangeOptional(address, TBlobRange(TUnifiedBlobId(0, 0, 0, 0, 0, 0, data.size()), 0, data.size()));
                if (!addressRange) {
                    colsFetching.emplace_back(TRangeFetchingState(""));
                } else {
                    AFL_VERIFY(addressRange->GetOffset() + addressRange->GetSize() <= data.size());
                    auto finalAddressData = data.substr(addressRange->GetOffset(), addressRange->GetSize());
                    colsFetching.emplace_back(TRangeFetchingState(finalAddressData));
                }
            }
        }
        return TFetchingState(colsFetching);
    }

    TFetchingState FetchingState;

public:
    ui32 GetRecordsCount() const {
        return RecordsCount;
    }

    const std::shared_ptr<IIndexHeader>& GetHeader() const {
        AFL_VERIFY(Header);
        return Header;
    }

    void FetchFrom(const std::shared_ptr<IIndexMeta>& indexMeta, const TString& storageId, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
        FetchingState.FillDataFrom(blobs);
        bool wasHeader = !!Header;
        if (!Header) {
            auto size = IIndexHeader::ReadHeaderSize(FetchingState.GetHeaderFetching().GetBlobData(), true).DetachResult();
            if (size <= FetchingState.GetHeaderFetching().GetBlobData().size()) {
                Header = indexMeta->BuildHeader(TChunkOriginalData(FetchingState.GetHeaderFetching().GetBlobData())).DetachResult();
            } else {
                FetchingState.MutableHeaderFetching() = TRangeFetchingState(storageId, OriginalData.GetBlobRangeVerified().BuildSubset(0, size));
            }
        }
        if (!wasHeader && !!Header) {
            FetchingState = BuildFetchingState(storageId, OriginalData, Header, IndexAddresses);
            AFL_VERIFY(!FetchingState.NeedFetchHeader());
            AFL_VERIFY(FetchingState.GetColumnsFetching().size());
        }
    }

    const TChunkOriginalData& GetOriginalData() const {
        return OriginalData;
    }

    const TFetchingState& GetResult() const {
        return FetchingState;
    }

    TFetchingState& MutableResult() {
        return FetchingState;
    }

    TIndexChunkFetching(const TString& storageId, const std::vector<TIndexDataAddress>& addresses, const TChunkOriginalData& originalData,
        const std::shared_ptr<IIndexHeader>& header, const ui32 recordsCount)
        : Header(header)
        , IndexAddresses(addresses)
        , OriginalData(std::move(originalData))
        , RecordsCount(recordsCount)
        , FetchingState(BuildFetchingState(storageId, OriginalData, Header, addresses)) {
    }
};

class TIndexFetcherLogic: public NReader::NCommon::IKernelFetchLogic {
private:
    using TBase = NReader::NCommon::IKernelFetchLogic;
    THashMap<NRequest::TOriginalDataAddress, TIndexDataAddress> DataAddresses;
    THashMap<TIndexDataAddress, std::vector<NRequest::TOriginalDataAddress>> IndexAddresses;
    TString StorageId;
    std::shared_ptr<IIndexMeta> IndexMeta;
    std::vector<TIndexChunkFetching> Fetching;
    std::vector<TIndexDataAddress> IndexAddressesVector;

    virtual void DoStart(TReadActionsCollection& nextRead, NReader::NCommon::TFetchingResultContext& context) override;
    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override;
    virtual void DoOnDataCollected(NReader::NCommon::TFetchingResultContext& context) override;

public:
    TIndexFetcherLogic(const THashSet<NRequest::TOriginalDataAddress>& dataAddress, const std::shared_ptr<IIndexMeta>& indexMeta,
        const std::shared_ptr<IStoragesManager>& storagesManager)
        : TBase(indexMeta->GetIndexId(), storagesManager)
        , IndexMeta(indexMeta) {
        for (auto&& i : dataAddress) {
            const TIndexDataAddress indexAddr(IndexMeta->GetIndexId(), IndexMeta->CalcCategory(i.GetSubColumnName()));
            DataAddresses.emplace(i, indexAddr);
            IndexAddresses[indexAddr].emplace_back(i);
        }
        for (auto&& i : IndexAddresses) {
            IndexAddressesVector.emplace_back(i.first);
        }
        AFL_VERIFY(IndexMeta);
    }
};

}   // namespace NKikimr::NOlap::NIndexes
