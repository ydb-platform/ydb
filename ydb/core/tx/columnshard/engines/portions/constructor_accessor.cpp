#include "constructor_accessor.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

void TPortionAccessorConstructor::ChunksValidation() const {
    AFL_VERIFY(Records.size());
    CheckChunksOrder(Records);
    CheckChunksOrder(Indexes);
    if (BlobIdxs.size()) {
        AFL_VERIFY(BlobIdxs.size() <= Records.size() + Indexes.size())("blobs", BlobIdxs.size())("records", Records.size())(
                                                           "indexes", Indexes.size());
    } else {
        std::set<ui32> blobIdxs;
        for (auto&& i : Records) {
            TBlobRange::Validate(PortionInfo.MetaConstructor.BlobIds, i.GetBlobRange()).Validate();
            blobIdxs.emplace(i.GetBlobRange().GetBlobIdxVerified());
        }
        for (auto&& i : Indexes) {
            if (i.HasBlobRange()) {
                TBlobRange::Validate(PortionInfo.MetaConstructor.BlobIds, i.GetBlobRangeVerified()).Validate();
                blobIdxs.emplace(i.GetBlobRangeVerified().GetBlobIdxVerified());
            }
        }
        if (PortionInfo.MetaConstructor.BlobIds.size()) {
            AFL_VERIFY(PortionInfo.MetaConstructor.BlobIds.size() == blobIdxs.size());
            AFL_VERIFY(PortionInfo.MetaConstructor.BlobIds.size() == *blobIdxs.rbegin() + 1);
        } else {
            AFL_VERIFY(blobIdxs.empty());
        }
    }
}

TPortionDataAccessor TPortionAccessorConstructor::Build(const bool needChunksNormalization) {
    AFL_VERIFY(!Constructed);
    Constructed = true;

    AFL_VERIFY(Records.size());

    PortionInfo.MetaConstructor.ColumnRawBytes = 0;
    PortionInfo.MetaConstructor.ColumnBlobBytes = 0;
    PortionInfo.MetaConstructor.IndexRawBytes = 0;
    PortionInfo.MetaConstructor.IndexBlobBytes = 0;

    PortionInfo.MetaConstructor.RecordsCount = CalcRecordsCount();
    for (auto&& r : Records) {
        *PortionInfo.MetaConstructor.ColumnRawBytes += r.GetMeta().GetRawBytes();
        *PortionInfo.MetaConstructor.ColumnBlobBytes += r.GetBlobRange().GetSize();
    }
    for (auto&& r : Indexes) {
        *PortionInfo.MetaConstructor.IndexRawBytes += r.GetRawBytes();
        *PortionInfo.MetaConstructor.IndexBlobBytes += r.GetDataSize();
    }

    std::shared_ptr<TPortionInfo> result = PortionInfo.Build();

    if (needChunksNormalization) {
        ReorderChunks();
    }
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("portion_id", PortionInfo.GetPortionIdVerified());
    if (BlobIdxs.size()) {
        auto itRecord = Records.begin();
        auto itIndex = Indexes.begin();
        auto itBlobIdx = BlobIdxs.begin();
        while (itRecord != Records.end() && itIndex != Indexes.end() && itBlobIdx != BlobIdxs.end()) {
            if (itRecord->GetAddress() < itIndex->GetAddress()) {
                AFL_VERIFY(itRecord->GetAddress() == itBlobIdx->GetAddress());
                itRecord->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
                ++itRecord;
                ++itBlobIdx;
            } else if (itIndex->GetAddress() < itRecord->GetAddress()) {
                if (itIndex->HasBlobData()) {
                    ++itIndex;
                    continue;
                }
                AFL_VERIFY(itIndex->GetAddress() == itBlobIdx->GetAddress());
                itIndex->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
                ++itIndex;
                ++itBlobIdx;
            } else {
                AFL_VERIFY(false);
            }
        }
        for (; itRecord != Records.end() && itBlobIdx != BlobIdxs.end(); ++itRecord, ++itBlobIdx) {
            AFL_VERIFY(itRecord->GetAddress() == itBlobIdx->GetAddress());
            itRecord->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
        }
        for (; itIndex != Indexes.end() && itBlobIdx != BlobIdxs.end(); ++itIndex) {
            if (itIndex->HasBlobData()) {
                continue;
            }
            AFL_VERIFY(itIndex->GetAddress() == itBlobIdx->GetAddress());
            itIndex->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
            ++itBlobIdx;
        }
        AFL_VERIFY(itRecord == Records.end());
        AFL_VERIFY(itBlobIdx == BlobIdxs.end());
    } else {
        for (auto&& i : Records) {
            AFL_VERIFY(i.BlobRange.GetBlobIdxVerified() < PortionInfo.MetaConstructor.BlobIds.size());
        }
        for (auto&& i : Indexes) {
            if (auto* blobId = i.GetBlobRangeOptional()) {
                AFL_VERIFY(blobId->GetBlobIdxVerified() < PortionInfo.MetaConstructor.BlobIds.size());
            }
        }
    }
    ChunksValidation();

    return TPortionDataAccessor(result, std::move(Records), std::move(Indexes), false);
}

void TPortionAccessorConstructor::LoadRecord(TColumnChunkLoadContextV1&& loadContext) {
    AFL_VERIFY(loadContext.GetBlobRange().GetBlobIdxVerified() < PortionInfo.MetaConstructor.BlobIds.size());
    AFL_VERIFY(loadContext.GetBlobRange().CheckBlob(PortionInfo.MetaConstructor.BlobIds[loadContext.GetBlobRange().GetBlobIdxVerified()]))(
        "blobs", JoinSeq(",", PortionInfo.MetaConstructor.BlobIds))("range", loadContext.GetBlobRange().ToString());
    TColumnRecord rec(loadContext);
    Records.push_back(std::move(rec));
}

void TPortionAccessorConstructor::LoadIndex(TIndexChunkLoadContext&& loadContext) {
    if (loadContext.GetBlobRange()) {
        const TBlobRangeLink16::TLinkId linkBlobId = PortionInfo.GetMeta().GetBlobIdxVerified(loadContext.GetBlobRange()->GetBlobId());
        AddIndex(loadContext.BuildIndexChunk(linkBlobId));
    } else {
        AddIndex(loadContext.BuildIndexChunk());
    }
}

TPortionDataAccessor TPortionAccessorConstructor::BuildForLoading(
    const TPortionInfo::TConstPtr& portion, std::vector<TColumnChunkLoadContextV1>&& records, std::vector<TIndexChunkLoadContext>&& indexes) {
    AFL_VERIFY(portion);
    std::vector<TColumnRecord> recordChunks;
    {
        const auto pred = [](const TColumnRecord& l, const TColumnRecord& r) -> bool {
            return l.GetAddress() < r.GetAddress();
        };
        bool needSort = false;
        for (auto&& i : records) {
            TColumnRecord chunk(i);
            if (recordChunks.size() && !pred(recordChunks.back(), chunk)) {
                needSort = true;
            }
            recordChunks.emplace_back(std::move(chunk));
        }
        if (needSort) {
            std::sort(recordChunks.begin(), recordChunks.end(), pred);
        }
    }
    std::vector<TIndexChunk> indexChunks;
    {

        const auto pred = [](const TIndexChunk& l, const TIndexChunk& r) ->bool {
            return l.GetAddress() < r.GetAddress();
        };
        bool needSort = false;
        for (auto&& i : indexes) {
            auto chunk = i.BuildIndexChunk(*portion);
            if (indexChunks.size() && !pred(indexChunks.back(), chunk)) {
                needSort = true;
            }
            indexChunks.emplace_back(std::move(chunk));
        }
        if (needSort) {
            std::sort(indexChunks.begin(), indexChunks.end(), pred);
        }
    }
    return TPortionDataAccessor(portion, std::move(recordChunks), std::move(indexChunks), true);
}

}   // namespace NKikimr::NOlap
