#include "portion_chunks.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/join.h>

#include <algorithm>
#include <deque>
#include <map>

namespace NKikimr::NOlap {

std::shared_ptr<TPortionChunks> TPortionChunks::Extract(const TExtractContext& context) const {
    std::vector<TColumnRecord> extractedRecords;
    if (context.GetColumnIds()) {
        auto itRec = Records.begin();
        auto itExt = context.GetColumnIds()->begin();
        while (itRec != Records.end() && itExt != context.GetColumnIds()->end()) {
            if (itRec->GetEntityId() == *itExt) {
                extractedRecords.emplace_back(*itRec);
                ++itRec;
            } else if (itRec->GetEntityId() < *itExt) {
                ++itRec;
            } else {
                ++itExt;
            }
        }
    } else {
        extractedRecords = Records;
    }

    std::vector<TIndexChunk> extractedIndexes;
    if (context.GetIndexIds()) {
        auto itIdx = Indexes.begin();
        auto itExt = context.GetIndexIds()->begin();
        while (itIdx != Indexes.end() && itExt != context.GetIndexIds()->end()) {
            if (itIdx->GetEntityId() == *itExt) {
                extractedIndexes.emplace_back(*itIdx);
                ++itIdx;
            } else if (itIdx->GetEntityId() < *itExt) {
                ++itIdx;
            } else {
                ++itExt;
            }
        }
    } else {
        extractedIndexes = Indexes;
    }

    std::vector<TUnifiedBlobId> blobIds = GetBlobIds();
    return std::make_shared<TPortionChunks>(Address, std::move(blobIds), std::move(extractedRecords), std::move(extractedIndexes));
}

bool TPortionChunks::HasEntityAddress(const TChunkAddress& address) const {
    {
        auto it = std::lower_bound(
            GetRecordsVerified().begin(), GetRecordsVerified().end(), address, [](const TColumnRecord& item, const TChunkAddress& address) {
                return item.GetAddress() < address;
            });
        if (it != GetRecordsVerified().end() && it->GetAddress() == address) {
            return true;
        }
    }
    {
        auto it = std::lower_bound(
            GetIndexesVerified().begin(), GetIndexesVerified().end(), address, [](const TIndexChunk& item, const TChunkAddress& address) {
                return item.GetAddress() < address;
            });
        if (it != GetIndexesVerified().end() && it->GetAddress() == address) {
            return true;
        }
    }
    return false;
}

const NKikimr::NOlap::TColumnRecord* TPortionChunks::GetRecordPointer(const TChunkAddress& address) const {
    auto it = std::lower_bound(
        GetRecordsVerified().begin(), GetRecordsVerified().end(), address, [](const TColumnRecord& item, const TChunkAddress& address) {
            return item.GetAddress() < address;
        });
    if (it != GetRecordsVerified().end() && it->GetAddress() == address) {
        return &*it;
    }
    return nullptr;
}

TString TPortionChunks::DebugString() const {
    TStringBuilder sb;
    sb << "chunks:(" << GetRecordsVerified().size() << ");";
    if (IS_TRACE_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD)) {
        std::vector<TBlobRange> blobRanges;
        for (auto&& i : GetRecordsVerified()) {
            blobRanges.emplace_back(RestoreBlobRange(i.BlobRange));
        }
        sb << "blobs:" << JoinSeq(",", blobRanges) << ";ranges_count:" << blobRanges.size() << ";";
    }
    return sb << ")";
}

ui64 TPortionChunks::GetColumnRawBytes(const std::set<ui32>& entityIds, const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TColumnRecord& r) {
        sum += r.GetMeta().GetRawBytes();
    };
    AggregateIndexChunksData(aggr, GetRecordsVerified(), &entityIds, validation);
    return sum;
}

ui64 TPortionChunks::GetColumnBlobBytes(const std::set<ui32>& entityIds, const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TColumnRecord& r) {
        sum += r.GetBlobRange().GetSize();
    };
    AggregateIndexChunksData(aggr, GetRecordsVerified(), &entityIds, validation);
    return sum;
}

ui64 TPortionChunks::GetIndexRawBytes(const std::set<ui32>& entityIds, const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetRawBytes();
    };
    AggregateIndexChunksData(aggr, GetIndexesVerified(), &entityIds, validation);
    return sum;
}

ui64 TPortionChunks::GetIndexBlobBytes(const std::set<ui32>& entityIds, const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetDataSize();
    };
    AggregateIndexChunksData(aggr, GetIndexesVerified(), &entityIds, validation);
    return sum;
}

ui64 TPortionChunks::GetIndexBlobBytes(const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetDataSize();
    };
    AggregateIndexChunksData(aggr, GetIndexesVerified(), nullptr, validation);
    return sum;
}

ui64 TPortionChunks::GetIndexRawBytes(const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetRawBytes();
    };
    AggregateIndexChunksData(aggr, GetIndexesVerified(), nullptr, validation);
    return sum;
}

std::vector<const TIndexChunk*> TPortionChunks::GetIndexChunksPointers(const ui32 indexId) const {
    std::vector<const TIndexChunk*> result;
    for (auto&& c : GetIndexesVerified()) {
        if (c.GetIndexId() == indexId) {
            AFL_VERIFY(c.GetChunkIdx() == result.size());
            result.emplace_back(&c);
        }
    }
    return result;
}

std::vector<const TColumnRecord*> TPortionChunks::GetColumnChunksPointers(const ui32 columnId) const {
    std::vector<const TColumnRecord*> result;
    for (auto&& c : GetRecordsVerified()) {
        if (c.ColumnId == columnId) {
            Y_ABORT_UNLESS(c.Chunk == result.size());
            Y_ABORT_UNLESS(c.GetMeta().GetRecordsCount());
            result.emplace_back(&c);
        }
    }
    return result;
}

std::vector<TPortionChunks::TPage> TPortionChunks::BuildPages() const {
    std::vector<TPage> pages;

    struct TPart {
    public:
        const TColumnRecord* Record = nullptr;
        const TIndexChunk* Index = nullptr;
        const ui32 RecordsCount;

        TPart(const TColumnRecord* record, const ui32 recordsCount)
            : Record(record)
            , RecordsCount(recordsCount)
        {
        }

        TPart(const TIndexChunk* record, const ui32 recordsCount)
            : Index(record)
            , RecordsCount(recordsCount)
        {
        }
    };

    std::map<ui32, std::deque<TPart>> entities;
    std::map<ui32, ui32> currentCursor;
    ui32 currentSize = 0;
    ui32 currentId = 0;
    for (auto&& i : GetRecordsVerified()) {
        if (currentId != i.GetColumnId()) {
            currentSize = 0;
            currentId = i.GetColumnId();
        }
        currentSize += i.GetMeta().GetRecordsCount();
        ++currentCursor[currentSize];
        entities[i.GetColumnId()].emplace_back(&i, i.GetMeta().GetRecordsCount());
    }
    for (auto&& i : GetIndexesVerified()) {
        if (currentId != i.GetIndexId()) {
            currentSize = 0;
            currentId = i.GetIndexId();
        }
        currentSize += i.GetRecordsCount();
        ++currentCursor[currentSize];
        entities[i.GetIndexId()].emplace_back(&i, i.GetRecordsCount());
    }
    const ui32 entitiesCount = entities.size();
    ui32 predCount = 0;
    for (auto&& i : currentCursor) {
        if (i.second != entitiesCount) {
            continue;
        }
        std::vector<const TColumnRecord*> records;
        std::vector<const TIndexChunk*> indexes;
        for (auto&& c : entities) {
            ui32 readyCount = 0;
            while (readyCount < i.first - predCount && c.second.size()) {
                if (c.second.front().Record) {
                    records.emplace_back(c.second.front().Record);
                } else {
                    AFL_VERIFY(c.second.front().Index);
                    indexes.emplace_back(c.second.front().Index);
                }
                readyCount += c.second.front().RecordsCount;
                c.second.pop_front();
            }
            AFL_VERIFY(readyCount == i.first - predCount)("ready", readyCount)("cursor", i.first)("pred_cursor", predCount);
        }
        pages.emplace_back(std::move(records), std::move(indexes), i.first - predCount);
        predCount = i.first;
    }
    for (auto&& i : entities) {
        AFL_VERIFY(i.second.empty());
    }
    return pages;
}

ui64 TPortionChunks::GetMinMemoryForReadColumns(const std::optional<std::set<ui32>>& columnIds) const {
    ui32 columnId = 0;
    ui32 chunkIdx = 0;

    struct TDelta {
        i64 BlobBytes = 0;
        i64 RawBytes = 0;

        void operator+=(const TDelta& add) {
            BlobBytes += add.BlobBytes;
            RawBytes += add.RawBytes;
        }
    };

    std::map<ui64, TDelta> diffByPositions;
    ui64 position = 0;
    ui64 RawBytesCurrent = 0;
    ui64 BlobBytesCurrent = 0;
    std::optional<ui32> recordsCount;

    const auto doFlushColumn = [&]() {
        if (!recordsCount && position) {
            recordsCount = position;
        } else {
            AFL_VERIFY(*recordsCount == position);
        }
        if (position) {
            TDelta delta;
            delta.RawBytes = -1 * RawBytesCurrent;
            delta.BlobBytes = -1 * BlobBytesCurrent;
            diffByPositions[position] += delta;
        }
        position = 0;
        chunkIdx = 0;
        RawBytesCurrent = 0;
        BlobBytesCurrent = 0;
    };

    for (auto&& i : GetRecordsVerified()) {
        if (columnIds && !columnIds->contains(i.GetColumnId())) {
            continue;
        }
        if (columnId != i.GetColumnId()) {
            if (columnId) {
                doFlushColumn();
            }
            AFL_VERIFY(i.GetColumnId() > columnId);
            AFL_VERIFY(i.GetChunkIdx() == 0);
            columnId = i.GetColumnId();
        } else {
            AFL_VERIFY(i.GetChunkIdx() == chunkIdx + 1);
        }
        chunkIdx = i.GetChunkIdx();
        TDelta delta;
        delta.RawBytes = -1 * RawBytesCurrent + i.GetMeta().GetRawBytes();
        delta.BlobBytes = -1 * BlobBytesCurrent + i.GetBlobRange().Size;
        diffByPositions[position] += delta;
        position += i.GetMeta().GetRecordsCount();
        RawBytesCurrent = i.GetMeta().GetRawBytes();
        BlobBytesCurrent = i.GetBlobRange().Size;
    }
    if (columnId) {
        doFlushColumn();
    }
    i64 maxRawBytes = 0;
    TDelta current;
    for (auto&& i : diffByPositions) {
        current += i.second;
        AFL_VERIFY(current.BlobBytes >= 0);
        AFL_VERIFY(current.RawBytes >= 0);
        if (maxRawBytes < current.RawBytes) {
            maxRawBytes = current.RawBytes;
        }
    }
    AFL_VERIFY(current.BlobBytes == 0)("real", current.BlobBytes);
    AFL_VERIFY(current.RawBytes == 0)("real", current.RawBytes);
    return maxRawBytes;
}

}   // namespace NKikimr::NOlap
