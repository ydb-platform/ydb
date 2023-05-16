#pragma once
#include "defs.h"
#include "engines/reader/description.h"
#include <ydb/core/tx/columnshard/engines/predicate/predicate.h>
#include <library/cpp/cache/cache.h>

namespace NKikimr::NOlap {
    struct TIndexInfo;
}

namespace NKikimr::NColumnShard {

using TReadDescription = NOlap::TReadDescription;
using IColumnResolver = NOlap::IColumnResolver;
using NOlap::TWriteId;

std::pair<NOlap::TPredicate, NOlap::TPredicate>
RangePredicates(const TSerializedTableRange& range, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns);

class TBatchCache {
public:
    using TUnifiedBlobId = NOlap::TUnifiedBlobId;
    using TInsertedBatch = std::pair<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>>;

    static constexpr ui32 MAX_COMMITTED_COUNT = 2 * TLimits::MIN_SMALL_BLOBS_TO_INSERT;
    static constexpr ui32 MAX_INSERTED_COUNT = 2 * TLimits::MIN_SMALL_BLOBS_TO_INSERT;
    static constexpr ui64 MAX_TOTAL_SIZE = 2 * TLimits::MIN_BYTES_TO_INSERT;

    TBatchCache()
        : Inserted(MAX_INSERTED_COUNT)
        , Committed(MAX_COMMITTED_COUNT)
    {}

    void Insert(TWriteId writeId, const TUnifiedBlobId& blobId, std::shared_ptr<arrow::RecordBatch>& batch) {
        if (Bytes() + blobId.BlobSize() > MAX_TOTAL_SIZE) {
            return;
        }
        InsertedBytes += blobId.BlobSize();
        Inserted.Insert(writeId, {blobId, batch});
    }

    void Commit(TWriteId writeId) {
        auto it = Inserted.FindWithoutPromote(writeId);
        if (it != Inserted.End()) {
            auto& blobId = it->first;
            InsertedBytes -= blobId.BlobSize();
            CommittedBytes += blobId.BlobSize();

            Committed.Insert(blobId, it->second);
            Inserted.Erase(it);
        }
    }

    void EraseInserted(TWriteId writeId) {
        auto it = Inserted.FindWithoutPromote(writeId);
        if (it != Inserted.End()) {
            InsertedBytes -= (*it).first.BlobSize();
            Inserted.Erase(it);
        }
    }

    void EraseCommitted(const TUnifiedBlobId& blobId) {
        auto it = Committed.FindWithoutPromote(blobId);
        if (it != Committed.End()) {
            CommittedBytes -= blobId.BlobSize();
            Committed.Erase(it);
        }
    }

    TInsertedBatch GetInserted(TWriteId writeId) const {
        auto it = Inserted.Find(writeId);
        if (it != Inserted.End()) {
            return *it;
        }
        return {};
    }

    std::shared_ptr<arrow::RecordBatch> Get(const TUnifiedBlobId& blobId) const {
        auto it = Committed.Find(blobId);
        if (it != Committed.End()) {
            return *it;
        }
        return {};
    }

    ui64 Bytes() const {
        return InsertedBytes + CommittedBytes;
    }

private:
    mutable TLRUCache<TWriteId, TInsertedBatch> Inserted;
    mutable TLRUCache<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>> Committed;
    ui64 InsertedBytes{0};
    ui64 CommittedBytes{0};
};

}
