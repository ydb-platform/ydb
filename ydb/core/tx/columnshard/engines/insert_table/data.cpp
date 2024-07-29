#include "data.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

namespace {

class TInsertTableCacheController {
private:
    TAtomicCounter BlobsCacheSize = 0;
    const i64 BlobsCacheLimit = (i64)1 << 30;
public:
    void Return(const ui64 size) {
        const i64 val = BlobsCacheSize.Sub(size);
        AFL_VERIFY(val >= 0)("size", size)("val", val);
    }

    bool Take(const ui64 size) {
        if (BlobsCacheSize.Add(size) <= BlobsCacheLimit) {
            return true;
        }
        const i64 val = BlobsCacheSize.Sub(size);
        AFL_VERIFY(val >= 0)("size", size)("val", val);
        return false;
    }
};

}

TInsertedData::TBlobStorageGuard::~TBlobStorageGuard() {
    Singleton<TInsertTableCacheController>()->Return(Data.size());
}

TInsertedData::~TInsertedData() {
}

TInsertedData::TInsertedData(ui64 planStep, ui64 writeTxId, ui64 pathId, TString dedupId, const TBlobRange& blobRange,
    const NKikimrTxColumnShard::TLogicalMetadata& proto, const ui64 schemaVersion, const std::optional<TString>& blobData)
    : Meta(proto)
    , BlobRange(blobRange)
    , PlanStep(planStep)
    , WriteTxId(writeTxId)
    , PathId(pathId)
    , DedupId(dedupId)
    , SchemaVersion(schemaVersion) {
    if (blobData) {
        AFL_VERIFY(blobData->size() == BlobRange.Size);
        if (Singleton<TInsertTableCacheController>()->Take(blobData->size())) {
            BlobDataGuard = std::make_shared<TBlobStorageGuard>(*blobData);
        }
    }
}

}
