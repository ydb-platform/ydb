#include "user_data.h"
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

TUserData::TBlobStorageGuard::~TBlobStorageGuard() {
    Singleton<TInsertTableCacheController>()->Return(Data.size());
}

 TUserData::TUserData(const ui64 pathId, const TBlobRange& blobRange, const NKikimrTxColumnShard::TLogicalMetadata& proto,
    const ui64 schemaVersion, const std::optional<TString>& blobData)
    : Meta(proto)
    , BlobRange(blobRange)
    , PathId(pathId)
    , SchemaVersion(schemaVersion) {
    if (blobData && Singleton<TInsertTableCacheController>()->Take(blobData->size())) {
        BlobDataGuard = std::make_shared<TBlobStorageGuard>(*blobData);
    }
}

}
