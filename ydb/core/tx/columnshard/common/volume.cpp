#include "volume.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

TBlobsVolume TBlobsVolume::operator-(const TBlobsVolume& item) const {
    YDB_LOG_VERIFY(item.BlobBytes <= BlobBytes);
    YDB_LOG_VERIFY(item.RawBytes <= RawBytes);
    return TBlobsVolume(BlobBytes - item.BlobBytes, RawBytes - item.RawBytes);
}

void TBlobsVolume::operator-=(const TBlobsVolume& item) {
    YDB_LOG_VERIFY(item.BlobBytes <= BlobBytes);
    YDB_LOG_VERIFY(item.RawBytes <= RawBytes);
    BlobBytes -= item.BlobBytes;
    RawBytes -= item.RawBytes;
}

}   // namespace NKikimr::NOlap
