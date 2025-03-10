#include "volume.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

TBlobsVolume TBlobsVolume::operator-(const TBlobsVolume& item) const {
    AFL_VERIFY(item.BlobBytes <= BlobBytes);
    AFL_VERIFY(item.RawBytes <= RawBytes);
    return TBlobsVolume(BlobBytes - item.BlobBytes, RawBytes - item.RawBytes);
}

void TBlobsVolume::operator-=(const TBlobsVolume& item) {
    AFL_VERIFY(item.BlobBytes <= BlobBytes);
    AFL_VERIFY(item.RawBytes <= RawBytes);
    BlobBytes -= item.BlobBytes;
    RawBytes -= item.RawBytes;
}

}   // namespace NKikimr::NOlap
