#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/system/types.h>

namespace NKikimr::NOlap {
class TBlobsVolume {
private:
    YDB_READONLY(ui64, BlobBytes, 0);
    YDB_READONLY(ui64, RawBytes, 0);

public:
    TBlobsVolume(const ui64 blob, const ui64 raw)
        : BlobBytes(blob)
        , RawBytes(raw) {
    }

    TBlobsVolume operator+(const TBlobsVolume& item) const {
        return TBlobsVolume(BlobBytes + item.BlobBytes, RawBytes + item.RawBytes);
    }

    void Clear() {
        BlobBytes = 0;
        RawBytes = 0;
    }

    bool CheckWithMax(const TBlobsVolume& maxLimit) const {
        return BlobBytes < maxLimit.BlobBytes && RawBytes < maxLimit.RawBytes;
    }

    void operator+=(const TBlobsVolume& item) {
        BlobBytes += item.BlobBytes;
        RawBytes += item.RawBytes;
    }

    TBlobsVolume operator-(const TBlobsVolume& item) const;

    void operator-=(const TBlobsVolume& item);
};
}   // namespace NKikimr::NOlap
