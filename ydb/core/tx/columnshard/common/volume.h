#pragma once
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

    TBlobsVolume operator-(const TBlobsVolume& item) const {
        AFL_VERIFY(item.BlobBytes <= BlobBytes);
        AFL_VERIFY(item.RawBytes <= RawBytes);
        return TBlobsVolume(BlobBytes - item.BlobBytes, RawBytes - item.RawBytes);
    }

    void operator-=(const TBlobsVolume& item) {
        AFL_VERIFY(item.BlobBytes <= BlobBytes);
        AFL_VERIFY(item.RawBytes <= RawBytes);
        BlobBytes -= item.BlobBytes;
        RawBytes -= item.RawBytes;
    }
};
}   // namespace NKikimr::NOlap
