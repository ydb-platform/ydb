#pragma once
#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr {

class TChunksLimiter {
private:
    ui64 RemainedBytes = 0;
    ui32 RemainedChunksCount = 0;
public:
    TChunksLimiter() = default;
    TChunksLimiter(const ui64 remainedBytes, const ui32 remainedChunksCount)
        : RemainedBytes(remainedBytes)
        , RemainedChunksCount(remainedChunksCount) {

    }

    ui64 GetRemainedBytes() const {
        return RemainedBytes;
    }

    bool HasMore() const;
    TString DebugString() const;
    bool Take(const ui64 bytes);
};
}
