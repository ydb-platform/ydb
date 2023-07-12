#include "chunks_limiter.h"
#include <util/string/builder.h>

namespace NKikimr {

bool TChunksLimiter::HasMore() const {
    return RemainedBytes && RemainedChunksCount;
}

TString TChunksLimiter::DebugString() const {
    return TStringBuilder() << "limits:(bytes=" << RemainedBytes << ";chunks=" << RemainedChunksCount << ");";
}

bool TChunksLimiter::Take(const ui64 bytes) {
    if (!HasMore()) {
        return false;
    }
    if (RemainedBytes > bytes && RemainedChunksCount > 1) {
        RemainedBytes -= bytes;
        RemainedChunksCount -= 1;
    } else {
        RemainedBytes = 0;
        RemainedChunksCount = 0;
    }
    return true;
}

}
