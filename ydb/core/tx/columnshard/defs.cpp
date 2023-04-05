#include "defs.h"

namespace NKikimr::NColumnShard {

namespace {
    const constexpr ui64 MAX_BLOB_SIZE_LIMIT = 8 * 1024 * 1024;
    ui64 MaxBlobSize = MAX_BLOB_SIZE_LIMIT;
}

ui64 TLimits::GetMaxBlobSize() {
    return MaxBlobSize;
}

ui64 TLimits::GetBlobSizeLimit() {
    return MAX_BLOB_SIZE_LIMIT;
}

void TLimits::SetMaxBlobSize(const ui64 value) {
    Y_VERIFY(value <= MAX_BLOB_SIZE_LIMIT);
    MaxBlobSize = value;
}

}
