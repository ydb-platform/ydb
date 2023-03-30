#include "defs.h"

namespace NKikimr::NColumnShard {

namespace {
    std::optional<ui64> BlobSizeForSplit;
}

ui64 TLimits::GetBlobSizeForSplit() {
    return BlobSizeForSplit.value_or(MAX_BLOB_SIZE * 0.95);
}

void TLimits::SetBlobSizeForSplit(const ui64 value) {
    BlobSizeForSplit = value;
}

}
