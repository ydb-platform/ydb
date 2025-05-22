#include "defs.h"

namespace NKikimr::NColumnShard {


void TLimits::SetMaxBlobSize(const ui64 value) {
    Y_ABORT_UNLESS(value <= MAX_BLOB_SIZE_LIMIT);
    MaxBlobSize = value;
}

TLimits::TLimits()
    : MinInsertBytes(MIN_BYTES_TO_INSERT, 1, 2 * MAX_BYTES_TO_INSERT)
    , MaxInsertBytes(15 * MAX_BYTES_TO_INSERT, 0, 30 * MAX_BYTES_TO_INSERT)
    , InsertTableSize(MIN_SMALL_BLOBS_TO_INSERT, 0, 1000)
{

}

}
