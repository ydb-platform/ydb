#pragma once

#include "vdisk_pdiskctx.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {

    // Turns an ordinary chunk write into PreformattedSectors: lays the payload out in whole sectors of a
    // page-aligned buffer and seals them (copy or encryption, canary, footer with nonce and hash) in the
    // calling actor, so that PDisk writes the buffer without touching the data.
    //
    // Leaves the write as it is, returning false, when sector preparation is off
    // (VDiskControls.EnableVDiskSectorPreparation), the disk uses plain data chunks, the chunk's nonce is
    // unknown (for instance a chunk allocated by the write itself) or the offset does not start a sector.
    // The on-disk result is the same either way.
    bool PrepareChunkWriteSectors(NPDisk::TEvChunkWrite& ev, const TPDiskCtx& pdiskCtx);

} // NKikimr
