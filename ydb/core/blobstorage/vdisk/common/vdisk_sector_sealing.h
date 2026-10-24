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

    // Whether a chunk read should ask for raw sectors (TEvChunkRead::RawSectors), to be checked and opened by
    // OpenRawSectors in the reading actor rather than in the PDisk completion thread.
    bool UseRawSectorReads(const TPDiskCtx& pdiskCtx);

    // Turns a successful RawSectors read result into an ordinary one: checks and decrypts its sectors (in place)
    // and leaves in msg.Data the payload [offset, offset + size) the read asked for, with msg.Offset = offset. A
    // sector that does not check out becomes a gap, as when PDisk checks the sectors itself. blobId is the one
    // the read was salted with, if any. Other results are left as they are.
    void OpenRawSectors(const TPDiskCtx& pdiskCtx, NPDisk::TEvChunkReadResult& msg, ui32 offset, ui32 size,
        TLogoBlobID blobId);

} // NKikimr
