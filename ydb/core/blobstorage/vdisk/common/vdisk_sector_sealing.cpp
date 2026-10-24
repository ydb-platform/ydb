#include "vdisk_sector_sealing.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_format_context.h>
#include <ydb/library/actors/util/rc_buf.h>
#include <ydb/library/actors/util/rope.h>

namespace NKikimr {

    bool PrepareChunkWriteSectors(NPDisk::TEvChunkWrite& ev, const TPDiskCtx& pdiskCtx) {
        const bool enabled = static_cast<i64>(pdiskCtx.EnableSectorPreparation) != 0;
        if (!enabled || !pdiskCtx.FormatContext || ev.PreformattedSectors || !ev.ChunkIdx || !ev.PartsPtr) {
            return false;
        }
        const NPDisk::TFormatContext& format = *pdiskCtx.FormatContext;
        if (format.IsPlainDataChunks()) {
            return false;
        }
        const ui32 payloadSize = format.GetSectorPayloadSize();
        const ui32 sectorSize = format.GetSectorSize();
        if (ev.Offset % payloadSize) {
            return false;
        }
        const std::optional<ui64> chunkNonce = pdiskCtx.ChunkNonces->Get(ev.ChunkIdx);
        if (!chunkNonce) {
            return false;
        }
        const ui64 totalSize = ev.PartsPtr->ByteSize();
        if (!totalSize) {
            return false;
        }

        const ui32 firstSectorIdx = ev.Offset / payloadSize;
        const ui64 rawSize = format.RawSize(totalSize);
        TRcBuf raw = TRcBuf::UninitializedPageAligned(rawSize);
        ui8 *out = reinterpret_cast<ui8*>(raw.GetDataMut());

        // Gather the payload of each sector at the sector's beginning, then seal it in place. A part with
        // a null pointer is padding, i.e. zeroes, as for the PDisk's own write path.
        NPDisk::TFormatContext::TSealer sealer(pdiskCtx.FormatContext, ev.ChunkIdx, *chunkNonce, ev.BlobId);
        const NPDisk::TEvChunkWrite::IParts& parts = *ev.PartsPtr;
        ui32 sectorIdx = 0;
        ui32 filled = 0;
        for (ui32 i = 0; i < parts.Size(); ++i) {
            auto [data, size] = parts[i];
            const ui8 *src = static_cast<const ui8*>(data);
            while (size) {
                const ui32 n = Min<ui32>(size, payloadSize - filled);
                ui8 *sector = out + (ui64)sectorIdx * sectorSize;
                if (src) {
                    memcpy(sector + filled, src, n);
                    src += n;
                } else {
                    memset(sector + filled, 0, n);
                }
                filled += n;
                size -= n;
                if (filled == payloadSize) {
                    sealer.SealInPlace(sector, payloadSize, firstSectorIdx + sectorIdx);
                    ++sectorIdx;
                    filled = 0;
                }
            }
        }
        if (filled) {
            sealer.SealInPlace(out + (ui64)sectorIdx * sectorSize, filled, firstSectorIdx + sectorIdx);
            ++sectorIdx;
        }
        Y_ABORT_UNLESS((ui64)sectorIdx * sectorSize == rawSize);

        ev.PartsPtr = MakeIntrusive<NPDisk::TEvChunkWrite::TRopeAlignedParts>(TRope(std::move(raw)), rawSize);
        ev.PreformattedSectors = true;
        ev.FirstSectorNonce = *chunkNonce + firstSectorIdx;
        return true;
    }

} // NKikimr
