#include "vdisk_sector_sealing.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_format_context.h>
#include <ydb/library/actors/util/rc_buf.h>
#include <ydb/library/actors/util/rope.h>

#include <cstdlib>
#include <optional>

namespace NKikimr {

    namespace {

        class TSectorBufferChunk : public IContiguousChunk {
            static constexpr size_t Alignment = 4096;

            char *Ptr = nullptr;
            const size_t Size;
            std::optional<TMemoryConsumer> Consumer;

        public:
            TSectorBufferChunk(size_t size, const TMemoryConsumer *consumer)
                : Size(size)
            {
                void *ptr = nullptr;
                Y_ABORT_UNLESS(posix_memalign(&ptr, Alignment, Max<size_t>(Size, 1)) == 0);
                Ptr = static_cast<char*>(ptr);
                if (consumer) {
                    Consumer.emplace(*consumer);
                    Consumer->Add(Size);
                }
            }

            ~TSectorBufferChunk() {
                if (Consumer) {
                    Consumer->Subtract(Size);
                }
                free(Ptr);
            }

            IContiguousChunk::TPtr Clone() override {
                TIntrusivePtr<TSectorBufferChunk> res = MakeIntrusive<TSectorBufferChunk>(Size,
                    Consumer ? &*Consumer : nullptr);
                memcpy(res->Ptr, Ptr, Size);
                return res;
            }

            TContiguousSpan GetData() const override {
                return {Ptr, Size};
            }

            TMutableContiguousSpan UnsafeGetDataMut() override {
                return {Ptr, Size};
            }

            size_t GetOccupiedMemorySize() const override {
                return Size;
            }
        };

    } // anonymous

    TRcBuf AllocateSectorBuffer(size_t size, const TMemoryConsumer *consumer) {
        return TRcBuf(IContiguousChunk::TPtr(MakeIntrusive<TSectorBufferChunk>(size, consumer)));
    }

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
        TRcBuf raw = AllocateSectorBuffer(rawSize);
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

    bool UseRawSectorReads(const TPDiskCtx& pdiskCtx) {
        return pdiskCtx.FormatContext && !pdiskCtx.FormatContext->IsPlainDataChunks()
            && static_cast<i64>(pdiskCtx.EnableSectorPreparation) != 0;
    }

    void OpenRawSectors(const TPDiskCtx& pdiskCtx, NPDisk::TEvChunkReadResult& msg, ui32 offset, ui32 size,
            TLogoBlobID blobId) {
        if (msg.Status != NKikimrProto::OK || !msg.RawSectors) {
            return;
        }
        const NPDisk::TFormatContextPtr& format = pdiskCtx.FormatContext;
        Y_ABORT_UNLESS(format && size);
        const ui32 payloadSize = format->GetSectorPayloadSize();
        const ui32 sectorSize = format->GetSectorSize();
        const ui32 firstSector = offset / payloadSize;
        Y_ABORT_UNLESS(msg.Offset == firstSector * sectorSize);

        // The buffer was read for this request alone, so it is decrypted in place.
        const ui32 sectors = (offset + size - 1) / payloadSize - firstSector + 1;
        ui8 *raw = msg.Data.RawDataPtr(0, sectors * sectorSize);
        TRcBuf data = TRcBuf::Uninitialized(size); // payload for the reader: no alignment needed
        NPDisk::TFormatContext::TOpener opener(format, msg.ChunkIdx, msg.ChunkNonce, blobId);
        TBufferWithGaps result(offset);
        ui32 done = 0;
        for (ui32 sector = firstSector; done < size; ++sector) {
            const ui32 from = sector == firstSector ? offset % payloadSize : 0;
            const ui32 len = Min(payloadSize - from, size - done);
            ui8 *dst = reinterpret_cast<ui8*>(data.GetDataMut()) + done;
            if (!opener.Open(dst, raw + (ui64)(sector - firstSector) * sectorSize, sector, from, len)) {
                memset(dst, 0, len);
                result.AddGap(sector * payloadSize, (sector + 1) * payloadSize);
            }
            done += len;
        }
        result.SetData(std::move(data));
        msg.Data = std::move(result);
        msg.Offset = offset;
        msg.RawSectors = false;
    }

} // NKikimr
