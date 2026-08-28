#pragma once

#include <util/generic/utility.h>
#include <util/generic/ylimits.h>
#include <util/system/align.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <atomic>

namespace NKikimr {

    // Tracks how much space the LogoBlobs Fresh segment is going to need once it is
    // compacted, so that a write can be judged by the color the disk will be in after
    // paying that debt rather than by the color it is in while the write is still cheap.
    //
    // Only LogoBlobs are tracked. Blocks and barriers are index-only records of a few
    // dozen bytes; a whole Fresh segment of them is kilobytes, but a chunk estimate has
    // to round up to a whole chunk, so counting them would charge two chunks for nothing.
    // Inline blob data is where the space actually goes.
    class TFreshSpaceTracker {
        const bool Enabled;
        const ui64 ChunkSize;
        const ui32 ChunksPerSst;
        const ui32 TotalPartCount;

        // Worst case for a single in-place record. Used only to bound the space a
        // chunk boundary can waste, never to charge a record: charging every record
        // at this size would refuse user writes far below the real capacity.
        std::atomic<ui64> MaxItemBytes;

        // Bytes admitted into the recovery log that have not reached Fresh yet. Without
        // this a burst of concurrent writes would all project against the same stale
        // segment size and collectively overshoot.
        std::atomic<ui64> InFlightBytes;

        static ui64 SaturatingAdd(ui64 lhs, ui64 rhs) {
            return lhs > Max<ui64>() - rhs ? Max<ui64>() : lhs + rhs;
        }

        static ui64 SaturatingMultiply(ui64 lhs, ui64 rhs) {
            return lhs && rhs > Max<ui64>() / lhs ? Max<ui64>() : lhs * rhs;
        }

        ui64 CalculateMaxItemBytes(ui64 maxInPlacePartSize) const {
            // A single Fresh record may merge with an older DiskBlob. DiskBlob packs
            // all locally available erasure parts into one inline value, while the
            // huge-blob threshold limits each part separately.
            const ui64 inlineData = SaturatingMultiply(maxInPlacePartSize, TotalPartCount);
            const ui64 item = SaturatingAdd(inlineData,
                SaturatingAdd(1024, SaturatingMultiply(TotalPartCount, 32)));
            // The huge-blob threshold keeps in-place items orders of magnitude below
            // a chunk, so this only guards nonsensical configuration from collapsing
            // the usable chunk capacity to nothing.
            return Min(item, ChunkSize / 2);
        }

    public:
        // Fresh stores a DiskBlob per record, and compaction writes it out verbatim
        // next to its index entry. Payload is padded the same way TFreshIndexAndData
        // accounts for it, so the charge matches what the segment will later report.
        static ui64 RecordBytes(ui64 indexBytes, ui64 dataBytes) {
            return SaturatingAdd(indexBytes, AlignUp<ui64>(dataBytes, 8));
        }

        TFreshSpaceTracker(bool enabled, ui64 chunkSize, ui32 chunksPerSst,
                ui64 maxInPlaceLogoBlobSize, ui32 totalPartCount)
            : Enabled(enabled)
            , ChunkSize(Max<ui64>(chunkSize, 1))
            , ChunksPerSst(Max<ui32>(chunksPerSst, 1))
            , TotalPartCount(totalPartCount)
            , MaxItemBytes(CalculateMaxItemBytes(maxInPlaceLogoBlobSize))
            , InFlightBytes(0)
        {}

        bool IsEnabled() const {
            return Enabled;
        }

        // Chunks a compaction would have to write to drain this many bytes of Fresh.
        ui64 CalculateChunks(ui64 bytes) const {
            if (!bytes) {
                return 0;
            }
            // The compaction writer packs items next-fit, so an item that does not
            // fit the tail of a chunk starts a new one. Discounting the chunk
            // capacity by one item keeps this an upper bound.
            const ui64 maxItem = MaxItemBytes.load(std::memory_order_relaxed);
            const ui64 usable = ChunkSize > maxItem ? ChunkSize - maxItem : 1;
            const ui64 chunks = 1 + (bytes - 1) / usable;
            const ui64 batches = 1 + (chunks - 1) / ChunksPerSst;
            return SaturatingMultiply(batches, ChunksPerSst);
        }

        void UpdateMaxInPlaceLogoBlobSize(ui64 bytes) {
            if (!Enabled) {
                return;
            }
            const ui64 maxItem = CalculateMaxItemBytes(bytes);
            ui64 previous = MaxItemBytes.load(std::memory_order_relaxed);
            while (previous < maxItem &&
                    !MaxItemBytes.compare_exchange_weak(previous, maxItem, std::memory_order_relaxed)) {
            }
        }

        // Chunks a Fresh compaction would need if this admission were accepted, on top
        // of what the segment already holds and what is already in flight.
        ui64 GetProjectedChunks(ui64 admittedBytes, ui64 segmentBytes) const {
            if (!Enabled) {
                return 0;
            }
            return CalculateChunks(SaturatingAdd(segmentBytes,
                SaturatingAdd(InFlightBytes.load(std::memory_order_relaxed), admittedBytes)));
        }

        void Admit(ui64 bytes) {
            if (!Enabled || !bytes) {
                return;
            }
            ui64 previous = InFlightBytes.load(std::memory_order_relaxed);
            while (!InFlightBytes.compare_exchange_weak(previous, SaturatingAdd(previous, bytes),
                    std::memory_order_relaxed)) {
            }
        }

        // Called once the bytes have reached Fresh (or the write was abandoned), at
        // which point the segment itself accounts for them.
        void CommitAdmission(ui64 bytes) {
            if (!Enabled || !bytes) {
                return;
            }
            const ui64 previous = InFlightBytes.fetch_sub(bytes, std::memory_order_relaxed);
            Y_ABORT_UNLESS(bytes <= previous, "Fresh admission underflow: committed# %" PRIu64
                " inFlight# %" PRIu64, bytes, previous);
        }

        ui64 GetInFlightBytes() const {
            return InFlightBytes.load(std::memory_order_relaxed);
        }
    };

} // NKikimr
