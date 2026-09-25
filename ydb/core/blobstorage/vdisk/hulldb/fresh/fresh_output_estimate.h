#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h>

#include <util/generic/utility.h>
#include <util/system/align.h>

namespace NKikimr {

    // The part of the Fresh compaction writer's layout that bounds how much it writes. Mirrors
    // TLevelSegment::TWriter, in particular TDataWriter::GetUsageAfterPush() and
    // TIndexBuilder::GetUsageAfterPush(): the simulation TWriter::CheckSpace() uses to decide
    // whether the next item still fits the current SST.
    struct TFreshOutputGeometry {
        ui32 ChunkSize = 0;
        ui32 AppendBlockSize = 0;
        // HullSstSizeInChunksFresh. The writer closes an SST as soon as the next item would not fit
        // this many chunks, and starts a new SST in a new chunk.
        ui32 ChunksPerSst = 1;
        // How many parts of one blob compaction can merge into a single record.
        ui32 TotalPartCount = 1;
        // sizeof(TIndexRecord<TKey, TMemRec>): one record's entry in the SST index.
        ui32 IndexRecordBytes = 0;
    };

    // An upper bound on the chunks compacting a Fresh segment writes, accumulated one record at a
    // time as the segment is filled.
    //
    // Each Fresh record is charged on its own, and that over-counts what compaction produces:
    // compaction merges the parts of a blob into one index record and one DiskBlob behind a single
    // header, drops what garbage collection allows, and never grows a record. The sum of the
    // charges therefore bounds the payload the writer packs. CalculateChunks() turns that payload
    // into chunks by adding back what packing can waste around it.
    class TFreshOutputEstimate {
    public:
        // A record whose blob is kept inline. It reaches the SST verbatim, aligned to 4 like
        // TBaseWriter::AppendAligned() does.
        void AddInline(ui32 dataBytes) {
            const ui32 aligned = AlignUp<ui32>(dataBytes, 4);
            ++Records;
            InlineBytes += aligned;
            MaxInlineBytes = Max(MaxInlineBytes, aligned);
        }

        // A record pointing at a huge blob. Once merged with other huge parts of the same blob it can
        // cost an outbound TDiskPart in the index; charging one always is the safe side.
        void AddHuge() {
            ++Records;
            ++HugeRefs;
        }

        // Records with nothing but an index entry: blocks, barriers, keep flags, sync appendices.
        void AddIndexOnly(ui64 count = 1) {
            Records += count;
        }

        void Merge(const TFreshOutputEstimate& other) {
            Records += other.Records;
            InlineBytes += other.InlineBytes;
            HugeRefs += other.HugeRefs;
            MaxInlineBytes = Max(MaxInlineBytes, other.MaxInlineBytes);
        }

        // Take back what Merge() added. The largest record cannot be taken back, since what else was
        // that size is not recorded; it stays as it was, which only keeps the bound on the safe side,
        // until nothing is left at all.
        void Subtract(const TFreshOutputEstimate& other) {
            Y_ABORT_UNLESS(other.Records <= Records && other.InlineBytes <= InlineBytes && other.HugeRefs <= HugeRefs,
                "subtracting more than was added");
            Records -= other.Records;
            InlineBytes -= other.InlineBytes;
            HugeRefs -= other.HugeRefs;
            if (!Records) {
                *this = {};
            }
        }

        bool Empty() const {
            return !Records;
        }

        ui64 GetRecords() const {
            return Records;
        }

        ui32 GetMaxInlineBytes() const {
            return MaxInlineBytes;
        }

        // Bytes of payload compaction lays out: index records, inline blobs and outbound entries.
        ui64 GetCharge(const TFreshOutputGeometry& geometry) const {
            return Records * geometry.IndexRecordBytes + InlineBytes + HugeRefs * sizeof(TDiskPart);
        }

        ui64 GetChunks(const TFreshOutputGeometry& geometry) const {
            return CalculateChunks(GetCharge(geometry), MaxInlineBytes, geometry);
        }

        // Chunks needed to write `charge` bytes of payload whose largest inline record is
        // `maxInlineBytes`.
        //
        // One SST of N chunks takes any payload up to
        //
        //   capacity = N * ChunkSize - sizeof(TIdxDiskPlaceHolder) - AppendBlockSize - (N - 1) * rollWaste
        //
        // The placeholder is the SST's entry point in its final chunk. The data region is padded to
        // AppendBlockSize before the index begins (TDataWriter::Finish()). A chunk boundary inside
        // the SST costs at most rollWaste: the writer packs next-fit, so the tail an item did not
        // fit is abandoned, and a chunk the index continues from also ends with a TIdxDiskLinker.
        //
        // Every SST but the last was closed because the next item did not fit, so it holds more than
        // capacity - footprint, where footprint is the largest item compaction can produce. That
        // gives ssts <= 1 + ceil((charge - capacity) / (capacity - footprint)).
        static ui64 CalculateChunks(ui64 charge, ui32 maxInlineBytes, const TFreshOutputGeometry& geometry) {
            if (!charge) {
                return 0;
            }

            const ui64 chunksPerSst = Max<ui32>(geometry.ChunksPerSst, 1);
            const ui64 parts = Max<ui32>(geometry.TotalPartCount, 1);

            // The largest record merging can make: one index entry, every part inline at the largest
            // size seen, and an outbound entry per part should they have been huge instead.
            const ui64 mergedInline = parts * maxInlineBytes;
            const ui64 footprint = geometry.IndexRecordBytes + mergedInline + parts * sizeof(TDiskPart);
            const ui64 rollWaste = sizeof(TIdxDiskLinker) + Max<ui64>(mergedInline, geometry.IndexRecordBytes);

            const ui64 raw = chunksPerSst * geometry.ChunkSize;
            const ui64 overhead = sizeof(TIdxDiskPlaceHolder) + geometry.AppendBlockSize
                + (chunksPerSst - 1) * rollWaste;
            if (raw <= overhead) {
                // A chunk that cannot hold even an empty SST: nothing can be compacted into it.
                return Max<ui32>();
            }
            const ui64 capacity = raw - overhead;
            if (charge <= capacity) {
                return chunksPerSst;
            }

            // The bound holds only while capacity - footprint is positive. A record that might not fit an SST
            // at all could not be written anyway, the writer never splitting one; the huge blob threshold keeps
            // inline records far below that. Like the geometry above, it needs more chunks than can be had.
            if (capacity <= footprint) {
                return Max<ui32>();
            }
            const ui64 step = capacity - footprint;
            const ui64 ssts = 1 + (charge - capacity + step - 1) / step;
            return ssts * chunksPerSst;
        }

    private:
        ui64 Records = 0;
        ui64 InlineBytes = 0;
        ui64 HugeRefs = 0;
        ui32 MaxInlineBytes = 0;
    };

    // What one operation adds to each of the three Fresh segments. A garbage collection command, for
    // one, writes a barrier and keep flags for blobs alike.
    struct TFreshAdmission {
        TFreshOutputEstimate LogoBlobs;
        TFreshOutputEstimate Blocks;
        TFreshOutputEstimate Barriers;

        bool Empty() const {
            return LogoBlobs.Empty() && Blocks.Empty() && Barriers.Empty();
        }

        void Merge(const TFreshAdmission& other) {
            LogoBlobs.Merge(other.LogoBlobs);
            Blocks.Merge(other.Blocks);
            Barriers.Merge(other.Barriers);
        }

        void Subtract(const TFreshAdmission& other) {
            LogoBlobs.Subtract(other.LogoBlobs);
            Blocks.Subtract(other.Blocks);
            Barriers.Subtract(other.Barriers);
        }
    };

    // Chunks each Fresh segment lacks to take an admission.
    struct TFreshShortfall {
        ui64 LogoBlobs = 0;
        ui64 Blocks = 0;
        ui64 Barriers = 0;

        ui64 Total() const {
            return LogoBlobs + Blocks + Barriers;
        }
    };

} // NKikimr
