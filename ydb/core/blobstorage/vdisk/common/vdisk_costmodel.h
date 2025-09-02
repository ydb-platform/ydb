#pragma once

#include "defs.h"
#include "vdisk_events.h"


namespace NKikimr {

    namespace NPDisk {
        struct TEvChunkRead;
        struct TEvChunkWrite;
    }

    ////////////////////////////////////////////////////////////////////////////
    // TCostModel -- estimate complexity of incoming request
    // All estimations are performed in time (ns)
    ////////////////////////////////////////////////////////////////////////////
    class TCostModel {
    public:
        class TMessageCostEssence {
            friend class TCostModel;

            ui64 BaseCost = 0;
            i32 SmallWriteSize = -1;
            i32 MovedPatchBlobSize = -1;
            TStackVec<i32, 1> PutBufferSizes;
            TVector<ui32> ReadSizes; // FIXME: optimize
            // handle class for TEvPut and TEvMultiPut only
            NKikimrBlobStorage::EPutHandleClass HandleClass = NKikimrBlobStorage::TabletLog;

        public:
            TMessageCostEssence(const TEvBlobStorage::TEvVGet& ev);

            TMessageCostEssence(const TEvBlobStorage::TEvVGetBlock& /*ev*/);

            TMessageCostEssence(const TEvBlobStorage::TEvVGetBarrier& /*ev*/);

            TMessageCostEssence(const TEvBlobStorage::TEvVBlock& ev);

            TMessageCostEssence(const TEvBlobStorage::TEvVCollectGarbage& ev);

            TMessageCostEssence(const TEvBlobStorage::TEvVMovedPatch& ev);

            TMessageCostEssence(const TEvBlobStorage::TEvVPatchStart&);

            TMessageCostEssence(const TEvBlobStorage::TEvVPatchDiff& ev);

            TMessageCostEssence(const TEvBlobStorage::TEvVPatchXorDiff& ev);

            TMessageCostEssence(const TEvBlobStorage::TEvVPut& ev);

            TMessageCostEssence(const TEvBlobStorage::TEvVMultiPut& ev);
        };

    public:
        ui64 SeekTimeUs;
        ui64 ReadSpeedBps;
        ui64 WriteSpeedBps;
        ui64 ReadBlockSize;
        ui64 WriteBlockSize;
        ui32 MinHugeBlobInBytes;
        TBlobStorageGroupType GType;

        TCostModel(ui64 seekTimeUs, ui64 readSpeedBps, ui64 writeSpeedBps, ui64 readBlockSize, ui64 writeBlockSize,
                   ui32 minHugeBlobInBytes, TBlobStorageGroupType gType);
        TCostModel(const NKikimrBlobStorage::TVDiskCostSettings &settings, TBlobStorageGroupType gType);

        /// SETTINGS
        void FillInSettings(NKikimrBlobStorage::TVDiskCostSettings &settings) const;

        /// READS
        ui64 GetCost(const TEvBlobStorage::TEvVGet &ev) const;
        ui64 GetCost(const TEvBlobStorage::TEvVGetBlock &ev) const;
        ui64 GetCost(const TEvBlobStorage::TEvVGetBarrier &ev) const;

        /// WRITES
        ui64 GetCost(const TEvBlobStorage::TEvVBlock &ev) const;
        ui64 GetCost(const TEvBlobStorage::TEvVCollectGarbage &ev) const;
        ui64 GetCost(const TEvBlobStorage::TEvVPut &ev) const;

        ui64 GetCost(const TEvBlobStorage::TEvVPatchStart &ev) const;
        ui64 GetCost(const TEvBlobStorage::TEvVPatchDiff &ev) const;
        ui64 GetCost(const TEvBlobStorage::TEvVPatchXorDiff &ev) const;
        ui64 GetCost(const TEvBlobStorage::TEvVMovedPatch &ev) const;

        ui64 GetCost(const TEvBlobStorage::TEvVPut &ev, bool *logPutInternalQueue) const;
        ui64 GetCost(const TEvBlobStorage::TEvVMultiPut &ev, bool *logPutInternalQueue) const;

        /// LAZY EVALUATION
        ui64 CalculateCost(const TMessageCostEssence& essence) const;

        bool operator !=(const TCostModel& other) const {
            return SeekTimeUs != other.SeekTimeUs ||
                ReadSpeedBps != other.ReadSpeedBps ||
                WriteSpeedBps != other.WriteSpeedBps ||
                ReadBlockSize != other.ReadBlockSize ||
                WriteBlockSize != other.WriteBlockSize ||
                MinHugeBlobInBytes != other.MinHugeBlobInBytes;
        }

        // PDisk messages cost
        ui64 GetCost(const NPDisk::TEvChunkRead &ev) const;
        ui64 GetCost(const NPDisk::TEvChunkWrite &ev) const;

    protected:
        ui64 SmallWriteCost(ui64 size) const {
            const ui64 seekCost = SeekTimeUs  * 1000ull / 100u; // assume we do one seek per 100 log records
            const ui64 writeCost = size * ui64(1000000000) / WriteSpeedBps;
            const ui64 cost = seekCost + writeCost;
            return cost ? cost : 1;
        }

        ui64 HugeWriteCost(ui64 size) const {
            const ui64 seekCost = (size / WriteBlockSize + 1) * SeekTimeUs * 1000ull; // huge blocks may require several seeks
            const ui64 writeCost = size * ui64(1000000000) / WriteSpeedBps;
            const ui64 cost = seekCost + writeCost;
            Y_DEBUG_ABORT_UNLESS(cost);
            return cost;
        }

        enum class EInMemType {
            Read = 0,
            Sync = 1,
            FullSync = 2
        };

        static ui64 InMemReadCost(EInMemType t = EInMemType::Read) {
            static TVector<ui64> costs = {1000u, 5000u, 100000u}; // Read, Sync, FullSync
            // NOTES: for Sync we don't know the cost, but we may assume that we read from memory
            return costs.at(size_t(t));
        }

        ui64 MovedPatchCostBySize(ui32 blobSize) const;
        ui64 ReadCostBySize(ui64 size) const;
        ui64 ReadCost(const TEvBlobStorage::TEvVGet &ev) const;

    public:
        TString ToString() const;
        void PessimisticComposition(const TCostModel& other);

    };

} // NKikimr
