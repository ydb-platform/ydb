#pragma once

#include "defs.h"
#include "vdisk_events.h"
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TCostModel -- estimate complexity of incoming request
    // All estimations are performed in time (Us)
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
            TMessageCostEssence(const TEvBlobStorage::TEvVGet& ev) {
                const auto &record = ev.Record;

                // range query
                if (record.HasRangeQuery()) {
                    if (record.GetIndexOnly()) {
                        // in-memory only
                        BaseCost += InMemReadCost();
                    } else {
                        // we don't know cost of the query, it depends on number of elements and their size
                        BaseCost += 10000000; // let's assume it's 10 ms
                    }
                }

                // extreme queries
                ui32 extremeQueriesCnt = record.ExtremeQueriesSize();
                ReadSizes.reserve(extremeQueriesCnt);
                for (ui32 i = 0; i != extremeQueriesCnt; ++i) {
                    const auto &x = record.GetExtremeQueries(i);
                    ui64 size = 0;
                    if (x.HasSize())
                        size = x.GetSize();
                    else {
                        TLogoBlobID id(LogoBlobIDFromLogoBlobID(x.GetId()));
                        size = id.BlobSize();
                    }

                    ReadSizes.push_back(size);
                }
            }

            TMessageCostEssence(const TEvBlobStorage::TEvVGetBlock& /*ev*/)
                : BaseCost(TCostModel::InMemReadCost(EInMemType::Read))
            {}

            TMessageCostEssence(const TEvBlobStorage::TEvVGetBarrier& /*ev*/)
                : BaseCost(TCostModel::InMemReadCost(EInMemType::Read))
            {}

            TMessageCostEssence(const TEvBlobStorage::TEvVBlock& ev)
                : SmallWriteSize(ev.GetCachedByteSize())
            {}

            TMessageCostEssence(const TEvBlobStorage::TEvVCollectGarbage& ev)
                : SmallWriteSize(ev.GetCachedByteSize())
            {}

            TMessageCostEssence(const TEvBlobStorage::TEvVMovedPatch& ev)
                : HandleClass(NKikimrBlobStorage::EPutHandleClass::AsyncBlob)
            {
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(ev.Record.GetOriginalBlobId());
                MovedPatchBlobSize = id.BlobSize();
            }

            TMessageCostEssence(const TEvBlobStorage::TEvVPatchStart&)
                : BaseCost(TCostModel::InMemReadCost(EInMemType::Read))
                , HandleClass(NKikimrBlobStorage::EPutHandleClass::AsyncBlob)
            {
            }

            TMessageCostEssence(const TEvBlobStorage::TEvVPatchDiff& ev)
                : HandleClass(NKikimrBlobStorage::EPutHandleClass::AsyncBlob)
            {
                // it has range vget subquery for finding parts
                TLogoBlobID id(LogoBlobIDFromLogoBlobID(ev.Record.GetOriginalPartBlobId()));
                ReadSizes.push_back(id.BlobSize());
                PutBufferSizes.push_back(id.BlobSize());
            }

            TMessageCostEssence(const TEvBlobStorage::TEvVPatchXorDiff& ev)
                : HandleClass(NKikimrBlobStorage::EPutHandleClass::AsyncBlob)
            {
                PutBufferSizes.push_back(ev.DiffSizeSum());
            }

            TMessageCostEssence(const TEvBlobStorage::TEvVPut& ev)
                : HandleClass(ev.Record.GetHandleClass())
            {
                PutBufferSizes.push_back(ev.Record.HasBuffer() ?
                        ev.Record.GetBuffer().size() : ev.GetPayload(0).GetSize());
            }

            TMessageCostEssence(const TEvBlobStorage::TEvVMultiPut& ev)
                : HandleClass(ev.Record.GetHandleClass())
            {
                const NKikimrBlobStorage::TEvVMultiPut &record = ev.Record;
                const ui64 itemsSize = record.ItemsSize();
                PutBufferSizes.reserve(itemsSize);
                for (ui64 idx = 0; idx < itemsSize; ++idx) {
                    PutBufferSizes.push_back(ev.GetBufferBytes(idx));
                }
            }
        };

    public:
        ui64 SeekTimeUs;
        ui64 ReadSpeedBps;
        ui64 WriteSpeedBps;
        ui64 ReadBlockSize;
        ui64 WriteBlockSize;
        ui32 MinREALHugeBlobInBytes;
        TBlobStorageGroupType GType;

        TCostModel(ui64 seekTimeUs, ui64 readSpeedBps, ui64 writeSpeedBps, ui64 readBlockSize, ui64 writeBlockSize,
                   ui32 minREALHugeBlobInBytes, TBlobStorageGroupType gType);
        TCostModel(const NKikimrBlobStorage::TVDiskCostSettings &settings, TBlobStorageGroupType gType);

        /// SETTINGS
        template <typename TProtoVDiskCostSettings>
        void FillInSettings(TProtoVDiskCostSettings &settings) const {
            settings.SetSeekTimeUs(SeekTimeUs);
            settings.SetReadSpeedBps(ReadSpeedBps);
            settings.SetWriteSpeedBps(WriteSpeedBps);
            settings.SetReadBlockSize(ReadBlockSize);
            settings.SetWriteBlockSize(WriteBlockSize);
            settings.SetMinREALHugeBlobInBytes(MinREALHugeBlobInBytes);
        }

        /// READS
        ui64 GetCost(const TEvBlobStorage::TEvVGet &ev) const {
            return ReadCost(ev);
        }

        ui64 GetCost(const TEvBlobStorage::TEvVGetBlock &ev) const {
            Y_UNUSED(ev);
            return InMemReadCost(TCostModel::EInMemType::Read);
        }

        ui64 GetCost(const TEvBlobStorage::TEvVGetBarrier &ev) const {
            Y_UNUSED(ev);
            return InMemReadCost(TCostModel::EInMemType::Read);
        }

        /// WRITES
        ui64 GetCost(const TEvBlobStorage::TEvVBlock &ev) const {
            const ui32 recByteSize = ev.GetCachedByteSize();
            return SmallWriteCost(recByteSize);
        }

        ui64 GetCost(const TEvBlobStorage::TEvVCollectGarbage &ev) const {
            const ui32 recByteSize = ev.GetCachedByteSize();
            return SmallWriteCost(recByteSize);
        }

        ui64 GetCost(const TEvBlobStorage::TEvVPut &ev) const {
            bool logPutInternalQueue = true;
            return GetCost(ev, &logPutInternalQueue);
        }

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
                MinREALHugeBlobInBytes != other.MinREALHugeBlobInBytes;
        }

    protected:
        ui64 SmallWriteCost(ui64 size) const {
            const ui64 seekCost = SeekTimeUs / 100u; // assume we do one seek per 100 log records
            const ui64 writeCost = size * ui64(1000000000) / WriteSpeedBps;
            const ui64 cost = seekCost + writeCost;
            return cost ? cost : 1;
        }

        ui64 HugeWriteCost(ui64 size) const {
            const ui64 seekCost = (size / WriteBlockSize + 1) * SeekTimeUs; // huge blocks may require several seeks
            const ui64 writeCost = size * ui64(1000000000) / WriteSpeedBps;
            const ui64 cost = seekCost + writeCost;
            Y_VERIFY_DEBUG(cost);
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
    };

} // NKikimr
