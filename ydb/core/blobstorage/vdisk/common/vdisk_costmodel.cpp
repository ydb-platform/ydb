#include "vdisk_costmodel.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_handle_class.h>

namespace NKikimr {

    TCostModel::TCostModel(ui64 seekTimeUs, ui64 readSpeedBps, ui64 writeSpeedBps, ui64 readBlockSize,
                           ui64 writeBlockSize, ui32 minREALHugeBlobInBytes, TBlobStorageGroupType gType)
        : SeekTimeUs(seekTimeUs)
        , ReadSpeedBps(readSpeedBps)
        , WriteSpeedBps(writeSpeedBps)
        , ReadBlockSize(readBlockSize)
        , WriteBlockSize(writeBlockSize)
        , MinREALHugeBlobInBytes(minREALHugeBlobInBytes)
        , GType(gType)
    {}

    TCostModel::TCostModel(const NKikimrBlobStorage::TVDiskCostSettings &settings, TBlobStorageGroupType gType)
        : SeekTimeUs(settings.GetSeekTimeUs())
        , ReadSpeedBps(settings.GetReadSpeedBps())
        , WriteSpeedBps(settings.GetWriteSpeedBps())
        , ReadBlockSize(settings.GetReadBlockSize())
        , WriteBlockSize(settings.GetWriteBlockSize())
        , MinREALHugeBlobInBytes(settings.GetMinREALHugeBlobInBytes())
        , GType(gType)
    {}

    void TCostModel::FillInSettings(NKikimrBlobStorage::TVDiskCostSettings &settings) const {
        settings.SetSeekTimeUs(SeekTimeUs);
        settings.SetReadSpeedBps(ReadSpeedBps);
        settings.SetWriteSpeedBps(WriteSpeedBps);
        settings.SetReadBlockSize(ReadBlockSize);
        settings.SetWriteBlockSize(WriteBlockSize);
        settings.SetMinREALHugeBlobInBytes(MinREALHugeBlobInBytes);
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVPatchStart &) const {
        return InMemReadCost();
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVPatchDiff &ev) const {
        TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(ev.Record.GetPatchedPartBlobId());
        ui32 size = blobId.BlobSize();
        return ReadCostBySize(size) + HugeWriteCost(size);
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVPatchXorDiff &ev) const {
        const ui64 bufSize = ev.DiffSizeSum();
        return HugeWriteCost(bufSize);
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVMovedPatch &ev) const {
        TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(ev.Record.GetPatchedBlobId());
        return MovedPatchCostBySize(blobId.BlobSize());
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVPut &ev, bool *logPutInternalQueue) const {
        const auto &record = ev.Record;
        const NKikimrBlobStorage::EPutHandleClass handleClass = record.GetHandleClass();
        const ui64 bufSize = record.HasBuffer() ? record.GetBuffer().size() : ev.GetPayload(0).GetSize();

        NPriPut::EHandleType handleType = NPriPut::HandleType(MinREALHugeBlobInBytes, handleClass, bufSize);
        if (handleType == NPriPut::Log) {
            *logPutInternalQueue = true;
            return SmallWriteCost(bufSize);
        } else {
            *logPutInternalQueue = false;
            return HugeWriteCost(bufSize);
        }
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVMultiPut &ev, bool *logPutInternalQueue) const {
        const NKikimrBlobStorage::TEvVMultiPut &record = ev.Record;
        const NKikimrBlobStorage::EPutHandleClass handleClass = record.GetHandleClass();
        *logPutInternalQueue = true;
        ui64 cost = 0;
        for (ui64 idx = 0; idx < record.ItemsSize(); ++idx) {
            const ui64 size = ev.GetBufferBytes(idx);
            NPriPut::EHandleType handleType = NPriPut::HandleType(MinREALHugeBlobInBytes, handleClass, size);
            if (handleType == NPriPut::Log) {
                cost += SmallWriteCost(size);
            } else {
                *logPutInternalQueue = false;
                cost += HugeWriteCost(size);
            }
        }
        return cost;
    }

    ui64 TCostModel::MovedPatchCostBySize(ui32 blobSize) const {
        ui32 partSize = GType.TErasureType::PartSize(TErasureType::CrcModeNone, blobSize);
        ui32 memMultiply = 3; // in the worst case (buffer + parts + serealized events)
        return memMultiply * GType.TotalPartCount() * (ReadCostBySize(partSize) + HugeWriteCost(partSize));
    }

    ui64 TCostModel::ReadCostBySize(ui64 size) const {
        ui64 seekCost = (size / ReadBlockSize + 1) * SeekTimeUs;
        ui64 readCost = size * ui64(1000000000) / ReadSpeedBps;
        return seekCost + readCost;
    }

    ui64 TCostModel::ReadCost(const TEvBlobStorage::TEvVGet &ev) const {
        const auto &record = ev.Record;
        ui64 cost = 0;

        // range query
        if (record.HasRangeQuery()) {
            if (record.GetIndexOnly()) {
                // in-memory only
                cost += InMemReadCost();
            } else {
                // we don't know cost of the query, it depends on number of elements and their size
                cost += 10000000; // let's assume it's 10 ms
            }
        }

        // extreme queries
        for (const auto &x : record.GetExtremeQueries()) {
            ui64 size = 0;
            if (x.HasSize())
                size = x.GetSize();
            else {
                TLogoBlobID id(LogoBlobIDFromLogoBlobID(x.GetId()));
                size = id.BlobSize();
            }

            cost += ReadCostBySize(size);
        }

        Y_VERIFY_DEBUG(cost);
        return cost;
    }

    ui64 TCostModel::CalculateCost(const TMessageCostEssence& essence) const {
        ui64 cost = essence.BaseCost;
        for (ui64 size : essence.ReadSizes) {
            cost += ReadCostBySize(size);
        }
        if (essence.SmallWriteSize != -1) {
            cost += SmallWriteCost(essence.SmallWriteSize);
        }
        if (essence.MovedPatchBlobSize != -1) {
            cost += MovedPatchCostBySize(essence.MovedPatchBlobSize);
        }
        for (ui64 size : essence.PutBufferSizes) {
            NPriPut::EHandleType handleType = NPriPut::HandleType(MinREALHugeBlobInBytes, essence.HandleClass, size);
            if (handleType == NPriPut::Log) {
                cost += SmallWriteCost(size);
            } else {
                cost += HugeWriteCost(size);
            }
        }
        Y_VERIFY_DEBUG(cost);
        return cost;
    }

} // NKikimr
