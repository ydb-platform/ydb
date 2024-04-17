#include "vdisk_costmodel.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_handle_class.h>

namespace NKikimr {

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVGet& ev) {
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
        ReadSizes.reserve(record.GetExtremeQueries().size());
        for (const auto &x : record.GetExtremeQueries()) {
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

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVGetBlock& /*ev*/)
        : BaseCost(TCostModel::InMemReadCost(EInMemType::Read))
    {}

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVGetBarrier& /*ev*/)
        : BaseCost(TCostModel::InMemReadCost(EInMemType::Read))
    {}

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVBlock& ev)
        : SmallWriteSize(ev.GetCachedByteSize())
    {}

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVCollectGarbage& ev)
        : SmallWriteSize(ev.GetCachedByteSize())
    {}

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVMovedPatch& ev)
        : HandleClass(NKikimrBlobStorage::EPutHandleClass::AsyncBlob)
    {
        TLogoBlobID id = LogoBlobIDFromLogoBlobID(ev.Record.GetOriginalBlobId());
        MovedPatchBlobSize = id.BlobSize();
    }

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVPatchStart&)
        : BaseCost(TCostModel::InMemReadCost(EInMemType::Read))
        , HandleClass(NKikimrBlobStorage::EPutHandleClass::AsyncBlob)
    {}

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVPatchDiff& ev)
        : HandleClass(NKikimrBlobStorage::EPutHandleClass::AsyncBlob)
    {
        // it has range vget subquery for finding parts
        TLogoBlobID id(LogoBlobIDFromLogoBlobID(ev.Record.GetOriginalPartBlobId()));
        ReadSizes.push_back(id.BlobSize());
        PutBufferSizes.push_back(id.BlobSize());
    }

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVPatchXorDiff& ev)
        : HandleClass(NKikimrBlobStorage::EPutHandleClass::AsyncBlob)
    {
        PutBufferSizes.push_back(ev.DiffSizeSum());
    }

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVPut& ev)
        : HandleClass(ev.Record.GetHandleClass())
    {
        PutBufferSizes.push_back(ev.Record.HasBuffer() ?
                ev.Record.GetBuffer().size() : ev.GetPayload(0).GetSize());
    }

    TCostModel::TMessageCostEssence::TMessageCostEssence(const TEvBlobStorage::TEvVMultiPut& ev)
        : HandleClass(ev.Record.GetHandleClass())
    {
        const NKikimrBlobStorage::TEvVMultiPut &record = ev.Record;
        const ui64 itemsSize = record.ItemsSize();
        PutBufferSizes.reserve(itemsSize);
        for (ui64 idx = 0; idx < itemsSize; ++idx) {
            PutBufferSizes.push_back(ev.GetBufferBytes(idx));
        }
    }

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

    /// READS
    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVGet &ev) const {
        return ReadCost(ev);
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVGetBlock &ev) const {
        Y_UNUSED(ev);
        return InMemReadCost(TCostModel::EInMemType::Read);
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVGetBarrier &ev) const {
        Y_UNUSED(ev);
        return InMemReadCost(TCostModel::EInMemType::Read);
    }

    /// WRITES
    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVBlock &ev) const {
        const ui32 recByteSize = ev.GetCachedByteSize();
        return SmallWriteCost(recByteSize);
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVCollectGarbage &ev) const {
        const ui32 recByteSize = ev.GetCachedByteSize();
        return SmallWriteCost(recByteSize);
    }

    ui64 TCostModel::GetCost(const TEvBlobStorage::TEvVPut &ev) const {
        bool logPutInternalQueue = true;
        return GetCost(ev, &logPutInternalQueue);
    }

    // PATCHES
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

        NPriPut::EHandleType handleType = NPriPut::HandleType(MinREALHugeBlobInBytes, handleClass, bufSize, true);
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
            NPriPut::EHandleType handleType = NPriPut::HandleType(MinREALHugeBlobInBytes, handleClass, size, true);
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
        ui64 seekCost = (size / ReadBlockSize + 1) * SeekTimeUs * 1000ull;
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
                cost += 1000ull * SeekTimeUs + 2'000'000ull * 1'000'000'000 / ReadSpeedBps;
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

        Y_DEBUG_ABORT_UNLESS(cost);
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
            NPriPut::EHandleType handleType = NPriPut::HandleType(MinREALHugeBlobInBytes, essence.HandleClass, size, true);
            if (handleType == NPriPut::Log) {
                cost += SmallWriteCost(size);
            } else {
                cost += HugeWriteCost(size);
            }
        }
        Y_DEBUG_ABORT_UNLESS(cost);
        return cost;
    }

    TString TCostModel::ToString() const {
        TStringStream str;
        str << "{SeekTimeUs# " << SeekTimeUs;
        str << " ReadSpeedBps# " << ReadSpeedBps;
        str << " WriteSpeedBps# " << WriteSpeedBps;
        str << " ReadBlockSize# " << ReadBlockSize;
        str << " WriteBlockSize# " << WriteBlockSize;
        str << " MinREALHugeBlobInBytes# " << MinREALHugeBlobInBytes;
        str << " GType# " << GType.ToString();
        str << "}";
        return str.Str();
    }

    void TCostModel::PessimisticComposition(const TCostModel& other) {
        SeekTimeUs = std::max(SeekTimeUs, other.SeekTimeUs);
        ReadSpeedBps = std::min(ReadSpeedBps, other.ReadSpeedBps);
        WriteSpeedBps = std::min(WriteSpeedBps, other.WriteSpeedBps);
        ReadBlockSize = std::min(ReadBlockSize, other.ReadBlockSize);
        WriteBlockSize = std::min(WriteBlockSize, other.WriteBlockSize);
        MinREALHugeBlobInBytes = std::max(MinREALHugeBlobInBytes, other.MinREALHugeBlobInBytes);
    }

    // PDisk messages cost
    ui64 TCostModel::GetCost(const NPDisk::TEvChunkRead& ev) const {
        return ReadCostBySize(ev.Size);
    }

    // PDisk messages cost
    ui64 TCostModel::GetCost(const NPDisk::TEvChunkWrite& ev) const {
        if (ev.PriorityClass == NPriPut::Log) {
            return SmallWriteCost(ev.PartsPtr->Size());
        } else {
            return HugeWriteCost(ev.PartsPtr->Size());
        }
    }

} // NKikimr
