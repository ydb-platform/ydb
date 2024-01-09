#pragma once

#include "defs.h"
#include "vdisk_costmodel.h"
#include "vdisk_events.h"
#include "vdisk_handle_class.h"

#include <util/system/compiler.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {

class TBsCostModelBase {
public:
    virtual ~TBsCostModelBase() = default;

protected:
    // Disk Settings
    ui64 DeviceSeekTimeNs = 5'000'000;
    ui64 HugeBlobSize = 1'000'000; // depends on erasure

    ui64 DeviceReadSpeedBps = 500 * 1'000'000; // 500 MB/sec
    ui64 DeviceReadBlockSize = 4 * 1'000; // 4 kB

    ui64 DeviceWriteSpeedBps = 250 * 1'000'000; // 250 MB/sec
    ui64 DeviceWriteBlockSize = 4 * 1'000; // 4 kB
    ui64 PDiskWriteBlockSize = 4ull * 1'000'000; // 4MB

    // Estimated Coefficients
    // cost = A + B * size
    double WriteA = 2520;
    double WriteB = 5.7668;

    double ReadA = WriteA;
    double ReadB = WriteB;

    double HugeWriteA = 1.26748409e+06;
    double HugeWriteB = 2.69514462e+01;

private:
    enum class EMemoryOperationType {
        Read = 0,
        Sync = 1,
        FullSync = 2
    };

    static ui64 MemoryOperationCost(EMemoryOperationType t = EMemoryOperationType::Read) {
        static TVector<ui64> costs = { 1000, 5000, 100000 }; // Read, Sync, FullSync
        // NOTES: for Sync we don't know the cost, but we may assume that we read from memory
        return costs.at(size_t(t));
    }

public:
    void Update(const TCostModel& costModel) {
        DeviceSeekTimeNs = costModel.SeekTimeUs * 1000;
        DeviceReadSpeedBps = costModel.ReadSpeedBps;
        DeviceWriteSpeedBps = costModel.WriteSpeedBps;
        DeviceReadBlockSize = costModel.ReadBlockSize;
        DeviceWriteBlockSize = costModel.WriteBlockSize;
        HugeBlobSize = costModel.MinREALHugeBlobInBytes;
    }

protected:
    ui64 BatchedWriteCost(ui64 chunkSize) const {
        ui64 seekTime = 1. * chunkSize / PDiskWriteBlockSize * DeviceSeekTimeNs;
        ui64 writeTime = chunkSize * 1'000'000'000ull / DeviceWriteSpeedBps;
        return seekTime + writeTime;
    }

    ui64 WriteCost(ui64 chunkSize) const {
        ui64 seekTime = 1. * chunkSize * DeviceSeekTimeNs;
        ui64 writeTime = chunkSize * 1'000'000'000ull / DeviceWriteSpeedBps;
        return seekTime + writeTime;
    }

    ui64 HugeWriteCost(ui64 chunkSize) const {
        ui64 blocksNumber = (chunkSize + DeviceWriteBlockSize - 1) / DeviceWriteBlockSize;
        ui64 seekTime = 1. * blocksNumber * DeviceSeekTimeNs;
        ui64 writeTime = chunkSize * 1'000'000'000ull / DeviceWriteSpeedBps;
        return seekTime + writeTime;
    }

    ui64 ReadCost(ui64 chunkSize) const {
        ui64 blocksNumber = (chunkSize + DeviceReadBlockSize - 1) / DeviceReadBlockSize;
        ui64 seekTime = 1. * blocksNumber * DeviceSeekTimeNs;
        ui64 readTime = chunkSize * 1'000'000'000ull / DeviceReadSpeedBps;
        return seekTime + readTime;
    }

    ui64 EstimatedWriteCost(ui64 chunkSize) const {
        return WriteA + WriteB * chunkSize;
    }

    ui64 EstimatedHugeWriteCost(ui64 chunkSize) const {
        return HugeWriteA + HugeWriteB * chunkSize;
    }

public:
    /////// DS Proxy requests
    /// READS
    virtual ui64 GetCost(const TEvBlobStorage::TEvGet&/* ev*/) const { return 0; }
    virtual ui64 GetCost(const TEvBlobStorage::TEvRange&/* ev*/) const { return 0; }

    /// WRITES
    virtual ui64 GetCost(const TEvBlobStorage::TEvBlock&/* ev*/) const { return 0; }

    virtual ui64 GetCost(const TEvBlobStorage::TEvCollectGarbage&/* ev*/) const { return 0; }

    virtual ui64 GetCost(const TEvBlobStorage::TEvPut&/* ev*/) const { return 0; }

    // PATCHES
    virtual ui64 GetCost(const TEvBlobStorage::TEvPatch&/* ev*/) const { return 0; }

    /////// VDisk requests
    /// READS
    ui64 GetCost(const TEvBlobStorage::TEvVGet& ev) const {
        const auto &record = ev.Record;
        ui64 cost = 0;

        // range query
        if (record.HasRangeQuery()) {
            if (record.GetIndexOnly()) {
                // in-memory only
                cost += MemoryOperationCost();
            } else {
                // we don't know cost of the query, it depends on number of elements and their size
                cost += DeviceSeekTimeNs + 2'000'000ull * 1'000'000'000 / DeviceReadSpeedBps;
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

            cost += ReadCost(size);
        }

        return cost;
    }

    ui64 GetCost(const TEvBlobStorage::TEvVGetBlock&/* ev*/) const {
        return MemoryOperationCost();
    }

    ui64 GetCost(const TEvBlobStorage::TEvVGetBarrier&/* ev*/) const {
        return MemoryOperationCost();
    }

    /// WRITES
    ui64 GetCost(const TEvBlobStorage::TEvVBlock& ev) const {
        return EstimatedWriteCost(ev.GetCachedByteSize());
    }

    ui64 GetCost(const TEvBlobStorage::TEvVCollectGarbage& ev) const {
        return EstimatedWriteCost(ev.GetCachedByteSize());
    }

    ui64 GetCost(const TEvBlobStorage::TEvVPut& ev) const { 
        const auto &record = ev.Record;
        const NKikimrBlobStorage::EPutHandleClass handleClass = record.GetHandleClass();
        const ui64 size = record.HasBuffer() ? record.GetBuffer().size() : ev.GetPayload(0).GetSize();

        NPriPut::EHandleType handleType = NPriPut::HandleType(HugeBlobSize, handleClass, size);
        if (handleType == NPriPut::Log) {
            return EstimatedWriteCost(size);
        } else {
            return EstimatedHugeWriteCost(size);
        }
    }

    ui64 GetCost(const TEvBlobStorage::TEvVMultiPut& ev) const {
        const auto &record = ev.Record;
        const NKikimrBlobStorage::EPutHandleClass handleClass = record.GetHandleClass();
        ui64 cost = 0;

        for (ui64 idx = 0; idx < record.ItemsSize(); ++idx) {
            const ui64 size = ev.GetBufferBytes(idx);
            NPriPut::EHandleType handleType = NPriPut::HandleType(HugeBlobSize, handleClass, size);
            if (handleType == NPriPut::Log) {
                cost += EstimatedWriteCost(size);
            } else {
                cost += EstimatedHugeWriteCost(size);
            }
        }
        return cost;
    }

    // PATCHES
    ui64 GetCost(const TEvBlobStorage::TEvVPatchStart&/* ev*/) const { return 0; }
    ui64 GetCost(const TEvBlobStorage::TEvVPatchDiff&/* ev*/) const { return 0; }
    ui64 GetCost(const TEvBlobStorage::TEvVPatchXorDiff&/* ev*/) const { return 0; }
    ui64 GetCost(const TEvBlobStorage::TEvVMovedPatch&/* ev*/) const { return 0; }

    /////// PDisk requests
    // READS
    ui64 GetCost(const NPDisk::TEvChunkRead& ev) const {
        return ReadCost(ev.Size);
    }

    // WRITES
    ui64 GetCost(const NPDisk::TEvChunkWrite& ev) const {
        if (ev.PriorityClass == NPriPut::Log) {
            return EstimatedWriteCost(ev.PartsPtr->Size());
        } else {
            return EstimatedHugeWriteCost(ev.PartsPtr->Size());
        }
    }
};

class TBsCostModelMirror3dc;
class TBsCostModel4Plus2Block;
class TBsCostModelMirror3of4;

class TBsCostTracker {
private:
    TBlobStorageGroupType GroupType;
    std::unique_ptr<TBsCostModelBase> CostModel;

    const TIntrusivePtr<::NMonitoring::TDynamicCounters> CostCounters;

    ::NMonitoring::TDynamicCounters::TCounterPtr UserDiskCost;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompactionDiskCost;
    ::NMonitoring::TDynamicCounters::TCounterPtr ScrubDiskCost;
    ::NMonitoring::TDynamicCounters::TCounterPtr DefragDiskCost;
    ::NMonitoring::TDynamicCounters::TCounterPtr InternalDiskCost;

public:
    TBsCostTracker(const TBlobStorageGroupType& groupType, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    template<class TEv>
    ui64 GetCost(const TEv& ev) const {
        ui64 cost = 0;
        if (CostModel) {
            cost = CostModel->GetCost(ev);
        } else {
            cost = 0;
        }
        return cost;
    }

    /// SETTINGS
    void UpdateFromVDiskSettings(NKikimrBlobStorage::TVDiskCostSettings &settings) const;

public:
    void UpdateCostModel(const TCostModel& costModel) {
        if (CostModel) {
            CostModel->Update(costModel);
        }
    }

public:
    template<class TEvent>
    void CountUserRequest(const TEvent& ev) {
        *UserDiskCost += GetCost(ev);
    }

    void CountUserCost(ui64 cost) {
        *UserDiskCost += cost;
    }

    template<class TEvent>
    void CountCompactionRequest(const TEvent& ev) {
        *CompactionDiskCost += GetCost(ev);
    }

    template<class TEvent>
    void CountScrubRequest(const TEvent& ev) {
        *UserDiskCost += GetCost(ev);
    }

    template<class TEvent>
    void CountDefragRequest(const TEvent& ev) {
        *DefragDiskCost += GetCost(ev);
    }

    template<class TEvent>
    void CountInternalRequest(const TEvent& ev) {
        *InternalDiskCost += GetCost(ev);
    }

    void CountInternalCost(ui64 cost) {
        *InternalDiskCost += cost;
    }
};

} // NKikimr
