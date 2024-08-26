#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_requestimpl.h"
#include <ydb/core/blobstorage/base/vdisk_priorities.h>

namespace NKikimr {

TPDiskMon::TPDiskMon(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui32 pDiskId,
        TPDiskConfig *cfg)
    : Counters(counters)
    , PDiskId(pDiskId)
    , ChunksGroup(Counters->GetSubgroup("subsystem", "chunks"))
    , StateGroup(Counters->GetSubgroup("subsystem", "state"))
    , DeviceGroup(Counters->GetSubgroup("subsystem", "device"))
    , QueueGroup(Counters->GetSubgroup("subsystem", "queue"))
    , SchedulerGroup(Counters->GetSubgroup("subsystem", "scheduler"))
    , BandwidthGroup(Counters->GetSubgroup("subsystem", "bandwidth"))
    , PDiskGroup(Counters->GetSubgroup("subsystem", "pdisk"))
{
    using EVisibility = NMonitoring::TCountableBase::EVisibility;

    bool extendedPDiskSensors = NActors::TlsActivationContext
        && NActors::TlsActivationContext->ExecutorThread.ActorSystem
        && AppData()->FeatureFlags.GetExtendedPDiskSensors();

    EVisibility visibilityForExtended = extendedPDiskSensors
        ? EVisibility::Public : EVisibility::Private;

#define COUNTER_INIT(group, name, derivative) \
    name = group->GetCounter(#name, derivative) \

#define COUNTER_INIT_IF_EXTENDED(group, name, derivative) \
    name = group->GetCounter(#name, derivative, visibilityForExtended) \

#define NAMED_DER_COUNTER_INIT_IF_EXTENDED(group, field, name) \
    field = group->GetCounter(#name, true, visibilityForExtended) \

#define IO_REQ_INIT(group, field, name) \
    field.Setup(group, #name, EVisibility::Public) \

#define IO_REQ_INIT_IF_EXTENDED(group, field, name) \
    field.Setup(group, #name, visibilityForExtended) \

#define TRACKER_INIT_IF_EXTENDED(field, subgroup, name) \
    field.Initialize(counters, "subsystem", #subgroup, #name, percentiles, visibilityForExtended) \

#define HISTOGRAM_INIT(field, name) \
    field.Initialize(counters, #name, deviceType) \

    // chunk states subgroup
    COUNTER_INIT(ChunksGroup, UntrimmedFreeChunks, false);
    COUNTER_INIT(ChunksGroup, FreeChunks, false);
    COUNTER_INIT(ChunksGroup, LogChunks, false);
    COUNTER_INIT(ChunksGroup, UncommitedDataChunks, false);
    COUNTER_INIT(ChunksGroup, CommitedDataChunks, false);
    COUNTER_INIT(ChunksGroup, LockedChunks, false);
    COUNTER_INIT(ChunksGroup, QuarantineChunks, false);
    COUNTER_INIT(ChunksGroup, QuarantineOwners, false);

    // stats subgroup
    StatsGroup = (cfg && cfg->PDiskCategory.IsSolidState())
        ? Counters->GetSubgroup("type", "ssd_excl")
        : Counters->GetSubgroup("type", "hdd_excl");
    StatsGroup = StatsGroup->GetSubgroup("subsystem", "stats");

    COUNTER_INIT_IF_EXTENDED(StatsGroup, FreeSpacePerMile, false);
    COUNTER_INIT_IF_EXTENDED(StatsGroup, UsedSpacePerMile, false);
    COUNTER_INIT_IF_EXTENDED(StatsGroup, SplicedLogChunks, true);

    COUNTER_INIT(StatsGroup, TotalSpaceBytes, false);
    COUNTER_INIT(StatsGroup, FreeSpaceBytes, false);
    COUNTER_INIT(StatsGroup, UsedSpaceBytes, false);
    COUNTER_INIT(StatsGroup, SectorMapAllocatedBytes, false);

    // states subgroup
    COUNTER_INIT(StateGroup, PDiskState, false);
    COUNTER_INIT(StateGroup, PDiskBriefState, false);
    COUNTER_INIT(StateGroup, PDiskDetailedState, false);
    COUNTER_INIT(StateGroup, AtLeastOneVDiskNotLogged, false);
    COUNTER_INIT(StateGroup, TooMuchLogChunks, false);
    COUNTER_INIT(StateGroup, SerialNumberMismatched, false);
    L6. Initialize(StateGroup, "L6");
    L7. Initialize(StateGroup, "L7");
    IdleLight.Initialize(StateGroup, "DeviceBusyPeriods", "DeviceIdleTimeMsPerSec", "DeviceBusyTimeMsPerSec");

    COUNTER_INIT_IF_EXTENDED(StateGroup, OwnerIdsIssued, false);
    COUNTER_INIT_IF_EXTENDED(StateGroup, LastOwnerId, false);
    COUNTER_INIT_IF_EXTENDED(StateGroup, PendingYardInits, false);

    SeqnoL6 = 0;
    LastDoneOperationTimestamp = 0;

    // device subgroup
    COUNTER_INIT(DeviceGroup, DeviceBytesRead, true);
    COUNTER_INIT(DeviceGroup, DeviceBytesWritten, true);
    COUNTER_INIT(DeviceGroup, DeviceReads, true);
    COUNTER_INIT(DeviceGroup, DeviceWrites, true);
    COUNTER_INIT(DeviceGroup, DeviceInFlightBytesRead, false);
    COUNTER_INIT(DeviceGroup, DeviceInFlightBytesWrite, false);
    COUNTER_INIT(DeviceGroup, DeviceInFlightReads, false);
    COUNTER_INIT(DeviceGroup, DeviceInFlightWrites, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceTakeoffs, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceLandings, false);
    COUNTER_INIT(DeviceGroup, DeviceHaltDetected, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceExpectedSeeks, true);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceReadCacheHits, true);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceReadCacheMisses, true);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceWriteCacheIsValid, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceWriteCacheIsEnabled, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceOperationPoolTotalAllocations, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceOperationPoolFreeObjectsMin, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceBufferPoolFailedAllocations, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceErasureSectorRestorations, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceEstimatedCostNs, true);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceActualCostNs, true);
    COUNTER_INIT(DeviceGroup, DeviceOverestimationRatio, false);
    COUNTER_INIT(DeviceGroup, DeviceNonperformanceMs, false);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceInterruptedSystemCalls, true);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceSubmitThreadBusyTimeNs, true);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceCompletionThreadBusyTimeNs, true);
    COUNTER_INIT(DeviceGroup, DeviceIoErrors, true);
    COUNTER_INIT_IF_EXTENDED(DeviceGroup, DeviceWaitTimeMs, true);

    UpdateDurationTracker.SetCounter(DeviceGroup->GetCounter("PDiskThreadBusyTimeNs", true));

    // queue subgroup
    COUNTER_INIT(QueueGroup, QueueRequests, true);
    COUNTER_INIT(QueueGroup, QueueBytes, true);

    auto deviceType = cfg ? cfg->PDiskCategory.Type() : NPDisk::DEVICE_TYPE_UNKNOWN;

    // scheduler subgroup
    COUNTER_INIT_IF_EXTENDED(SchedulerGroup, ForsetiCbsNotFound, false);

    TVector<float> percentiles;
    percentiles.push_back(0.50f);
    percentiles.push_back(0.90f);
    percentiles.push_back(0.99f);
    percentiles.push_back(1.00f);

    TRACKER_INIT_IF_EXTENDED(UpdateDurationTracker.UpdateCycleTime, updateCycle, Time in millisec);

    HISTOGRAM_INIT(DeviceReadDuration, deviceReadDuration);
    HISTOGRAM_INIT(DeviceWriteDuration, deviceWriteDuration);
    HISTOGRAM_INIT(DeviceTrimDuration, deviceTrimDuration);
    HISTOGRAM_INIT(DeviceFlushDuration, deviceFlushDuration);

    TRACKER_INIT_IF_EXTENDED(LogQueueTime, logQueueTime, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(GetQueueSyncLog, getQueueSyncLog, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(GetQueueHullComp, getQueueHullComp, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(GetQueueHullOnlineRt, getQueueHullOnlineRt, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(GetQueueHullOnlineOther, getQueueHullOnlineOther, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(GetQueueHullLoad, getQueueHullLoad, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(GetQueueHullLow, getQueueHullLow, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(WriteQueueSyncLog, writeQueueSyncLog, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(WriteQueueHullFresh, writeQueueHullFresh, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(WriteQueueHullHuge, writeQueueHullHuge, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(WriteQueueHullComp, writeQueueHullComp, Time in millisec);

    TRACKER_INIT_IF_EXTENDED(SensitiveBurst, sensitiveBurst, Time in millisec);
    TRACKER_INIT_IF_EXTENDED(BestEffortBurst, bestEffortBurst, Time in millisec);

    TRACKER_INIT_IF_EXTENDED(InputQLA, inputQLA, Queue length seen by arrivals);
    TRACKER_INIT_IF_EXTENDED(InputQCA, inputQCA, Queue cost seen by arrivals);

    TRACKER_INIT_IF_EXTENDED(LogOperationSizeBytes, logOperationSize, Size in bytes);
    TRACKER_INIT_IF_EXTENDED(GetSyncLogSizeBytes, getSyncLogSize, Size in bytes);

    TRACKER_INIT_IF_EXTENDED(GetHullCompSizeBytes, getHullCompSize, Size in bytes);
    TRACKER_INIT_IF_EXTENDED(GetHullOnlineRtSizeBytes, getHullOnlineRtSize, Size in bytes);
    TRACKER_INIT_IF_EXTENDED(GetHullOnlineOtherSizeBytes, getHullOnlineOtherSize, Size in bytes);
    TRACKER_INIT_IF_EXTENDED(GetHullLoadSizeBytes, getHullLoadSize, Size in bytes);
    TRACKER_INIT_IF_EXTENDED(GetHullLowSizeBytes, getHullLowSize, Size in bytes);

    TRACKER_INIT_IF_EXTENDED(WriteSyncLogSizeBytes, writeSyncLogSize, Size in bytes);
    TRACKER_INIT_IF_EXTENDED(WriteHullFreshSizeBytes, writeHullFreshSize, Size in bytes);
    TRACKER_INIT_IF_EXTENDED(WriteHullHugeSizeBytes, writeHullHugeSize, Size in bytes);
    TRACKER_INIT_IF_EXTENDED(WriteHullCompSizeBytes, writeHullCompSize, Size in bytes);

    HISTOGRAM_INIT(LogResponseTime, logresponse);
    HISTOGRAM_INIT(GetResponseSyncLog, getResponseSyncLog);

    HISTOGRAM_INIT(GetResponseHullComp, getResponseHullComp);
    HISTOGRAM_INIT(GetResponseHullOnlineRt, getResponseHullOnlineRt);
    HISTOGRAM_INIT(GetResponseHullOnlineOther, getResponseHullOnlineOther);
    HISTOGRAM_INIT(GetResponseHullLoad, getResponseHullLoad);
    HISTOGRAM_INIT(GetResponseHullLow, getResponseHullLow);

    HISTOGRAM_INIT(WriteResponseSyncLog, writeResponseSyncLog);
    HISTOGRAM_INIT(WriteResponseHullFresh, writeResponseHullFresh);
    HISTOGRAM_INIT(WriteResponseHullHuge, writeResponseHullHuge);
    HISTOGRAM_INIT(WriteResponseHullComp, writeResponseHullComp);

    // bandwidth
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPLogPayload, Bandwidth/PDisk/Log/Payload);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPLogCommit, Bandwidth/PDisk/Log/Commit);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPLogSectorFooter, Bandwidth/PDisk/Log/SectorFooter);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPLogRecordHeader, Bandwidth/PDisk/Log/RecordHeader);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPLogPadding, Bandwidth/PDisk/Log/Padding);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPLogErasure, Bandwidth/PDisk/Log/Erasure);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPLogChunkPadding, Bandwidth/PDisk/Log/ChunkPadding);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPLogChunkFooter, Bandwidth/PDisk/Log/ChunkFooter);

    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPSysLogPayload, Bandwidth/PDisk/SysLog/Payload);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPSysLogSectorFooter, Bandwidth/PDisk/SysLog/SectorFooter);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPSysLogRecordHeader, Bandwidth/PDisk/SysLog/RecordHeader);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPSysLogPadding, Bandwidth/PDisk/SysLog/Padding);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPSysLogErasure, Bandwidth/PDisk/SysLog/Erasure);

    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPChunkPayload, Bandwidth/PDisk/Chunk/Payload);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPChunkSectorFooter, Bandwidth/PDisk/Chunk/SectorFooter);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPChunkPadding, Bandwidth/PDisk/Chunk/Padding);

    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPChunkReadPayload, Bandwidth/PDisk/ChunkRead/Payload);
    NAMED_DER_COUNTER_INIT_IF_EXTENDED(BandwidthGroup, BandwidthPChunkReadSectorFooter, Bandwidth/PDisk/ChunkRead/SectorFooter);

    // pdisk (interface)
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, YardInit, YardInit);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, CheckSpace, YardCheckSpace);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, YardConfigureScheduler, YardConfigureScheduler);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, ChunkReserve, YardChunkReserve);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, ChunkForget, YardChunkForget);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, Harakiri, YardHarakiri);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, YardSlay, YardSlay);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, YardControl, YardControl);

    IO_REQ_INIT(PDiskGroup, WriteSyncLog, WriteSyncLog);
    IO_REQ_INIT(PDiskGroup, WriteFresh, WriteFresh);
    IO_REQ_INIT(PDiskGroup, WriteHuge, WriteHuge);
    IO_REQ_INIT(PDiskGroup, WriteComp, WriteComp);
    IO_REQ_INIT(PDiskGroup, Trim, WriteTrim);

    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, ReadSyncLog, ReadSyncLog);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, ReadComp, ReadComp);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, ReadOnlineRt, ReadOnlineRt);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, ReadOnlineOther, ReadOnlineOther);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, ReadLoad, ReadLoad);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, ReadLow, ReadLow);

    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, Unknown, Unknown);

    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, WriteLog, WriteLog);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, WriteHugeLog, WriteHugeLog);
    IO_REQ_INIT_IF_EXTENDED(PDiskGroup, LogRead, ReadLog);

    COUNTER_INIT(PDiskGroup, PDiskThreadCPU, true);
    COUNTER_INIT(PDiskGroup, SubmitThreadCPU, true);
    COUNTER_INIT(PDiskGroup, GetThreadCPU, true);
    COUNTER_INIT(PDiskGroup, TrimThreadCPU, true);
    COUNTER_INIT(PDiskGroup, CompletionThreadCPU, true);
}

::NMonitoring::TDynamicCounters::TCounterPtr TPDiskMon::GetBusyPeriod(const TString& owner, const TString& queue) {
    return SchedulerGroup->GetCounter("SchedulerBusyPeriod_" + owner + "_" + queue, true);
}

void TPDiskMon::IncrementQueueTime(ui8 priorityClass, size_t timeMs) {
    switch (priorityClass) {
        case NPriRead::SyncLog:
            GetQueueSyncLog.Increment(timeMs);
        break;
        case NPriRead::HullComp:
            GetQueueHullComp.Increment(timeMs);
        break;
        case NPriRead::HullOnlineRt:
            GetQueueHullOnlineRt.Increment(timeMs);
        break;
        case NPriRead::HullOnlineOther:
            GetQueueHullOnlineOther.Increment(timeMs);
        break;
        case NPriRead::HullLoad:
            GetQueueHullLoad.Increment(timeMs);
        break;
        case NPriRead::HullLow:
            GetQueueHullLow.Increment(timeMs);
        break;

        case NPriWrite::SyncLog:
            WriteQueueSyncLog.Increment(timeMs);
        break;
        case NPriWrite::HullFresh:
            WriteQueueHullFresh.Increment(timeMs);
        break;
        case NPriWrite::HullHugeAsyncBlob:
        case NPriWrite::HullHugeUserData:
            WriteQueueHullHuge.Increment(timeMs);
        break;
        case NPriWrite::HullComp:
            WriteQueueHullComp.Increment(timeMs);
        break;

        default:
        break;
    }
}

void TPDiskMon::IncrementResponseTime(ui8 priorityClass, double timeMs, size_t sizeBytes) {
    switch (priorityClass) {
        case NPriRead::SyncLog:
            GetResponseSyncLog.Increment(timeMs);
            GetSyncLogSizeBytes.Increment(sizeBytes);
        break;
        case NPriRead::HullComp:
            GetResponseHullComp.Increment(timeMs);
            GetHullCompSizeBytes.Increment(sizeBytes);
        break;
        case NPriRead::HullOnlineRt:
            GetResponseHullOnlineRt.Increment(timeMs);
            GetHullOnlineRtSizeBytes.Increment(sizeBytes);
        break;
        case NPriRead::HullOnlineOther:
            GetResponseHullOnlineOther.Increment(timeMs);
            GetHullOnlineOtherSizeBytes.Increment(sizeBytes);
        break;
        case NPriRead::HullLoad:
            GetResponseHullLoad.Increment(timeMs);
            GetHullLoadSizeBytes.Increment(sizeBytes);
        break;
        case NPriRead::HullLow:
            GetResponseHullLow.Increment(timeMs);
            GetHullLowSizeBytes.Increment(sizeBytes);
        break;

        case NPriWrite::SyncLog:
            WriteResponseSyncLog.Increment(timeMs);
            WriteSyncLogSizeBytes.Increment(sizeBytes);
        break;
        case NPriWrite::HullFresh:
            WriteResponseHullFresh.Increment(timeMs);
            WriteHullFreshSizeBytes.Increment(sizeBytes);
        break;
        case NPriWrite::HullHugeAsyncBlob:
        case NPriWrite::HullHugeUserData:
            WriteResponseHullHuge.Increment(timeMs);
            WriteHullHugeSizeBytes.Increment(sizeBytes);
        break;
        case NPriWrite::HullComp:
            WriteResponseHullComp.Increment(timeMs);
            WriteHullCompSizeBytes.Increment(sizeBytes);
        break;

        default:
        break;
    }
}

void TPDiskMon::UpdatePercentileTrackers() {

    UpdateDurationTracker.UpdateCycleTime.Update();

    LogOperationSizeBytes.Update();

    GetSyncLogSizeBytes.Update();
    GetHullCompSizeBytes.Update();
    GetHullOnlineRtSizeBytes.Update();
    GetHullOnlineOtherSizeBytes.Update();
    GetHullLoadSizeBytes.Update();
    GetHullLowSizeBytes.Update();

    WriteSyncLogSizeBytes.Update();
    WriteHullFreshSizeBytes.Update();
    WriteHullHugeSizeBytes.Update();
    WriteHullCompSizeBytes.Update();

    LogQueueTime.Update();

    GetQueueSyncLog.Update();
    GetQueueHullComp.Update();
    GetQueueHullOnlineRt.Update();
    GetQueueHullOnlineOther.Update();
    GetQueueHullLoad.Update();
    GetQueueHullLow.Update();

    WriteQueueSyncLog.Update();
    WriteQueueHullFresh.Update();
    WriteQueueHullHuge.Update();
    WriteQueueHullComp.Update();

    SensitiveBurst.Update();
    BestEffortBurst.Update();

    InputQLA.Update();
    InputQCA.Update();
}

void TPDiskMon::UpdateLights() {
    if (HPSecondsFloat(std::abs(HPNow() - AtomicGet(LastDoneOperationTimestamp))) > 15.0) {
        auto seqnoL6 = AtomicGetAndIncrement(SeqnoL6);
        L6.Set(false, seqnoL6);
    }

    L6. Update();
    L7. Update();
    IdleLight.Update();
}

bool TPDiskMon::UpdateDeviceHaltCounters() {
    NHPTimer::STime hpNow = HPNow();
    if (*DeviceTakeoffs != *DeviceLandings) {
        // Halt?
        if (*DeviceTakeoffs == LastHaltDeviceTakeoffs &&
                *DeviceLandings == LastHaltDeviceLandings &&
                LastHaltDeviceLandings != LastHaltDeviceTakeoffs) {
            // Halt!
            if (hpNow > LastHaltTimestamp) {
                double haltDuration = HPSecondsFloat(std::abs(hpNow - LastHaltTimestamp));
                if (haltDuration > 7.5) {
                    *DeviceHaltDetected = 1;
                }
                if (haltDuration >= 60.0) {
                    return true;
                }
            } else {
                LastHaltTimestamp = hpNow;
            }
        } else {
            LastHaltDeviceTakeoffs = *DeviceTakeoffs;
            LastHaltDeviceLandings = *DeviceLandings;
            LastHaltTimestamp = hpNow;
        }
    }
    return false;
}

void TPDiskMon::UpdateStats() {
    ui64 freeChunks = *FreeChunks + *UntrimmedFreeChunks;
    ui64 usedChunks = *LogChunks + *UncommitedDataChunks + *CommitedDataChunks + *LockedChunks;
    ui64 totalChunks = freeChunks + usedChunks;
    ui64 freePerMile = totalChunks ? (freeChunks * 1000 / totalChunks) : 0;
    ui64 usedPerMile = 1000 - freePerMile;

    *FreeSpacePerMile = freePerMile;
    *UsedSpacePerMile = usedPerMile;
}

TPDiskMon::TIoCounters *TPDiskMon::GetWriteCounter(ui8 priority) {
    switch (priority) {
        case NPriWrite::SyncLog: return &WriteSyncLog;
        case NPriWrite::HullFresh: return &WriteFresh;
        case NPriWrite::HullHugeAsyncBlob: return &WriteHuge;
        case NPriWrite::HullHugeUserData: return &WriteHuge;
        case NPriWrite::HullComp: return &WriteComp;
    }
    return &Unknown;
}

TPDiskMon::TIoCounters *TPDiskMon::GetReadCounter(ui8 priority) {
    switch (priority) {
        case NPriRead::SyncLog: return &ReadSyncLog;
        case NPriRead::HullComp: return &ReadComp;
        case NPriRead::HullOnlineRt: return &ReadOnlineRt;
        case NPriRead::HullOnlineOther: return &ReadOnlineOther;
        case NPriRead::HullLoad: return &ReadLoad;
        case NPriRead::HullLow: return &ReadLow;
    }
    return &Unknown;
}

} // NKikimr

