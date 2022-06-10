#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_requestimpl.h"
#include <ydb/core/blobstorage/base/vdisk_priorities.h>

namespace NKikimr {

TPDiskMon::TPDiskMon(const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters, ui32 pDiskId,
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
    // chunk states subgroup
    UntrimmedFreeChunks = ChunksGroup->GetCounter("UntrimmedFreeChunks");
    FreeChunks = ChunksGroup->GetCounter("FreeChunks");
    LogChunks = ChunksGroup->GetCounter("LogChunks");
    UncommitedDataChunks = ChunksGroup->GetCounter("UncommitedDataChunks");
    CommitedDataChunks = ChunksGroup->GetCounter("CommitedDataChunks");
    LockedChunks = ChunksGroup->GetCounter("LockedChunks");
    QuarantineChunks = ChunksGroup->GetCounter("QuarantineChunks");
    QuarantineOwners = ChunksGroup->GetCounter("QuarantineOwners");

    // stats subgroup
    StatsGroup = (cfg && cfg->PDiskCategory.IsSolidState())
        ? Counters->GetSubgroup("type", "ssd_excl")
        : Counters->GetSubgroup("type", "hdd_excl");
    StatsGroup = StatsGroup->GetSubgroup("subsystem", "stats");

    FreeSpacePerMile = StatsGroup->GetCounter("FreeSpacePerMile");
    UsedSpacePerMile = StatsGroup->GetCounter("UsedSpacePerMile");
    SplicedLogChunks = StatsGroup->GetCounter("SplicedLogChunks", true);

    TotalSpaceBytes = StatsGroup->GetCounter("TotalSpaceBytes");
    FreeSpaceBytes = StatsGroup->GetCounter("FreeSpaceBytes");
    UsedSpaceBytes = StatsGroup->GetCounter("UsedSpaceBytes");
    SectorMapAllocatedBytes = StatsGroup->GetCounter("SectorMapAllocatedBytes");

    // states subgroup
    PDiskState = StateGroup->GetCounter("PDiskState");
    PDiskBriefState = StateGroup->GetCounter("PDiskBriefState");
    PDiskDetailedState = StateGroup->GetCounter("PDiskDetailedState");
    AtLeastOneVDiskNotLogged = StateGroup->GetCounter("AtLeastOneVDiskNotLogged");
    TooMuchLogChunks = StateGroup->GetCounter("TooMuchLogChunks");
    SerialNumberMismatched = StateGroup->GetCounter("SerialNumberMismatched");
    L6. Initialize(StateGroup, "L6");
    L7. Initialize(StateGroup, "L7");
    IdleLight.Initialize(StateGroup, "DeviceBusyPeriods", "DeviceIdleTimeMsPerSec", "DeviceBusyTimeMsPerSec");

    OwnerIdsIssued = StateGroup->GetCounter("OwnerIdsIssued");
    LastOwnerId = StateGroup->GetCounter("LastOwnerId");
    PendingYardInits = StateGroup->GetCounter("PendingYardInits");

    SeqnoL6 = 0;
    LastDoneOperationTimestamp = 0;

    // device subgroup
    DeviceBytesRead = DeviceGroup->GetCounter("DeviceBytesRead", true);
    DeviceBytesWritten = DeviceGroup->GetCounter("DeviceBytesWritten", true);
    DeviceReads = DeviceGroup->GetCounter("DeviceReads", true);
    DeviceWrites = DeviceGroup->GetCounter("DeviceWrites", true);
    DeviceInFlightBytesRead = DeviceGroup->GetCounter("DeviceInFlightBytesRead");
    DeviceInFlightBytesWrite = DeviceGroup->GetCounter("DeviceInFlightBytesWrite");
    DeviceInFlightReads = DeviceGroup->GetCounter("DeviceInFlightReads");
    DeviceInFlightWrites = DeviceGroup->GetCounter("DeviceInFlightWrites");
    DeviceTakeoffs = DeviceGroup->GetCounter("DeviceTakeoffs");
    DeviceLandings = DeviceGroup->GetCounter("DeviceLandings");
    DeviceHaltDetected = DeviceGroup->GetCounter("DeviceHaltDetected");
    DeviceExpectedSeeks = DeviceGroup->GetCounter("DeviceExpectedSeeks", true);
    DeviceReadCacheHits = DeviceGroup->GetCounter("DeviceReadCacheHits", true);
    DeviceReadCacheMisses = DeviceGroup->GetCounter("DeviceReadCacheMisses", true);
    DeviceWriteCacheIsValid = DeviceGroup->GetCounter("DeviceWriteCacheIsValid");
    DeviceWriteCacheIsEnabled = DeviceGroup->GetCounter("DeviceWriteCacheIsEnabled");
    DeviceOperationPoolTotalAllocations = DeviceGroup->GetCounter("DeviceOperationPoolTotalAllocations");
    DeviceOperationPoolFreeObjectsMin = DeviceGroup->GetCounter("DeviceOperationPoolFreeObjectsMin");
    DeviceBufferPoolFailedAllocations = DeviceGroup->GetCounter("DeviceBufferPoolFailedAllocations");
    DeviceErasureSectorRestorations = DeviceGroup->GetCounter("DeviceErasureSectorRestorations");
    DeviceEstimatedCostNs = DeviceGroup->GetCounter("DeviceEstimatedCostNs", true);
    DeviceActualCostNs = DeviceGroup->GetCounter("DeviceActualCostNs", true);
    DeviceOverestimationRatio = DeviceGroup->GetCounter("DeviceOverestimationRatio");
    DeviceNonperformanceMs = DeviceGroup->GetCounter("DeviceNonperformanceMs");
    DeviceInterruptedSystemCalls = DeviceGroup->GetCounter("DeviceInterruptedSystemCalls", true);
    DeviceSubmitThreadBusyTimeNs = DeviceGroup->GetCounter("DeviceSubmitThreadBusyTimeNs", true);
    DeviceCompletionThreadBusyTimeNs = DeviceGroup->GetCounter("DeviceCompletionThreadBusyTimeNs", true);
    DeviceIoErrors = DeviceGroup->GetCounter("DeviceIoErrors", true);

    UpdateDurationTracker.SetCounter(DeviceGroup->GetCounter("PDiskThreadBusyTimeNs", true));

    // queue subgroup
    QueueRequests = QueueGroup->GetCounter("QueueRequests", true);
    QueueBytes = QueueGroup->GetCounter("QueueBytes", true);

    auto deviceType = cfg ? cfg->PDiskCategory.Type() : NPDisk::DEVICE_TYPE_UNKNOWN;

    // scheduler subgroup
    ForsetiCbsNotFound = SchedulerGroup->GetCounter("ForsetiCbsNotFound");

    TVector<float> percentiles;
    percentiles.push_back(0.50f);
    percentiles.push_back(0.90f);
    percentiles.push_back(0.99f);
    percentiles.push_back(1.00f);

    UpdateDurationTracker.UpdateCycleTime.Initialize(counters, "subsystem", "updateCycle", "Time in millisec", percentiles);

    DeviceReadDuration.Initialize(counters, "deviceReadDuration", deviceType);
    DeviceWriteDuration.Initialize(counters, "deviceWriteDuration", deviceType);
    DeviceTrimDuration.Initialize(counters, "deviceTrimDuration", deviceType);

    LogQueueTime.Initialize(counters, "subsystem", "logQueueTime", "Time in millisec", percentiles);
    GetQueueSyncLog.Initialize(counters, "subsystem", "getQueueSyncLog", "Time in millisec", percentiles);
    GetQueueHullComp.Initialize(counters, "subsystem", "getQueueHullComp", "Time in millisec", percentiles);
    GetQueueHullOnlineRt.Initialize(counters, "subsystem", "getQueueHullOnlineRt", "Time in millisec", percentiles);
    GetQueueHullOnlineOther.Initialize(counters, "subsystem", "getQueueHullOnlineOther",
            "Time in millisec", percentiles);
    GetQueueHullLoad.Initialize(counters, "subsystem", "getQueueHullLoad", "Time in millisec", percentiles);
    GetQueueHullLow.Initialize(counters, "subsystem", "getQueueHullLow", "Time in millisec", percentiles);
    WriteQueueSyncLog.Initialize(counters, "subsystem", "writeQueueSyncLog", "Time in millisec", percentiles);
    WriteQueueHullFresh.Initialize(counters, "subsystem", "writeQueueHullFresh", "Time in millisec", percentiles);
    WriteQueueHullHuge.Initialize(counters, "subsystem", "writeQueueHullHuge", "Time in millisec", percentiles);
    WriteQueueHullComp.Initialize(counters, "subsystem", "writeQueueHullComp", "Time in millisec", percentiles);

    SensitiveBurst.Initialize(counters, "subsystem", "sensitiveBurst", "Time in millisec", percentiles);
    BestEffortBurst.Initialize(counters, "subsystem", "bestEffortBurst", "Time in millisec", percentiles);

    InputQLA.Initialize(counters, "subsystem", "inputQLA", "Queue length seen by arrivals", percentiles);
    InputQCA.Initialize(counters, "subsystem", "inputQCA", "Queue cost seen by arrivals", percentiles);

    LogOperationSizeBytes.Initialize(counters, "subsystem", "logOperationSize", "Size in bytes", percentiles);
    GetSyncLogSizeBytes.Initialize(counters, "subsystem", "getSyncLogSize", "Size in bytes", percentiles);

    GetHullCompSizeBytes.Initialize(counters, "subsystem", "getHullCompSize", "Size in bytes", percentiles);
    GetHullOnlineRtSizeBytes.Initialize(counters, "subsystem", "getHullOnlineRtSize", "Size in bytes", percentiles);
    GetHullOnlineOtherSizeBytes.Initialize(counters, "subsystem", "getHullOnlineOtherSize", "Size in bytes",
        percentiles);
    GetHullLoadSizeBytes.Initialize(counters, "subsystem", "getHullLoadSize", "Size in bytes", percentiles);
    GetHullLowSizeBytes.Initialize(counters, "subsystem", "getHullLowSize", "Size in bytes", percentiles);

    WriteSyncLogSizeBytes.Initialize(counters, "subsystem", "writeSyncLogSize", "Size in bytes", percentiles);
    WriteHullFreshSizeBytes.Initialize(counters, "subsystem", "writeHullFreshSize", "Size in bytes", percentiles);
    WriteHullHugeSizeBytes.Initialize(counters, "subsystem", "writeHullHugeSize", "Size in bytes", percentiles);
    WriteHullCompSizeBytes.Initialize(counters, "subsystem", "writeHullCompSize", "Size in bytes", percentiles);

    LogResponseTime.Initialize(counters, "logresponse", deviceType);
    GetResponseSyncLog.Initialize(counters, "getResponseSyncLog", deviceType);

    GetResponseHullComp.Initialize(counters, "getResponseHullComp", deviceType);
    GetResponseHullOnlineRt.Initialize(counters, "getResponseHullOnlineRt", deviceType);
    GetResponseHullOnlineOther.Initialize(counters, "getResponseHullOnlineOther", deviceType);
    GetResponseHullLoad.Initialize(counters, "getResponseHullLoad", deviceType);
    GetResponseHullLow.Initialize(counters, "getResponseHullLow", deviceType);

    WriteResponseSyncLog.Initialize(counters, "writeResponseSyncLog", deviceType);
    WriteResponseHullFresh.Initialize(counters, "writeResponseHullFresh", deviceType);
    WriteResponseHullHuge.Initialize(counters, "writeResponseHullHuge", deviceType);
    WriteResponseHullComp.Initialize(counters, "writeResponseHullComp", deviceType);

    // bandwidth
    BandwidthPLogPayload = BandwidthGroup->GetCounter("Bandwidth/PDisk/Log/Payload", true);
    BandwidthPLogCommit = BandwidthGroup->GetCounter("Bandwidth/PDisk/Log/Commit", true);
    BandwidthPLogSectorFooter = BandwidthGroup->GetCounter("Bandwidth/PDisk/Log/SectorFooter", true);
    BandwidthPLogRecordHeader = BandwidthGroup->GetCounter("Bandwidth/PDisk/Log/RecordHeader", true);
    BandwidthPLogPadding = BandwidthGroup->GetCounter("Bandwidth/PDisk/Log/Padding", true);
    BandwidthPLogErasure = BandwidthGroup->GetCounter("Bandwidth/PDisk/Log/Erasure", true);
    BandwidthPLogChunkPadding = BandwidthGroup->GetCounter("Bandwidth/PDisk/Log/ChunkPadding", true);
    BandwidthPLogChunkFooter = BandwidthGroup->GetCounter("Bandwidth/PDisk/Log/ChunkFooter", true);

    BandwidthPSysLogPayload = BandwidthGroup->GetCounter("Bandwidth/PDisk/SysLog/Payload", true);
    BandwidthPSysLogSectorFooter = BandwidthGroup->GetCounter("Bandwidth/PDisk/SysLog/SectorFooter", true);
    BandwidthPSysLogRecordHeader = BandwidthGroup->GetCounter("Bandwidth/PDisk/SysLog/RecordHeader", true);
    BandwidthPSysLogPadding = BandwidthGroup->GetCounter("Bandwidth/PDisk/SysLog/Padding", true);
    BandwidthPSysLogErasure = BandwidthGroup->GetCounter("Bandwidth/PDisk/SysLog/Erasure", true);

    BandwidthPChunkPayload = BandwidthGroup->GetCounter("Bandwidth/PDisk/Chunk/Payload", true);
    BandwidthPChunkSectorFooter = BandwidthGroup->GetCounter("Bandwidth/PDisk/Chunk/SectorFooter", true);
    BandwidthPChunkPadding = BandwidthGroup->GetCounter("Bandwidth/PDisk/Chunk/Padding", true);

    BandwidthPChunkReadPayload = BandwidthGroup->GetCounter("Bandwidth/PDisk/ChunkRead/Payload", true);
    BandwidthPChunkReadSectorFooter = BandwidthGroup->GetCounter("Bandwidth/PDisk/ChunkRead/SectorFooter", true);

    // pdisk (interface)
    YardInit.Setup(PDiskGroup, "YardInit");
    CheckSpace.Setup(PDiskGroup, "YardCheckSpace");
    YardConfigureScheduler.Setup(PDiskGroup, "YardConfigureScheduler");
    ChunkReserve.Setup(PDiskGroup, "YardChunkReserve");
    Harakiri.Setup(PDiskGroup, "YardHarakiri");
    YardSlay.Setup(PDiskGroup, "YardSlay");
    YardControl.Setup(PDiskGroup, "YardControl");

    WriteSyncLog.Setup(PDiskGroup, "WriteSyncLog");
    WriteFresh.Setup(PDiskGroup, "WriteFresh");
    WriteHuge.Setup(PDiskGroup, "WriteHuge");
    WriteComp.Setup(PDiskGroup, "WriteComp");
    Trim.Setup(PDiskGroup, "WriteTrim");

    ReadSyncLog.Setup(PDiskGroup, "ReadSyncLog");
    ReadComp.Setup(PDiskGroup, "ReadComp");
    ReadOnlineRt.Setup(PDiskGroup, "ReadOnlineRt");
    ReadOnlineOther.Setup(PDiskGroup, "ReadOnlineOther");
    ReadLoad.Setup(PDiskGroup,"ReadLoad");
    ReadLow.Setup(PDiskGroup, "ReadLow");

    Unknown.Setup(PDiskGroup,"Unknown");

    WriteLog.Setup(PDiskGroup, "WriteLog");
    WriteHugeLog.Setup(PDiskGroup, "WriteHugeLog");
    LogRead.Setup(PDiskGroup, "ReadLog");

    PDiskThreadCPU = PDiskGroup->GetCounter("PDiskThreadCPU", true);
    SubmitThreadCPU = PDiskGroup->GetCounter("SubmitThreadCPU", true);
    GetThreadCPU = PDiskGroup->GetCounter("GetThreadCPU", true);
    TrimThreadCPU = PDiskGroup->GetCounter("TrimThreadCPU", true);
    CompletionThreadCPU = PDiskGroup->GetCounter("CompletionThreadCPU", true);
}

NMonitoring::TDynamicCounters::TCounterPtr TPDiskMon::GetBusyPeriod(const TString& owner, const TString& queue) {
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

