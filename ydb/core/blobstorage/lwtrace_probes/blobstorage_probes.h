#pragma once

//#include <ydb/core/protos/base.pb.h>

#include <library/cpp/lwtrace/all.h>

// Helper class for printing pdisk id in the same was as it done for counters
struct TPDiskIdField {
    typedef ui32 TStoreType;
    typedef ui32 TFuncParam;

    static void ToString(ui32 value, TString* out) {
        *out = Sprintf("%09" PRIu32, value);
    }
    static ui32 ToStoreType(ui32 value) {
        return value;
    }
};

namespace NKikimr { namespace NPDisk {

struct TRequestTypeField {
    typedef ui32 TStoreType;
    typedef ui32 TFuncParam;

    static void ToString(ui32 value, TString* out);
    static ui32 ToStoreType(ui32 value) {
        return value;
    }
};

}}

namespace NKikimr {

struct TBlobPutTactics {
    typedef ui64 TStoreType;
    typedef ui64 TFuncParam;

    static void ToString(ui64 value, TString* out);
    static ui64 ToStoreType(ui64 value) {
        return value;
    }
};

struct TEventTypeField {
    typedef ui64 TStoreType;
    typedef ui64 TFuncParam;

    static void ToString(ui64 value, TString* out);
    static ui64 ToStoreType(ui64 value) {
        return value;
    }
};

}

#define BLOBSTORAGE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(DSProxyBatchedPutRequest, GROUPS("DSProxy"), \
        TYPES(ui64, ui32), \
        NAMES("count" , "groupId")) \
    PROBE(DSProxyBatchedGetRequest, GROUPS("DSProxy"), \
        TYPES(ui64, ui32), \
        NAMES("count" , "groupId")) \
    PROBE(ProxyPutBootstrapPart, GROUPS("Durations"), \
      TYPES(ui64, double, double, ui64, double), \
      NAMES("size", "waitMs", "splitMs", "splitCount", "splitElapsedMs")) \
    PROBE(DSProxyRequestDuration, GROUPS("DSProxyRequest", "DSProxy"), \
      TYPES(NKikimr::TEventTypeField, ui64, double, ui64, ui32, ui32, TString, bool), \
      NAMES("type", "size", "durationMs", "tabletId", "groupId", "channel", "handleClass", "isOk")) \
    PROBE(DSProxyVDiskRequestDuration, GROUPS("VDisk", "DSProxy"), \
      TYPES(NKikimr::TEventTypeField, ui64, ui64, ui32, ui32, ui32, double, double, double, double, TString, TString), \
      NAMES("type", "size", "tabletId", "groupId", "channel", "vdiskOrderNum", "startTime", "totalDurationMs", \
          "vdiskDurationMs", "transferDurationMs", "handleClass", "status")) \
    PROBE(VDiskSkeletonFrontVMovedPatchRecieved, GROUPS("VDisk", "DSProxy"), \
      TYPES(ui32, ui32, ui32, ui64, ui64), \
      NAMES("nodeId", "groupId", "vdiskOrderNum", "tabletId", "size")) \
    PROBE(VDiskSkeletonFrontVPatchStartRecieved, GROUPS("VDisk", "DSProxy"), \
      TYPES(ui32, ui32, ui32, ui64, ui64), \
      NAMES("nodeId", "groupId", "vdiskOrderNum", "tabletId", "size")) \
    PROBE(VDiskSkeletonFrontVPatchDiffRecieved, GROUPS("VDisk", "DSProxy"), \
      TYPES(ui32, ui32, ui32, ui64, ui64), \
      NAMES("nodeId", "groupId", "vdiskOrderNum", "tabletId", "size")) \
    PROBE(VDiskSkeletonFrontVPatchXorDiffRecieved, GROUPS("VDisk", "DSProxy"), \
      TYPES(ui32, ui32, ui32, ui64, ui64), \
      NAMES("nodeId", "groupId", "vdiskOrderNum", "tabletId", "size")) \
    PROBE(VDiskSkeletonFrontVPutRecieved, GROUPS("VDisk", "DSProxy"), \
      TYPES(ui32, ui32, ui32, ui64, ui64), \
      NAMES("nodeId", "groupId", "vdiskOrderNum", "tabletId", "size")) \
    PROBE(VDiskSkeletonFrontVMultiPutRecieved, GROUPS("VDisk", "DSProxy"), \
      TYPES(ui32, ui32, ui32, ui64, ui64), \
      NAMES("nodeId", "groupId", "vdiskOrderNum", "count", "size")) \
    PROBE(VDiskSkeletonVPutRecieved, GROUPS("VDisk", "DSProxy"), \
      TYPES(ui32, ui32, ui32, ui64, ui64), \
      NAMES("nodeId", "groupId", "vdiskOrderNum", "tabletId", "size")) \
    PROBE(VDiskSkeletonVMultiPutRecieved, GROUPS("VDisk", "DSProxy"), \
      TYPES(ui32, ui32, ui32, ui64, ui64), \
      NAMES("nodeId", "groupId", "vdiskOrderNum", "tabletId", "size")) \
    PROBE(VDiskRecoveryLogWriterVPutIsRecieved, GROUPS("VDisk"), \
      TYPES(ui32, ui64), \
      NAMES("owner", "lsn")) \
    PROBE(VDiskRecoveryLogWriterVPutIsSent, GROUPS("VDisk"), \
      TYPES(ui32, ui64), \
      NAMES("owner", "lsn")) \
    PROBE(VDiskSkeletonRecordLogged, GROUPS("VDisk"), \
      TYPES(ui64), \
      NAMES("lsn")) \
    PROBE(HugeKeeperWriteHugeBlobReceived, GROUPS("VDisk", "HullHuge"), \
      TYPES(), \
      NAMES()) \
    PROBE(HugeBlobChunkAllocatorStart, GROUPS("VDisk", "HullHuge"), \
      TYPES(), \
      NAMES()) \
    PROBE(HugeWriterStart, GROUPS("VDisk", "HullHuge"), \
      TYPES(), \
      NAMES()) \
    PROBE(HugeWriterFinish, GROUPS("VDisk", "HullHuge"), \
      TYPES(TString), \
      NAMES("status")) \
    PROBE(DSProxyBlobPutTactics, GROUPS("DSProxyRequest", "DSProxy"), \
      TYPES(ui64, ui32, TString, NKikimr::TBlobPutTactics, TString), \
      NAMES("tabletId", "groupId", "blob", "tactics", "handleClass")) \
    PROBE(DSProxyPutVPut, GROUPS("DSProxyRequest", "DSProxy"), \
      TYPES(ui64, ui32, ui32, ui32, TString, NKikimr::TBlobPutTactics, TString, ui32, ui32, \
          ui32, TString, double), \
      NAMES("tabletId", "groupId", "channel", "partId", "blob", "tactics", "handleClass", "blobSize", "partSize", \
          "vdiskOrderNum", "queueId", "predictedMs")) \
    PROBE(DSProxyPutVPutIsSent, GROUPS("DSProxyRequest", "DSProxy", "LWTrackStart"), \
      TYPES(ui64, ui32, ui32, ui32, TString, ui32), \
      NAMES("vdiskOrderNum", "groupId", "channel", "partId", "blob", "blobSize")) \
    PROBE(DSQueueVPutIsQueued, GROUPS("DSQueueRequest", "DSQueue"), \
      TYPES(ui32, TString, ui32, ui32, ui32), \
      NAMES("groupId", "blob", "channel", "partId", "blobSize")) \
    PROBE(DSQueueVPutIsSent, GROUPS("DSQueueRequest", "DSQueue"), \
      TYPES(double), \
      NAMES("inQueueMs")) \
    PROBE(DSQueueVPutResultRecieved, GROUPS("DSQueueRequest", "DSQueue"), \
      TYPES(double, ui32, bool), \
      NAMES("processingTimeMs", "size", "isDiscarded")) \
    PROBE(PDiskNewRequest, GROUPS("PDisk", "PDiskRequest", "LWTrackStart"), \
      TYPES(TPDiskIdField, ui64, double, double, bool, bool, ui64, ui64, NKikimr::NPDisk::TRequestTypeField), \
      NAMES("pdisk", "reqId", "creationTimeSec", "costMs", "isSensitive", "isFast", "owner", "priorityClass", "type")) \
    PROBE(PDiskFairSchedulerPush, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, TString, ui64, ui64, NKikimr::NPDisk::TRequestTypeField, ui64, double), \
      NAMES("pdisk", "reqId", "queueType", "owner", "priorityClass", "type", "fairQLA", "fairQCA")) \
    PROBE(PDiskFairSchedulerPop, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, TString, ui64, ui64, NKikimr::NPDisk::TRequestTypeField, ui64, double, bool), \
      NAMES("pdisk", "reqId", "queueType", "owner", "priorityClass", "type", "fairQLD", "fairQCD", "guarantyLost")) \
    PROBE(PDiskInputRequestTimeout, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, double), \
      NAMES("pdisk", "reqId", "deadlineSec")) \
    PROBE(PDiskInputRequest, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, double, double, ui64, double, double, ui64, bool, ui64, ui64), \
      NAMES("pdisk", "reqId", "creationTimeSec", "costMs", "inputQLA", "inputQCA", "deadlineSec", "owner", "isFast", \
          "priorityClass", "inputQueueSize")) \
    PROBE(PDiskAddToScheduler, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, double, ui64, bool, ui64), \
      NAMES("pdisk", "reqId", "creationTimeSec", "owner", "isFast", "priorityClass")) \
    PROBE(PDiskRouteRequest, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, double, ui64, bool, ui64), \
      NAMES("pdisk", "reqId", "creationTimeSec", "owner", "isFast", "priorityClass")) \
    PROBE(PDiskLogWriteFlush, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, double, double, double, ui64, bool, ui64), \
      NAMES("pdisk", "reqId", "creationTimeSec", "costMs", "deadlineSec", "owner", "isFast", "priorityClass")) \
    PROBE(PDiskBurst, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, double, bool, double, double), \
      NAMES("pdisk", "reqId", "creationTimeSec", "isSensitive", "costMs", "burstMs")) \
    PROBE(PDiskSchedulerPack, GROUPS("PDisk", "PDiskSchedulerStep"), \
      TYPES(TPDiskIdField, ui64, ui64, double, ui64), \
      NAMES("pdisk", "schedStep", "schedPack", "totalCostMs", "reqCount")) \
    PROBE(PDiskSchedulerAdhesion, GROUPS("PDisk"), \
      TYPES(TPDiskIdField, ui32, double, ui64, ui64), \
      NAMES("pdisk", "chunkIdx", "adhesionCostMs", "adhesionWrites", "adhesionReorderings")) \
    PROBE(PDiskSchedulerAdhesionLookup, GROUPS("PDisk"), \
      TYPES(TPDiskIdField, bool, bool, bool, bool, bool, bool, bool, bool), \
      NAMES("pdisk", "found", "queueEnd", "skipCostLimit", "skipCountLimit", "orderConstraint", "disabled", "adhesionSizeLimit", "interrupted")) \
    PROBE(PDiskSchedulerDispatch, GROUPS("PDisk", "PDiskSchedulerStep", "PDiskSchedulerSubStep", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, ui64, ui64, ui64, bool, double), \
      NAMES("pdisk", "schedStep", "schedSubStep", "reqId", "reqDispatched", "isSensitive", "costMs")) \
    PROBE(PDiskSchedulerSlack, GROUPS("PDisk", "PDiskSchedulerStep", "PDiskSchedulerSubStep"), \
      TYPES(TPDiskIdField, ui64, ui64, double, double, double, double), \
      NAMES("pdisk", "schedStep", "schedSubStep", "slackMs", "nonscheduledMs", "burstMs", "reorderingMs")) \
    PROBE(PDiskSchedulerInject, GROUPS("PDisk", "PDiskSchedulerStep"), \
      TYPES(TPDiskIdField, ui64, double, double, double, double), \
      NAMES("pdisk", "schedStep", "sensitiveQueueMs", "bestEffortQueueMs", "unusedBurstMs", "injectedMs")) \
    PROBE(PDiskSchedulerAllocIdleSlack, GROUPS("PDisk", "PDiskSchedulerStep"), \
      TYPES(TPDiskIdField, ui64, double, double, double), \
      NAMES("pdisk", "schedStep", "allocIdleSlackMaxMs", "slackMs", "allocatedIdleSlackMs")) \
    PROBE(PDiskSchedulerStepCost, GROUPS("PDisk", "PDiskSchedulerStep"), \
      TYPES(TPDiskIdField, ui64, ui64, double, ui64, double, ui64, double, ui64), \
      NAMES("pdisk", "schedStep", "schedPacks", "stepSensitiveCost", "stepSensitiveCount", "stepBestEffortCost", "stepBestEffortCount" \
          , "stepCost", "stepCount")) \
    PROBE(PDiskSchedulerStepInjected, GROUPS("PDisk", "PDiskSchedulerStep"), \
      TYPES(TPDiskIdField, ui64, double, ui64), \
      NAMES("pdisk", "schedStep", "stepInjectedCost", "stepInjectedCount")) \
    PROBE(PDiskSchedulerStepBlocks, GROUPS("PDisk", "PDiskSchedulerStep"), \
      TYPES(TPDiskIdField, ui64, ui64, ui64, ui64, ui64), \
      NAMES("pdisk", "schedStep", "bestEffortBlocks", "bestEffortUnblocks", "sensitiveBlocks", "sensitiveUnblocks")) \
    PROBE(PDiskSchedulerStepIdleSlack, GROUPS("PDisk", "PDiskSchedulerStep"), \
      TYPES(TPDiskIdField, ui64, double, ui64), \
      NAMES("pdisk", "schedStep", "stepAllocatedIdleSlackMs", "stepAllocatedIdleSlackCount")) \
    PROBE(PDiskSchedulerSubStep, GROUPS("PDisk", "PDiskSchedulerStep", "PDiskSchedulerSubStep"), \
      TYPES(TPDiskIdField, ui64, ui64, double, double), \
      NAMES("pdisk", "schedStep", "schedSubStep", "subStepCost", "subStepCount")) \
    PROBE(PDiskSchedulerStartStep, GROUPS("PDisk", "PDiskSchedulerStep", "LWTrackStart"), \
      TYPES(TPDiskIdField, ui64), \
      NAMES("pdisk", "schedStep")) \
    PROBE(PDiskChunkReadPieceAddToScheduler, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui32, ui64, ui64), \
      NAMES("pdisk", "pieceIdx", "offset", "size")) \
    PROBE(PDiskChunkReadPiecesSendToDevice, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField), \
      NAMES("pdisk")) \
    PROBE(PDiskChunkReadPieceComplete, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, ui64, double), \
      NAMES("pdisk", "size", "relativeOffset", "deviceTimeMs")) \
    PROBE(PDiskChunkWriteAddToScheduler, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, double, ui64, bool, ui64, ui64), \
      NAMES("pdisk", "reqId", "creationTimeSec", "owner", "isFast", "priorityClass", "size")) \
    PROBE(PDiskChunkWriteLastPieceSendToDevice, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, ui64, ui64, ui64), \
      NAMES("pdisk", "owner", "chunkIdx", "pieceOffset", "pieceSize")) \
    PROBE(PDiskLogWriteComplete, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, double, double, double, double, double, double, double), \
      NAMES("pdisk", "reqId", "creationTimeSec", "costMs", "responseTimeMs", "inputTimeMs", "scheduleTimeMs", "deviceTotalTimeMs", "deviceOnlyTimeMs")) \
    PROBE(PDiskChunkResponseTime, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, ui64, double, ui64), \
      NAMES("pdisk", "reqId", "priorityClass", "responseTimeMs", "sizeBytes")) \
    PROBE(PDiskTrimResponseTime, GROUPS("PDisk", "PDiskRequest"), \
      TYPES(TPDiskIdField, ui64, double, ui64), \
      NAMES("pdisk", "reqId", "responseTimeMs", "sizeBytes")) \
    PROBE(PDiskDeviceReadDuration, GROUPS("PDisk"), \
      TYPES(TPDiskIdField, double, ui64), \
      NAMES("pdisk", "deviceTimeMs", "size")) \
    PROBE(PDiskDeviceWriteDuration, GROUPS("PDisk"), \
      TYPES(TPDiskIdField, double, ui64), \
      NAMES("pdisk", "deviceTimeMs", "size")) \
    PROBE(PDiskDeviceGetFromDevice, GROUPS("PDisk"), \
      TYPES(), \
      NAMES()) \
    PROBE(PDiskDeviceGetFromWaiting, GROUPS("PDisk"), \
      TYPES(), \
      NAMES()) \
    PROBE(PDiskDeviceTrimDuration, GROUPS("PDisk"), \
      TYPES(TPDiskIdField, double, ui64), \
      NAMES("pdisk", "trimTimeMs", "trimOffset")) \
    PROBE(PDiskDeviceOperationSizeAndType, GROUPS("PDisk"), \
      TYPES(TPDiskIdField, ui64, ui64), \
      NAMES("pdisk", "operationSize", "operationType")) \
    PROBE(PDiskMilliBatchSize, GROUPS("PDisk"), \
      TYPES(TPDiskIdField, ui64, ui64, ui64, ui64), \
      NAMES("pdisk", "milliBatchLogCost", "milliBatchNonLogCost", "milliBatchLogReqs", "milliBatchNonLogReqs")) \
    PROBE(PDiskHandleWakeup, GROUPS("PDisk"), \
      TYPES(TPDiskIdField, double, double, double), \
      NAMES("pdisk", "updatePercentileTrackersMs", "whiteboardReportMs", "updateSchedulerMs")) \
    PROBE(PDiskForsetiCycle, GROUPS("PDisk"), \
      TYPES(TPDiskIdField, ui64, ui64, ui64, ui64, ui64, ui64, ui64, ui64, ui64), \
      NAMES("pdisk", "realTimeNs", "uncorrectedForsetiTimeNs", "correctedForsetiTimeNs", "timeCorrectionNs", \
            "realDurationNs", "virtualDurationNs", "newForsetiTimeNs", "totalCostNs", "virtualDeadlineNs")) \
    PROBE(LoadActorEvChunkReadCreated, GROUPS("LoadActor", "PDiskEvent"), \
      TYPES(ui32, ui64, ui64), \
      NAMES("chunkIdx", "size", "offset")) \
    PROBE(PDiskUpdateCycleDetails, GROUPS("PDisk"), \
      TYPES(float, float, float, float, float), \
      NAMES("entireUpdateMs", "inputQueueMs", "schedulingMs", "processingMs", "waitingMs")) \
    PROBE(DSProxyGetEnqueue, GROUPS("DSProxy", "LWTrackStart"), TYPES(), NAMES()) \
    PROBE(DSProxyGetBootstrap, GROUPS("DSProxy"), TYPES(), NAMES()) \
    PROBE(DSProxyGetHandle, GROUPS("DSProxy", "LWTrackStart"), TYPES(), NAMES()) \
    PROBE(DSProxyGetReply, GROUPS("DSProxy"), TYPES(), NAMES()) \
    PROBE(DSProxyPutEnqueue, GROUPS("DSProxy", "LWTrackStart"), TYPES(), NAMES()) \
    PROBE(DSProxyPutHandle, GROUPS("DSProxyRequest", "DSProxy", "LWTrackStart"), TYPES(), NAMES()) \
    PROBE(DSProxyPutBootstrapStart, GROUPS("DSProxy"), TYPES(), NAMES()) \
    PROBE(DSProxyPutBootstrapDone, GROUPS("DSProxy","Durations"), \
      TYPES(ui64, double, double, double, double, ui64, ui64), \
      NAMES("size", "wilsonMs", "allocateMs", "waitTotalMs", "splitTotalMs", "splitTotalCount", "blobIdx")) \
    PROBE(DSProxyPutReply, GROUPS("DSProxy"), TYPES(TString, TString, TString), NAMES("blobId", "status", "errorReason")) \
    PROBE(DSProxyPutResumeBootstrap, GROUPS("DSProxy"), TYPES(), NAMES()) \
    PROBE(DSProxyPutPauseBootstrap, GROUPS("DSProxy"), TYPES(), NAMES()) \
    PROBE(DSProxyScheduleAccelerate, GROUPS("DSProxy"), TYPES(double), NAMES("timeBeforeAccelerationMs")) \
    PROBE(DSProxyStartTransfer, GROUPS("DSProxy"), TYPES(), NAMES()) \
    PROBE(VDiskStartProcessing, GROUPS("DSProxy"), TYPES(), NAMES()) \
    PROBE(VDiskReply, GROUPS("DSProxy"), TYPES(), NAMES()) \
    PROBE(DSProxyPutRequest, GROUPS("DSProxy", "LWTrackStart"), TYPES(ui32, TString, TString, ui64, ui64), NAMES("groupId", "handleClass", "tactic", "count", "totalSize")) \
    PROBE(DSProxyVPutSent, GROUPS("DSProxy"), TYPES(NKikimr::TEventTypeField, TString, ui32, ui32, ui64, bool), NAMES("type", "vDiskId", "vdiskOrderNum", "count", "totalSize", "accelerate")) \
/**/
LWTRACE_DECLARE_PROVIDER(BLOBSTORAGE_PROVIDER)

#define FAIL_INJECTION_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(PDiskFailInjection, GROUPS("PDisk"), TYPES(ui64), NAMES("cookie")) \
/**/
LWTRACE_DECLARE_PROVIDER(FAIL_INJECTION_PROVIDER)

#define PDISK_FAIL_INJECTION(COOKIE) GLOBAL_LWPROBE(FAIL_INJECTION_PROVIDER, PDiskFailInjection, COOKIE)
