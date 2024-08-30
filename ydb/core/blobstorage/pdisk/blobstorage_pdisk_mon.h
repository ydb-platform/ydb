#pragma once

#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>
#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/util/light.h>

#include <library/cpp/bucket_quoter/bucket_quoter.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/percentile/percentile_lg.h>

namespace NKikimr {

struct TPDiskConfig;

class TBurstmeter {
private:
    TBucketQuoter<i64, TSpinLock, THPTimerUs> Bucket;
    NMonitoring::TPercentileTrackerLg<5, 4, 15> Tracker;
public:
    TBurstmeter()
        : Bucket(1000ull * 1000ull * 1000ull, 0)
    {}

    void Initialize(const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters,
                    const TString& group, const TString& subgroup, const TString& name,
                    const TVector<float> &thresholds, 
                    NMonitoring::TCountableBase::EVisibility visibility = NMonitoring::TCountableBase::EVisibility::Public) {
        Tracker.Initialize(counters, group, subgroup, name, thresholds, visibility);
    }

    double Increment(ui64 tokens) {
        double burst = -double(Bucket.UseAndFill(tokens)) / (1000000ull);
        Tracker.Increment(burst);
        return burst;
    }

    void Update() {
        Tracker.Update();
    }
};

class THistogram {
private:
    NMonitoring::THistogramPtr Histo;

public:
    void Initialize(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            const TString &name, NPDisk::EDeviceType deviceType) {
        TString histName = name + "Ms";
        // Histogram backets in milliseconds
        auto h = NMonitoring::ExplicitHistogram(GetCommonLatencyHistBounds(deviceType));
        Histo = counters->GetNamedHistogram("sensor", histName, std::move(h));
    }

    void Increment(double timeMs) {
        if (Histo) {
            Histo->Collect(timeMs);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk monitoring counters
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TPDiskMon {
    struct TPDisk {
        enum EBriefState {
            Booting,
            OK,
            Error,
        };

        enum EDetailedState {
            EverythingIsOk,
            BootingFormatRead,
            BootingSysLogRead,
            BootingCommonLogRead,
            BootingFormatMagicChecking,
            BootingDeviceFormattingAndTrimming,
            ErrorInitialFormatRead,
            ErrorInitialFormatReadDueToGuid,
            ErrorInitialFormatReadIncompleteFormat,
            ErrorDiskCannotBeFormated,
            ErrorPDiskCannotBeInitialised,
            ErrorInitialSysLogRead,
            ErrorInitialSysLogParse,
            ErrorInitialCommonLogRead,
            ErrorInitialCommonLogParse,
            ErrorCommonLoggerInit,
            ErrorOpenNonexistentFile,
            ErrorOpenFileWithoutPermissions,
            ErrorOpenFileUnknown,
            ErrorCalculatingChunkQuotas,
            ErrorDeviceIoError,
            ErrorNoDeviceWithSuchSerial,
            ErrorDeviceSerialMismatch,
            ErrorFake,
            BootingReencryptingFormat,
        };

        static TString StateToStr(i64 val) {
            return NKikimrBlobStorage::TPDiskState::E_Name(static_cast<NKikimrBlobStorage::TPDiskState::E>(val));
        }

        static const char *BriefStateToStr(i64 val) {
            switch (val) {
                case Booting: return "Booting";
                case OK: return "OK";
                case Error: return "Error";
                default: return "Unknown";
            }
        }

        static const char *DetailedStateToStr(i64 val) {
            switch (val) {
                case EverythingIsOk: return "EverythingIsOk";
                case BootingFormatRead: return "BootingSysLogRead";
                case BootingSysLogRead: return "BootingSysLogRead";
                case BootingCommonLogRead: return "BootingCommonLogRead";
                case BootingFormatMagicChecking: return "BootingFormatMagicChecking";
                case BootingDeviceFormattingAndTrimming: return "BootingDeviceFormattingAndTrimming";
                case ErrorInitialFormatRead: return "ErrorInitialFormatRead";
                case ErrorInitialFormatReadDueToGuid: return "ErrorInitialFormatReadDueToGuid";
                case ErrorInitialFormatReadIncompleteFormat: return "ErrorInitialFormatReadIncompleteFormat";
                case ErrorDiskCannotBeFormated: return "ErrorDiskCannotBeFormated";
                case ErrorPDiskCannotBeInitialised: return "ErrorPDiskCannotBeInitialised";
                case ErrorInitialSysLogRead: return "ErrorInitialSysLogRead";
                case ErrorInitialSysLogParse: return "ErrorInitialSysLogParse";
                case ErrorInitialCommonLogRead: return "ErrorInitialCommonLogRead";
                case ErrorInitialCommonLogParse: return "ErrorInitialCommonLogParse";
                case ErrorCommonLoggerInit: return "ErrorCommonLoggerInit";
                case ErrorOpenNonexistentFile: return "ErrorOpenNonexistentFile";
                case ErrorOpenFileWithoutPermissions: return "ErrorOpenFileWithoutPermissions";
                case ErrorOpenFileUnknown: return "ErrorOpenFileUnknown";
                case ErrorCalculatingChunkQuotas: return "ErrorCalculatingChunkQuotas";
                case ErrorDeviceIoError: return "ErrorDeviceIoError";
                case ErrorNoDeviceWithSuchSerial: return "ErrorNoDeviceWithSuchSerial";
                case ErrorDeviceSerialMismatch: return "ErrorDeviceSerialMismatch";
                case ErrorFake: return "ErrorFake";
                case BootingReencryptingFormat: return "BootingReencryptingFormat";
                default: return "Unknown";
            }
        }
    };

    class TUpdateDurationTracker {
        bool IsLwProbeEnabled = false;
        NHPTimer::STime BeginUpdateAt = 0;
        NHPTimer::STime SchedulingStartAt = 0;
        NHPTimer::STime ProcessingStartAt = 0;
        NHPTimer::STime WaitingStartAt = 0;

        ::NMonitoring::TDynamicCounters::TCounterPtr PDiskThreadBusyTimeNs;

    public:
        NMonitoring::TPercentileTrackerLg<5, 4, 15> UpdateCycleTime;

    public:
        TUpdateDurationTracker()
            : BeginUpdateAt(HPNow())
        {}

        void SetCounter(const ::NMonitoring::TDynamicCounters::TCounterPtr& pDiskThreadBusyTimeNs) {
            PDiskThreadBusyTimeNs = pDiskThreadBusyTimeNs;
        }

        void UpdateStarted() {
            // BeginUpdateAt is set on the end of previous update cycle
            IsLwProbeEnabled = GLOBAL_LWPROBE_ENABLED(BLOBSTORAGE_PROVIDER, PDiskUpdateCycleDetails);
        }

        void SchedulingStart() {
            if (IsLwProbeEnabled) {
                SchedulingStartAt = HPNow();
            }
        }

        void ProcessingStart() {
            if (IsLwProbeEnabled) {
                ProcessingStartAt = HPNow();
            }
        }

        void WaitingStart(bool isNothingToDo) {
            const auto now = HPNow();
            if (PDiskThreadBusyTimeNs) {
                *PDiskThreadBusyTimeNs += HPNanoSeconds(now - BeginUpdateAt);
            }
            if (IsLwProbeEnabled || !isNothingToDo) {
                WaitingStartAt = now;
                if (!isNothingToDo) {
                    ui64 durationMs = HPMilliSeconds(WaitingStartAt - BeginUpdateAt);
                    UpdateCycleTime.Increment(durationMs);
                }
            }
        }

        void UpdateEnded() {
            NHPTimer::STime updateEndedAt = HPNow();
            if (IsLwProbeEnabled) {
                float entireUpdateMs = HPMilliSecondsFloat(updateEndedAt - BeginUpdateAt);
                float inputQueueMs = HPMilliSecondsFloat(SchedulingStartAt - BeginUpdateAt);
                float schedulingMs = HPMilliSecondsFloat(ProcessingStartAt - SchedulingStartAt);
                float processingMs = HPMilliSecondsFloat(WaitingStartAt - ProcessingStartAt);
                float waitingMs = HPMilliSecondsFloat(updateEndedAt - WaitingStartAt);
                GLOBAL_LWPROBE(BLOBSTORAGE_PROVIDER, PDiskUpdateCycleDetails, entireUpdateMs, inputQueueMs,
                        schedulingMs, processingMs, waitingMs);
            }
            BeginUpdateAt = updateEndedAt;
        }
    };

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    ui32 PDiskId;

    // chunk states subgroup
    TIntrusivePtr<::NMonitoring::TDynamicCounters> ChunksGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr UntrimmedFreeChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr FreeChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr LogChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr UncommitedDataChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr CommitedDataChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr LockedChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr QuarantineChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr QuarantineOwners;

    // statistics subgroup
    TIntrusivePtr<::NMonitoring::TDynamicCounters> StatsGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr FreeSpacePerMile;
    ::NMonitoring::TDynamicCounters::TCounterPtr UsedSpacePerMile;
    ::NMonitoring::TDynamicCounters::TCounterPtr SplicedLogChunks;

    ::NMonitoring::TDynamicCounters::TCounterPtr TotalSpaceBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr FreeSpaceBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr UsedSpaceBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr SectorMapAllocatedBytes;

    // states subgroup
    TIntrusivePtr<::NMonitoring::TDynamicCounters> StateGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr PDiskState;
    ::NMonitoring::TDynamicCounters::TCounterPtr PDiskBriefState;
    ::NMonitoring::TDynamicCounters::TCounterPtr PDiskDetailedState;
    ::NMonitoring::TDynamicCounters::TCounterPtr AtLeastOneVDiskNotLogged;
    ::NMonitoring::TDynamicCounters::TCounterPtr TooMuchLogChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr SerialNumberMismatched;
    TLight L6;
    TLight L7;
    TLight IdleLight;
    ::NMonitoring::TDynamicCounters::TCounterPtr OwnerIdsIssued;
    ::NMonitoring::TDynamicCounters::TCounterPtr LastOwnerId;
    ::NMonitoring::TDynamicCounters::TCounterPtr PendingYardInits;

    TAtomic SeqnoL6;
    TAtomic LastDoneOperationTimestamp;

    // device subgroup
    TIntrusivePtr<::NMonitoring::TDynamicCounters> DeviceGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceBytesRead;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceBytesWritten;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceReads;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceWrites;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceInFlightBytesRead;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceInFlightBytesWrite;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceInFlightReads;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceInFlightWrites;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceTakeoffs;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceLandings;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceHaltDetected;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceExpectedSeeks;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceReadCacheHits;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceReadCacheMisses;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceWriteCacheIsValid;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceWriteCacheIsEnabled;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceOperationPoolTotalAllocations;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceOperationPoolFreeObjectsMin;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceBufferPoolFailedAllocations;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceErasureSectorRestorations;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceEstimatedCostNs;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceActualCostNs;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceOverestimationRatio;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceNonperformanceMs;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceInterruptedSystemCalls;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceSubmitThreadBusyTimeNs;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceCompletionThreadBusyTimeNs;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceIoErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeviceWaitTimeMs;

    // queue subgroup
    TIntrusivePtr<::NMonitoring::TDynamicCounters> QueueGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueBytes;

    // Update cycle time
    TUpdateDurationTracker UpdateDurationTracker;

    // Device times
    THistogram DeviceReadDuration;
    THistogram DeviceWriteDuration;
    THistogram DeviceTrimDuration;
    THistogram DeviceFlushDuration;

    // <BASE_BITS, EXP_BITS, FRAME_COUNT>
    using TDurationTracker = NMonitoring::TPercentileTrackerLg<5, 4, 15>;
    // log queue duration
    TDurationTracker LogQueueTime;
    // get queue duration
    TDurationTracker GetQueueSyncLog;
    TDurationTracker GetQueueHullComp;
    TDurationTracker GetQueueHullOnlineRt;
    TDurationTracker GetQueueHullOnlineOther;
    TDurationTracker GetQueueHullLoad;
    TDurationTracker GetQueueHullLow;
    // write queue duration
    TDurationTracker WriteQueueSyncLog;
    TDurationTracker WriteQueueHullFresh;
    TDurationTracker WriteQueueHullHuge;
    TDurationTracker WriteQueueHullComp;

    // incoming flow burstiness
    TBurstmeter SensitiveBurst;
    TBurstmeter BestEffortBurst;

    // queue length seen by arriving request in front of it (QLA = Queue Length at Arrival)
    using TQLATracker = NMonitoring::TPercentileTrackerLg<5, 4, 15>;
    TQLATracker InputQLA; // for PDisk.InputQueue

    // queue cost seen by arriving request in front of it (QCA = Queue Cost at Arrival)
    using TQCATracker = NMonitoring::TPercentileTrackerLg<5, 4, 15>;
    TQCATracker InputQCA; // for PDisk.InputQueue

    // log cumulative size bytes
    // <BASE_BITS, EXP_BITS, FRAME_COUNT>
    using TSizeTracker = NMonitoring::TPercentileTrackerLg<5, 4, 15>;
    TSizeTracker LogOperationSizeBytes;
    TSizeTracker GetSyncLogSizeBytes;

    TSizeTracker GetHullCompSizeBytes;
    TSizeTracker GetHullOnlineRtSizeBytes;
    TSizeTracker GetHullOnlineOtherSizeBytes;
    TSizeTracker GetHullLoadSizeBytes;
    TSizeTracker GetHullLowSizeBytes;

    TSizeTracker WriteSyncLogSizeBytes;
    TSizeTracker WriteHullFreshSizeBytes;
    TSizeTracker WriteHullHugeSizeBytes;
    TSizeTracker WriteHullCompSizeBytes;

    // log response time
    THistogram LogResponseTime;
    // get response time
    THistogram GetResponseSyncLog;
    THistogram GetResponseHullComp;
    THistogram GetResponseHullOnlineRt;
    THistogram GetResponseHullOnlineOther;
    THistogram GetResponseHullLoad;
    THistogram GetResponseHullLow;
    // write response time
    THistogram WriteResponseSyncLog;
    THistogram WriteResponseHullFresh;
    THistogram WriteResponseHullHuge;
    THistogram WriteResponseHullComp;

    // scheduler subgroup
    TIntrusivePtr<::NMonitoring::TDynamicCounters> SchedulerGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr ForsetiCbsNotFound;

    // bandwidth subgroup
    TIntrusivePtr<::NMonitoring::TDynamicCounters> BandwidthGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPLogPayload;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPLogCommit;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPLogSectorFooter;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPLogRecordHeader;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPLogPadding;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPLogErasure;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPLogChunkPadding;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPLogChunkFooter;

    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPSysLogPayload;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPSysLogSectorFooter;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPSysLogRecordHeader;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPSysLogPadding;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPSysLogErasure;

    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPChunkPayload;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPChunkSectorFooter;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPChunkPadding;

    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPChunkReadPayload;
    ::NMonitoring::TDynamicCounters::TCounterPtr BandwidthPChunkReadSectorFooter;

    struct TIoCounters {
        ::NMonitoring::TDynamicCounters::TCounterPtr Requests;
        ::NMonitoring::TDynamicCounters::TCounterPtr Bytes;
        ::NMonitoring::TDynamicCounters::TCounterPtr Results;

        void Setup(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& group, TString name, NMonitoring::TCountableBase::EVisibility vis) {
            TIntrusivePtr<::NMonitoring::TDynamicCounters> subgroup = group->GetSubgroup("req", name);
            Requests = subgroup->GetCounter("Requests", true, vis);
            Bytes = subgroup->GetCounter("Bytes", true, vis);
            Results = subgroup->GetCounter("Results", true, vis);
        }

        void CountRequest(ui32 size) {
            Requests->Inc();
            *Bytes += size;
        }

        void CountRequest() {
            Requests->Inc();
        }

        void CountResponse() {
            Results->Inc();
        }

        void CountResponse(ui32 size) {
            Results->Inc();
            *Bytes += size;
        }

        void CountMultipleResponses(ui32 num) {
            Results->Add(num);
        }
    };

    struct TReqCounters {
        ::NMonitoring::TDynamicCounters::TCounterPtr Requests;
        ::NMonitoring::TDynamicCounters::TCounterPtr Results;

        void Setup(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& group, TString name, NMonitoring::TCountableBase::EVisibility vis) {
            TIntrusivePtr<::NMonitoring::TDynamicCounters> subgroup = group->GetSubgroup("req", name);
            Requests = subgroup->GetCounter("Requests", true, vis);
            Results = subgroup->GetCounter("Results", true, vis);
        }

        void CountRequest() {
            Requests->Inc();
        }

        void CountResponse() {
            Results->Inc();
        }
    };

    // yard subgroup
    TIntrusivePtr<::NMonitoring::TDynamicCounters> PDiskGroup;
    TReqCounters YardInit;
    TReqCounters CheckSpace;
    TReqCounters YardConfigureScheduler;
    TReqCounters ChunkReserve;
    TReqCounters ChunkForget;
    TReqCounters Harakiri;
    TReqCounters YardSlay;
    TReqCounters YardControl;

    TIoCounters WriteSyncLog;
    TIoCounters WriteFresh;
    TIoCounters WriteHuge;
    TIoCounters WriteComp;
    TIoCounters Trim;

    TIoCounters ReadSyncLog;
    TIoCounters ReadComp;
    TIoCounters ReadOnlineRt;
    TIoCounters ReadOnlineOther;
    TIoCounters ReadLoad;
    TIoCounters ReadLow;

    TIoCounters Unknown;

    TIoCounters WriteLog;
    TReqCounters WriteHugeLog;
    TIoCounters LogRead;


    // Halter
    i64 LastHaltDeviceTakeoffs = 0;
    i64 LastHaltDeviceLandings = 0;
    NHPTimer::STime LastHaltTimestamp = 0;

    // System counters - for tracking usage of CPU, memory etc.
    TIntrusivePtr<::NMonitoring::TDynamicCounters> SystemGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr PDiskThreadCPU;
    ::NMonitoring::TDynamicCounters::TCounterPtr SubmitThreadCPU;
    ::NMonitoring::TDynamicCounters::TCounterPtr GetThreadCPU;
    ::NMonitoring::TDynamicCounters::TCounterPtr TrimThreadCPU;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompletionThreadCPU;

    TPDiskMon(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui32 pdiskId, TPDiskConfig *cfg);

    ::NMonitoring::TDynamicCounters::TCounterPtr GetBusyPeriod(const TString& owner, const TString& queue);
    void IncrementQueueTime(ui8 priorityClass, size_t timeMs);
    void IncrementResponseTime(ui8 priorityClass, double timeMs, size_t sizeBytes);
    void UpdatePercentileTrackers();
    void UpdateLights();
    bool UpdateDeviceHaltCounters();
    void UpdateStats();
    TIoCounters *GetWriteCounter(ui8 priority);
    TIoCounters *GetReadCounter(ui8 priority);
};

} // NKikimr

