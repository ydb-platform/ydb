#pragma once

#include "defs.h"

#include "ddisk.h"
#include "integrity_manager.h"
#include "persistent_buffer.h"
#include "persistent_buffer_header.h"
#include "persistent_buffer_barriers_manager.h"
#include "persistent_buffer_space_allocator.h"
#include "segment_manager.h"
#include "span_utils.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

#if defined(__linux__)
#include <ydb/library/pdisk_io/uring_router.h>
#endif

#include <ydb/library/pdisk_io/uring_operation.h>
#include <ydb/library/pdisk_io/device_io_sample.h>

#include <ydb/core/util/spsc_circular_queue.h>

#include <array>
#include <atomic>
#include <optional>
#include <queue>

#include <util/generic/hash_set.h>
#include <util/system/mutex.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>

namespace NKikimrBlobStorage::NDDisk::NInternal {
    class TChunkMapLogRecord;
    class TPersistentBufferChunkMapLogRecord;
}

#define LIST_COUNTERS_INTERFACE_OPS(XX) \
    XX(Write) \
    XX(Read) \
    XX(Sync) \
    XX(WritePersistentBuffer) \
    XX(ReadPersistentBuffer) \
    XX(ErasePersistentBuffer) \
    XX(ListPersistentBuffer) \
    /**/

namespace NKikimr::NDDisk {

    namespace NPrivate {
        template<typename TRecord>
        struct THasSelectorField {
            template<typename T> static constexpr auto check(T*) -> typename std::is_same<
                std::decay_t<decltype(std::declval<T>().GetSelector())>,
                NKikimrBlobStorage::NDDisk::TBlockSelector
            >::type;

            template<typename> static constexpr std::false_type check(...);

            static constexpr bool value = decltype(check<TRecord>(nullptr))::value;
        };

        template<typename TRecord>
        struct THasWriteInstructionField {
            template<typename T> static constexpr auto check(T*) -> typename std::is_same<
                std::decay_t<decltype(std::declval<T>().GetInstruction())>,
                NKikimrBlobStorage::NDDisk::TWriteInstruction
            >::type;

            template<typename> static constexpr std::false_type check(...);

            static constexpr bool value = decltype(check<TRecord>(nullptr))::value;
        };
    }

    class TDDiskActor : public TActorBootstrapped<TDDiskActor> {
        TString DDiskId;
        TVDiskConfig::TBaseInfo BaseInfo;
        TDDiskConfig Config;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TIntrusivePtr<NMonitoring::TDynamicCounters> CountersParent;
        TIntrusivePtr<NMonitoring::TDynamicCounters> CountersBase;
        std::vector<std::pair<TString, TString>> CountersChain;
        ui64 DDiskInstanceGuid = RandomNumber<ui64>();

        static constexpr ui32 MaxInFlight = 256; // TODO: make configurable

        class TDirectIoOpBase;
        class TDDiskIoOp;
        class TPersistentBufferPartIoOp;
        class TInternalSyncWriteOp;
        class TIntegrityIoOp;
        class TChunkFormatIoOp;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // I/O operation pools
        //
        // SPSC contract: the queues have a single producer and a single consumer.
        //   Consumer (TryPop)  — always the actor thread (AllocateOp).
        //   Producer (TryPush) — the io_uring I/O thread (OnComplete/OnDrop → SelfRecycle → ReturnOp)
        //                        when UringRouter is active, or the actor thread itself on the PDisk fallback
        //                        path. These two paths are mutually exclusive: either UringRouter is set for
        //                        the whole lifetime (uring path) or it is not (PDisk fallback), so only one
        //                        thread ever pushes.
        //   FillPool (TryPush) runs once during Bootstrap before any I/O is in flight.
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        static constexpr ui32 IoOpPoolCapacity = 128;

        TSpscCircularQueue<std::unique_ptr<TDDiskIoOp>> DdiskIoOpPool;
        TSpscCircularQueue<std::unique_ptr<TPersistentBufferPartIoOp>> PersistentBufferPartIoOpPool;
        TSpscCircularQueue<std::unique_ptr<TInternalSyncWriteOp>> InternalSyncWriteOpPool;
        TSpscCircularQueue<std::unique_ptr<TIntegrityIoOp>> IntegrityIoOpPool;

        template <typename T>
        std::unique_ptr<T> AllocateOp(const IEventHandle* ev = nullptr);

        void ReturnOp(TDDiskIoOp* op);
        void ReturnOp(TPersistentBufferPartIoOp* op);
        void ReturnOp(TInternalSyncWriteOp* op);
        void ReturnOp(TIntegrityIoOp* op);

        template <typename T>
        void FillPool(TSpscCircularQueue<std::unique_ptr<T>>& pool);

        void InitUring();

        NPDisk::TDiskFormatPtr DiskFormat{nullptr, nullptr};

    private:
        struct TOpCountersBase {
            NMonitoring::TDynamicCounters::TCounterPtr Requests;
            NMonitoring::TDynamicCounters::TCounterPtr RequestsInFlight;
            NMonitoring::TDynamicCounters::TCounterPtr Bytes;
            NMonitoring::TDynamicCounters::TCounterPtr BytesInFlight;
            NMonitoring::THistogramPtr RequestSizeKiB;
            NMonitoring::THistogramPtr ResponseTime;

            void Request(ui32 bytes = 0) {
                ++*Requests;
                ++*RequestsInFlight;
                if (bytes) {
                    *Bytes += bytes;
                    *BytesInFlight += bytes;
                    RequestSizeKiB->Collect(bytes >> 10);
                }
            }

            void Done(ui32 bytes, double durationMs = 0) {
                --*RequestsInFlight;
                *BytesInFlight -= bytes;
                if (durationMs != 0) {
                    ResponseTime->Collect(durationMs);
                }
            }
        };

        struct TInterfaceOpCounters : public TOpCountersBase {
            NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
            NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;

            void Reply(bool ok, ui32 bytes = 0, double durationMs = 0) {
                ++*(ok ? ReplyOk : ReplyErr);
                Done(bytes, durationMs);
            }
        };

        struct TCounters {
            struct {
#define DECLARE_COUNTERS_INTERFACE(NAME) \
                TInterfaceOpCounters NAME;

                LIST_COUNTERS_INTERFACE_OPS(DECLARE_COUNTERS_INTERFACE)

#undef DECLARE_COUNTERS_INTERFACE
            } Interface;

            struct {
                NMonitoring::TDynamicCounters::TCounterPtr ReadLogChunks;
                NMonitoring::TDynamicCounters::TCounterPtr LogRecordsProcessed;
                NMonitoring::TDynamicCounters::TCounterPtr LogRecordsApplied;
                NMonitoring::TDynamicCounters::TCounterPtr LogRecordsWritten;
                NMonitoring::TDynamicCounters::TCounterPtr NumChunkMapSnapshots;
                NMonitoring::TDynamicCounters::TCounterPtr NumChunkMapIncrements;
                NMonitoring::TDynamicCounters::TCounterPtr CutLogMessages;
            } RecoveryLog;

            struct {
                NMonitoring::TDynamicCounters::TCounterPtr ChunksOwned;
            } Chunks;

            struct {
                TOpCountersBase Write;
                TOpCountersBase Read;

                NMonitoring::TDynamicCounters::TCounterPtr ShortReads;
                NMonitoring::TDynamicCounters::TCounterPtr ShortWrites;

                NMonitoring::TDynamicCounters::TCounterPtr RegularUringCount;
                NMonitoring::TDynamicCounters::TCounterPtr FallbackUringCount;
                NMonitoring::TDynamicCounters::TCounterPtr FallbackPDiskCount;

                NMonitoring::TDynamicCounters::TCounterPtr RunningCount;
            } DirectIO;

#if defined(__linux__)
            NPDisk::TUringCounters UringCounters;
#endif

            struct {
                NMonitoring::TDynamicCounters::TCounterPtr AllocatedChunks;
                NMonitoring::TDynamicCounters::TCounterPtr TotalBytes;
                NMonitoring::TDynamicCounters::TCounterPtr PendingEventsQueueSize;
                NMonitoring::TDynamicCounters::TCounterPtr InMemoryCacheSize;
                NMonitoring::THistogramPtr WriteBatchSize;
            } PersistentBuffer;

            struct {
                // Writes rejected because no checksum list was attached.
                NMonitoring::TDynamicCounters::TCounterPtr WritesWithoutChecksums;
                // Sender-supplied checksum mismatches detected on TEvWrite / TEvWritePersistentBuffer(s),
                // i.e. rejections with TReplyStatus::CORRUPTED. Covers both the DDisk data path and the
                // PersistentBuffer path, so it lives in its own subsystem rather than under PersistentBuffer.
                NMonitoring::TDynamicCounters::TCounterPtr ChecksumMismatch;
                NMonitoring::TDynamicCounters::TCounterPtr IntegrityPairReads;
                // Pair-slot writes only; chunk-header and extent-format writes are excluded.
                NMonitoring::TDynamicCounters::TCounterPtr IntegrityPairWrites;
                NMonitoring::TDynamicCounters::TCounterPtr IntegrityCorruption;
                NMonitoring::TDynamicCounters::TCounterPtr IntegrityLostWriteDetected;
            } Checksums;
        };

        TCounters Counters;

#if defined(__linux__)
        // we share Counters with UringRouter, so that
        // UringRouter must be after the counters to have a
        // proper destruction order
        std::unique_ptr<NPDisk::TUringRouter> UringRouter;
#endif

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // The io_uring I/O thread (via UringRouter's sample sink)
        // pushes raw TDeviceIoSample-s into DeviceOverestimationSamples under
        // DeviceOverestimationSamplesMutex. Periodically (WakeupFlushDeviceOverestimationSamples)
        // the actor thread drains the buffer and forwards a batch to the owning
        // PDisk actor (BaseInfo.PDiskActorID), which merges it with samples from
        // other sources (PDisk's own block device, other DDisk/PB slots on the
        // same PDisk) sharing the same physical device.
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        static constexpr size_t MaxBufferedDeviceOverestimationSamples = 4096;
        static constexpr TDuration DeviceOverestimationFlushPeriod = TDuration::Seconds(5);

        TMutex DeviceOverestimationSamplesMutex;
        std::vector<NPDisk::TDeviceIoSample> DeviceOverestimationSamples;

        // Flat cost-estimation constants derived once from PDiskParams (seek
        // time, read/write speed) in InitUring(). Deliberately reuses PDisk's
        // measured constants for the first iteration; IO_URING may warrant
        // its own calibrated constants later.
        ui64 DeviceOverestimationReadSpeedBps = 0;
        ui64 DeviceOverestimationWriteSpeedBps = 0;

        static constexpr ui64 NanosecondsPerSecond = 1'000'000'000ull;

        // Estimates the cost of an operation excluding any seek cost (the
        // owning PDisk actor's aggregator applies seek cost itself based on
        // the merged, cross-source stream).
        ui64 EstimateDeviceIoBaseCostNs(bool isWrite, ui64 size) const {
            const ui64 speedBps = isWrite ? DeviceOverestimationWriteSpeedBps : DeviceOverestimationReadSpeedBps;
            if (speedBps == 0) {
                return 0;
            }
            return size * NanosecondsPerSecond / speedBps;
        }

        void OnDeviceIoSample(const NPDisk::TDeviceIoSample& sample);
        void FlushDeviceOverestimationSamples();

    public:
        struct TEvPrivate {
            enum {
                EvHandleSingleQuery = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvHandleEventForChunk,
                EvHandlePersistentBufferEventForChunk,
                EvShortIO,
                EvWritePersistentBufferPart,
                EvReadPersistentBufferPart,
                EvInternalSyncWriteResult,
                EvIssuePersistentBufferChunkAllocation,
                EvDeallocatePersistentBufferChunk,
                EvDeallocatePersistentBufferChunkResult,
                EvRetryListPersistentBuffer,
                EvDDiskIoResult,
                EvIntegrityIoResult,
                EvHandleSerializedWriteForChunk,
                EvChunkFormatIoResult,
            };

           struct TEvRetryListPersistentBuffer : TEventLocal<TEvRetryListPersistentBuffer, EvRetryListPersistentBuffer> {
                TAutoPtr<TEventHandle<TEvListPersistentBuffer>> Ev;
                ui32 RetriesLeft;

                TEvRetryListPersistentBuffer(TAutoPtr<TEventHandle<TEvListPersistentBuffer>> ev, ui32 retriesLeft)
                    : Ev(ev)
                    , RetriesLeft(retriesLeft)
                {}
            };

            struct TEvIssuePersistentBufferChunkAllocation : TEventLocal<TEvIssuePersistentBufferChunkAllocation, EvIssuePersistentBufferChunkAllocation> {
            };

            struct TEvDeallocatePersistentBufferChunk : TEventLocal<TEvDeallocatePersistentBufferChunk, EvDeallocatePersistentBufferChunk> {
                ui32 ChunkIdx;

                TEvDeallocatePersistentBufferChunk(ui32 chunkIdx)
                    : ChunkIdx(chunkIdx)
                {}
            };

            struct TEvDeallocatePersistentBufferChunkResult : TEventLocal<TEvDeallocatePersistentBufferChunkResult, EvDeallocatePersistentBufferChunkResult> {
                ui32 ChunkIdx;

                TEvDeallocatePersistentBufferChunkResult(ui32 chunkIdx)
                    : ChunkIdx(chunkIdx)
                {}
            };

            struct TEvHandleEventForChunk : TEventLocal<TEvHandleEventForChunk, EvHandleEventForChunk> {
                ui64 TabletId;
                ui64 VChunkIndex;

                TEvHandleEventForChunk(ui64 tabletId, ui64 vChunkIndex)
                    : TabletId(tabletId)
                    , VChunkIndex(vChunkIndex)
                {}
            };

            struct TEvHandleSerializedWriteForChunk
                : TEventLocal<TEvHandleSerializedWriteForChunk, EvHandleSerializedWriteForChunk>
            {
                ui64 TabletId;
                ui64 VChunkIndex;

                TEvHandleSerializedWriteForChunk(ui64 tabletId, ui64 vChunkIndex)
                    : TabletId(tabletId)
                    , VChunkIndex(vChunkIndex)
                {}
            };

            struct TEvHandlePersistentBufferEventForChunk : TEventLocal<TEvHandlePersistentBufferEventForChunk, EvHandlePersistentBufferEventForChunk> {
                ui32 ChunkIndex;

                TEvHandlePersistentBufferEventForChunk(ui32 chunkIndex)
                    : ChunkIndex(chunkIndex)
                {}
            };

            struct TEvReadPersistentBufferPart : TEventLocal<TEvReadPersistentBufferPart, EvReadPersistentBufferPart> {
                ui64 InflightCookie;
                ui64 PartCookie;
                NKikimrBlobStorage::NDDisk::TReplyStatus::E Status;
                TString ErrorMessage;
                TRope Data;
                bool IsRestore = false;

                TEvReadPersistentBufferPart(ui64 inflightCookie, ui64 partCookie,
                    NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString errorMessage, TRope data, bool isRestore)
                    : InflightCookie(inflightCookie)
                    , PartCookie(partCookie)
                    , Status(status)
                    , ErrorMessage(std::move(errorMessage))
                    , Data(std::move(data))
                    , IsRestore(isRestore)
                {}
            };

            struct TEvWritePersistentBufferPart : TEventLocal<TEvWritePersistentBufferPart, EvWritePersistentBufferPart> {
                ui64 InflightCookie;
                ui64 PartCookie;
                NKikimrBlobStorage::NDDisk::TReplyStatus::E Status;
                TString ErrorMessage;
                bool IsErase = false;

                TEvWritePersistentBufferPart(ui64 inflightCookie, ui64 partCookie,
                    NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString errorMessage, bool isErase = false)
                    : InflightCookie(inflightCookie)
                    , PartCookie(partCookie)
                    , Status(status)
                    , ErrorMessage(errorMessage)
                    , IsErase(isErase)
                {}
            };

            struct TEvShortIO : TEventLocal<TEvShortIO, EvShortIO> {
                std::unique_ptr<TDirectIoOpBase> Op;

                explicit TEvShortIO(std::unique_ptr<TDirectIoOpBase> op);
                ~TEvShortIO();
            };

            // I/O callback for a client DDisk read/write. The callback only
            // packages status/data and routing metadata; the actor serializes it with
            // integrity failures, decides the final reply status, and sends the client response.
            struct TEvDDiskIoResult : TEventLocal<TEvDDiskIoResult, EvDDiskIoResult> {
                NPDisk::TUringOperationBase::EOperationType OperationType;
                NKikimrBlobStorage::NDDisk::TReplyStatus::E Status;
                TString ErrorMessage;
                TRope Data;
                TActorId OriginalRequester;
                TActorId InterconnectSession;
                ui64 Cookie = 0;
                NWilson::TSpan Span;
                ui64 TotalSize = 0;
                double RequestTimeMs = 0;
                ui64 TabletId = 0;
                ui64 VChunkIndex = 0;
                bool HasChunkKey = false;
                ui64 IntegrityOperationId = 0;
                std::vector<ui64> Checksums;

                TEvDDiskIoResult(NPDisk::TUringOperationBase::EOperationType operationType,
                        NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString errorMessage,
                        TRope data, TActorId originalRequester, TActorId interconnectSession,
                        ui64 cookie, NWilson::TSpan span, ui64 totalSize, double requestTimeMs,
                        ui64 tabletId = 0, ui64 vChunkIndex = 0, bool hasChunkKey = false,
                        ui64 integrityOperationId = 0, std::vector<ui64> checksums = {})
                    : OperationType(operationType)
                    , Status(status)
                    , ErrorMessage(std::move(errorMessage))
                    , Data(std::move(data))
                    , OriginalRequester(originalRequester)
                    , InterconnectSession(interconnectSession)
                    , Cookie(cookie)
                    , Span(std::move(span))
                    , TotalSize(totalSize)
                    , RequestTimeMs(requestTimeMs)
                    , TabletId(tabletId)
                    , VChunkIndex(vChunkIndex)
                    , HasChunkKey(hasChunkKey)
                    , IntegrityOperationId(integrityOperationId)
                    , Checksums(std::move(checksums))
                {}
            };

            // Completion of a TIntegrityManager-emitted TWriteIo executed via TIntegrityIoOp.
            struct TEvIntegrityIoResult : TEventLocal<TEvIntegrityIoResult, EvIntegrityIoResult> {
                ui64 IoId = 0;
                NKikimrBlobStorage::NDDisk::TReplyStatus::E Status = NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN;
                TString ErrorMessage;
                TRope Data;
                bool IsRead = false;

                TEvIntegrityIoResult(ui64 ioId, NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                        TString errorMessage = {}, TRope data = {}, bool isRead = false)
                    : IoId(ioId)
                    , Status(status)
                    , ErrorMessage(std::move(errorMessage))
                    , Data(std::move(data))
                    , IsRead(isRead)
                {}
            };

            struct TEvChunkFormatIoResult : TEventLocal<TEvChunkFormatIoResult, EvChunkFormatIoResult> {
                TChunkIdx ChunkIdx = 0;
                ui32 OffsetInBytes = 0;
                ui32 Size = 0;
                NKikimrBlobStorage::NDDisk::TReplyStatus::E Status =
                    NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN;
                TString ErrorMessage;

                TEvChunkFormatIoResult(TChunkIdx chunkIdx, ui32 offsetInBytes, ui32 size,
                        NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString errorMessage = {})
                    : ChunkIdx(chunkIdx)
                    , OffsetInBytes(offsetInBytes)
                    , Size(size)
                    , Status(status)
                    , ErrorMessage(std::move(errorMessage))
                {}
            };

            struct TEvInternalSyncWriteResult : TEventLocal<TEvInternalSyncWriteResult, EvInternalSyncWriteResult> {
                ui64 SyncId = 0;
                ui64 RequestId = 0;
                ui64 SegmentBegin = 0;
                ui64 SegmentEnd = 0;
                ui64 IntegrityOperationId = 0;
                NKikimrBlobStorage::NDDisk::TReplyStatus::E Status = NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN;
                TString ErrorMessage;

                TEvInternalSyncWriteResult(ui64 syncId, ui64 requestId, ui64 segmentBegin, ui64 segmentEnd,
                    ui64 integrityOperationId, NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                    TString errorMessage = {})
                    : SyncId(syncId)
                    , RequestId(requestId)
                    , SegmentBegin(segmentBegin)
                    , SegmentEnd(segmentEnd)
                    , IntegrityOperationId(integrityOperationId)
                    , Status(status)
                    , ErrorMessage(std::move(errorMessage))
                {}
            };
        };

    private:
        enum EWakeupTag {
            WakeupUpdateFreeSpaceInfo = 2,
            WakeupCollectPbStats = 3,
            WakeupProcessPersistentBufferBatchWrite = 4,
            WakeupProcessDeallocatePersistentBufferChunk = 5,
            WakeupFlushDeviceOverestimationSamples = 6,
        };

        struct TPbOpSnapshot {
            TInstant Timestamp;
            ui64 Requests = 0;
            std::vector<ui64> BucketCounts;
        };

        // Sliding window of cumulative snapshots for each PB operation,
        // used to compute IOPS and latency percentiles over the last ~15 seconds.
        std::unordered_map<TString, std::deque<TPbOpSnapshot>> PbStatsHistory;
        static constexpr TDuration PbStatsWindow = TDuration::Seconds(15);
        static constexpr TDuration PbStatsSnapshotPeriod = TDuration::Seconds(1);

        void CollectPbStatsSnapshot();

        const bool IsPersistentBufferActor = false;

        // Actor-thread-only health state. I/O callbacks communicate status/data exclusively
        // through TEvPrivate callbacks, so Broken ordering is defined by the actor mailbox.
        bool Broken = false;
        TString BrokenReason;

        bool IsBroken() const;
        bool ChecksumsEnabled() const {
            return Config.EnableChecksums;
        }
        TString GetBrokenReason() const;
        void EnterBroken(TString reason);
        void FailPendingDDiskQuery(std::unique_ptr<IEventHandle> ev);
        void FailDirectIoOp(std::unique_ptr<TDirectIoOpBase> op);

    public:
        TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters, bool isPersistentBufferActor = false);

        TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const std::vector<ui32>& initPersistentBufferChunks,
            ui64 persistentBufferUniqueId, TIntrusivePtr<TPDiskParams> pDiskParams, NPDisk::TDiskFormatPtr diskFormat,
            TFileHandle&& diskFd);

        ~TDDiskActor();
        void Bootstrap();
        STFUNC(StateFuncDDisk);
        STFUNC(StateFuncPersistentBuffer);
        STFUNC(StateFuncTerminate);
        void PassAway() override;

        // Mirrors TVDiskContext::CheckPDiskResponse: returns true on OK, returns false and
        // switches to StateFuncTerminate on session-loss statuses (ERROR / INVALID_OWNER /
        // INVALID_ROUND) and device-error statuses (CORRUPTED / OUT_OF_SPACE), Y_ABORTs on
        // anything else. Caller must `return` immediately on false because the actor's
        // state has changed.
        bool CheckPDiskReply(NKikimrProto::EReplyStatus status,
            const TString& errorReason, TStringBuf source);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Boot sequence and PDisk management
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TPendingEvent {
            std::unique_ptr<IEventHandle> Ev;
            NWilson::TSpan QueueSpan;

            template<typename TEvent>
            TPendingEvent(TAutoPtr<TEventHandle<TEvent>> ev, const char *name)
                : Ev(ev.Release())
                , QueueSpan(TWilson::DDiskTopLevel, NWilson::TTraceId(Ev->TraceId), name, NWilson::EFlags::AUTO_END,
                    TActivationContext::ActorSystem())
            {
                NPrivate::AddMessageWaitAttributes(QueueSpan);
            }

            TAutoPtr<IEventHandle> Release() {
                return Ev.release();
            }
        };

        struct TChunkRef {
            TChunkIdx ChunkIdx = 0;
            ui32 InFlightDataIo = 0;
            std::queue<TPendingEvent> PendingEventsForChunk;
            bool IntegrityExtentWriteInFlight = false;
            bool SerializedWriteResumeScheduled = false;
            std::queue<TPendingEvent> PendingSerializedWrites;
        };

        THashMap<ui64, THashMap<ui64, TChunkRef>> ChunkRefs; // TabletId -> (VChunkIndex -> ChunkIdx)
        TIntrusivePtr<TPDiskParams> PDiskParams;
        TFileHandle DiskFd;
        std::vector<TChunkIdx> OwnedChunksOnBoot;
        ui64 ChunkMapSnapshotLsn = Max<ui64>();
        std::queue<TPendingEvent> PendingQueries;
        bool HandlingQueries = false;
        bool LogReplayComplete = false;
        std::optional<ui64> DeferredCutLogFreeUpToLsn;
        ui64 NextLsn = 1;
        std::set<std::tuple<ui64, ui64, ui32>> ChunkMapIncrementsInFlight;

        void InitPDiskInterface();
        void Handle(NPDisk::TEvYardInitResult::TPtr ev);
        void Handle(NPDisk::TEvReadLogResult::TPtr ev);
        void ValidateChecksumsModeAfterLogReplay();
        void StartHandlingQueries();
        void HandleSingleQuery();

        template<typename TEvent>
        bool CanHandleQuery(TAutoPtr<TEventHandle<TEvent>>& ev) {
            if (HandlingQueries) {
                return true;
            }
            PendingQueries.emplace(ev, "WaitPDiskInit");
            return false;
        }

        // Chunk management code

        // DDisk may pull an integrity chunk from the same reserve as a data
        // chunk, so it keeps a larger reserve than PersistentBuffer.
        static constexpr ui32 MinChunksReservedDDisk = 4;
        static constexpr ui32 MinChunksReservedPersistentBuffer = 2;
        const ui32 MinChunksReserved;
        std::queue<TChunkIdx> ChunkReserve;
        // Newly reserved chunks are zeroed in slices before they become allocatable in
        // checksums-disabled mode. Value is the next byte offset to format.
        absl::flat_hash_map<TChunkIdx, ui32> FormattingChunks;
        bool ReserveInFlight = false;

        struct TChunkForData {
            ui64 TabletId;
            ui64 VChunkIndex;
        };

        struct TChunkForPersistentBuffer {};

        struct TChunkForIntegrity {};

        std::queue<std::variant<TChunkForData, TChunkForPersistentBuffer,
            TChunkForIntegrity>> ChunkAllocateQueue;
        struct TLogCallback {
            std::function<void()> Callback;
            bool IsDDisk = false;
        };
        absl::flat_hash_map<ui64, TLogCallback> LogCallbacks;
        ui64 NextCookie = 1;

        struct TPendingIoOp {
            std::unique_ptr<TDirectIoOpBase> Op;

            TPendingIoOp() = default;
            explicit TPendingIoOp(std::unique_ptr<TDirectIoOpBase> op);
            TPendingIoOp(TPendingIoOp&&) noexcept;

            TPendingIoOp(const TPendingIoOp&) = delete;

            TPendingIoOp& operator=(TPendingIoOp&&) noexcept;
            TPendingIoOp& operator=(const TPendingIoOp&) = delete;

            ~TPendingIoOp();
        };

        THashMap<ui64, TPendingIoOp> WriteCallbacks;
        THashMap<ui64, TPendingIoOp> ReadCallbacks;

        void IssueChunkAllocation(ui64 tabletId, ui64 vChunkIndex);
        void Handle(NPDisk::TEvChunkReserveResult::TPtr ev);
        void HandleChunkReserved();
        size_t CountPendingPersistentBufferChunkAllocations() const;
        void IssueNextChunkFormatWrite(TChunkIdx chunkIdx);
        void Handle(TEvPrivate::TEvChunkFormatIoResult::TPtr ev);
        void Handle(NPDisk::TEvLogResult::TPtr ev);
        void Handle(TEvPrivate::TEvHandleEventForChunk::TPtr ev);
        void Handle(TEvPrivate::TEvHandlePersistentBufferEventForChunk::TPtr ev);

        void Handle(NPDisk::TEvCutLog::TPtr ev);
        void ProcessCutLog(ui64 freeUpToLsn);
        void Handle(TEvDeleteTabletChunks::TPtr ev);
        // Tablets whose removal snapshot is not committed yet. Their integrity extents are
        // quarantined in TIntegrityManager and data operations must not start a new incarnation
        // with the same (TabletId, VChunkIndex) keys until the deletion becomes durable.
        absl::flat_hash_set<ui64> TabletChunkDeletionsInFlight;
        struct TTabletChunkDeletionReply {
            TActorId ReplyTo;
            ui64 Cookie = 0;
            TActorId InterconnectSession;
        };
        THashMap<ui64, TTabletChunkDeletionReply> TabletChunkDeletionReplies;

        void Handle(NPDisk::TEvChunkWriteRawResult::TPtr ev);
        void Handle(NPDisk::TEvChunkReadRawResult::TPtr ev);

        ui64 GetFirstLsnToKeep() const;

        void IssuePDiskLogRecord(TLogSignature signature, TChunkIdx chunkIdxToCommit, const NProtoBuf::Message& data,
            ui64 *startingPointLsnPtr, std::function<void()> callback,
            TVector<TChunkIdx> chunksToDelete = {});
        void IssuePDiskLogRecord(TLogSignature signature, TVector<TChunkIdx> chunksToCommit,
            const NProtoBuf::Message& data, ui64 *startingPointLsnPtr, std::function<void()> callback,
            TVector<TChunkIdx> chunksToDelete = {});

        NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord CreatePersistentBufferChunkMapSnapshot();
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord CreateChunkMapSnapshot();
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord CreateChunkMapIncrement(ui64 tabletId, ui64 vChunkIndex,
            TChunkIdx chunkIdx, const TIntegrityManager::TExtentRef* extentRef,
            const TIntegrityManager::TMappingSnapshot::TIntegrityChunkEntry* integrityChunk = nullptr);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Integrity management (DDisk mode only)
        //
        // TIntegrityManager is the pure-logic owner of integrity chunks / extents; the actor executes
        // its queued actions (chunk allocations, formatting writes) asynchronously. A reserved
        // chunk is formatted immediately. Data writes start once the extent is placed (IntegrityChunk
        // found). The combined chunk-map increment is logged only after the extent is Ready, and
        // the originating write/sync is not answered until that record is durable.
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Constructed in Handle(TEvYardInitResult) once DiskFormat (chunk size) is known.
        std::optional<TIntegrityManager> IntegrityManager;

        // Integrity chunks that have appeared in a durable (or in-flight) log record, with the
        // generation stamped into that record. Appended when the increment is issued, so a
        // concurrently written snapshot already includes the chunk - by the time that snapshot is
        // read back the commit has landed.
        std::vector<TIntegrityManager::TMappingSnapshot::TIntegrityChunkEntry> CommittedIntegrityChunks;

        // DataChunk -> IntegrityExtent mapping accumulated from the chunk-map snapshot and log
        // increments during boot; fed to IntegrityManager->ApplyMappingSnapshot at end-of-log.
        TIntegrityManager::TMappingSnapshot RestoredIntegrityMapping;

        struct TParkedWriteReply {
            NKikimrBlobStorage::NDDisk::TReplyStatus::E Status = NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN;
            TString ErrorMessage;
            TActorId OriginalRequester;
            TActorId InterconnectSession;
            ui64 Cookie = 0;
            NWilson::TSpan Span;
            ui64 TotalSize = 0;
            double RequestTimeMs = 0;
            ui64 TabletId = 0;
            ui64 VChunkIndex = 0;
        };

        struct TPendingClientWrite {
            std::optional<TParkedWriteReply> DataResult;
            bool IntegrityCompleted = false;
            TIntegrityManager::EOperationStatus IntegrityStatus = TIntegrityManager::EOperationStatus::Ok;
            TString IntegrityError;
        };

        absl::flat_hash_map<ui64, TPendingClientWrite> PendingClientWrites; // integrity operation id

        struct TPendingChecksumRead {
            std::unique_ptr<IEventHandle> Event;
        };

        absl::flat_hash_map<ui64, TPendingChecksumRead> PendingChecksumReads; // integrity operation id

        struct TDataChunkAllocationInFlight {
            TChunkIdx ChunkIdx = 0;
            bool LogIssued = false;
            ui32 NewlyCommittedChunks = 0;
            std::vector<TParkedWriteReply> ParkedWriteResults;
            std::vector<ui64> ParkedSyncIds;
        };

        absl::flat_hash_map<std::pair<ui64, ui64>, TDataChunkAllocationInFlight>
            DataChunkAllocationsInFlight; // (tabletId, vChunkIndex)

        // Drains IntegrityManager->TakeActions(): submits integrity reads/writes via TIntegrityIoOp
        // and queues TChunkForIntegrity entries. Returns true when a chunk allocation was queued;
        // callers not already inside HandleChunkReserved() must then call it.
        bool ProcessIntegrityActions();
        void ProcessIntegrityCompletions();
        void MaybeFinishClientWrite(ui64 operationId);
        void FinishClientWrite(TParkedWriteReply result);
        void StartDDiskDataRead(std::unique_ptr<IEventHandle> ev, std::vector<ui64> checksums);
        void OpenDataChunkWritePath(std::vector<TIntegrityManager::TDataChunkKey> placedKeys);
        void DrainIntegrityManager(bool kickReserve = true);
        void ReleaseIntegrityExtentWrite(ui64 tabletId, ui64 vChunkIndex);
        void ScheduleSerializedWrite(ui64 tabletId, ui64 vChunkIndex);
        void Handle(TEvPrivate::TEvHandleSerializedWriteForChunk::TPtr ev);
        // Assigns newly free slots to pending extents, submits the resulting actions, and
        // releases integrity chunks that remain completely unused. Never-logged chunks return to
        // the reserve; committed ones are dropped via a snapshot. completion runs after the
        // optional release snapshot commits.
        void ReclaimUnusedIntegrityChunks(std::function<void()> completion = {});
        void IssueDataChunkIncrement(ui64 tabletId, ui64 vChunkIndex);
        void CompleteDataChunkAllocation(ui64 tabletId, ui64 vChunkIndex);
        void FlushParkedAllocationReplies(TDataChunkAllocationInFlight& allocation);
        bool IsIntegrityChunkCommitted(TChunkIdx chunkIdx) const;
        void Handle(TEvPrivate::TEvIntegrityIoResult::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Connection management
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        enum class EConnectionTokenInvalidationReason : ui8 {
            Reconnect,
            Disconnect,
        };

        struct TPreviousConnectionTokenInfo {
            TConnectionToken Token;
            ui64 TabletId = 0;
            ui32 Generation = 0;
            ui32 DirectBlockGroupIndex = 0;
            ui64 DDiskSessionSeqNo = 0;
            EConnectionTokenInvalidationReason InvalidationReason = EConnectionTokenInvalidationReason::Reconnect;
            bool Valid = false;
        };

        struct TConnectionInfo {
            ui64 TabletId = 0;
            ui32 Generation = 0;
            ui32 DirectBlockGroupIndex = 0;
            ui64 DDiskSessionSeqNo = 0;
            ui32 NodeId = 0;
            TActorId InterconnectSessionId;
            TConnectionToken Token;
            ui8 TokenSequenceNo = 0;
            std::array<TPreviousConnectionTokenInfo, 2> PreviousTokens;
            ui32 NextPreviousTokenIndex = 0;
            bool Active = false;
        };

        using TConnectionKey = std::pair<ui64, ui32>;
        TVector<TConnectionInfo> Connections;
        THashMap<TConnectionKey, ui32> ConnectionIndexBySession;
        TVector<ui32> FreeConnectionIndices;

        void Handle(TEvConnect::TPtr ev);
        void Handle(TEvDisconnect::TPtr ev);

        TConnectionToken IssueConnectionToken(ui32 connectionIndex, TConnectionInfo& connection);

        void RememberConnectionToken(TConnectionInfo& connection, EConnectionTokenInvalidationReason reason);

        enum class EConnectionResolution : ui8 {
            Resolved,
            StaleToken,
            InvalidToken,
        };

        // validate query credentials and restore token-backed connection data
        EConnectionResolution ResolveConnection(const TQueryCredentials& requestCreds, TQueryCredentials* resolvedCreds) const;
        static TStringBuf ConnectionErrorReason(EConnectionResolution resolution);
        static TStringBuf ConnectionInvalidationReason(EConnectionTokenInvalidationReason reason);
        TString DescribeConnectionFailure(const TQueryCredentials& requestCreds, EConnectionResolution resolution) const;

        // a general way to send reply to any incoming message
        void SendReply(const IEventHandle& queryEv, std::unique_ptr<IEventBase> replyEv) const;

        // common function to validate any incoming event's credentials
        template<typename TEvent, typename TCountersPtr>
        bool CheckQuery(TEventHandle<TEvent>& ev, TCountersPtr counters) const {
            auto& record = ev.Get()->Record;
            using TEventType = std::decay_t<TEvent>;

            auto registerError = [&] {
                if constexpr (!std::is_same_v<TCountersPtr, std::nullptr_t>) {
                    counters->Request(0);
                    counters->Reply(false);
                }
            };

            if (IsBroken()) {
                SendReply(ev, std::make_unique<typename TEvent::TResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, GetBrokenReason()));
                registerError();
                return false;
            }

            auto logError = [&](TStringBuf reason) {
                YDB_LOG_DEBUG_CTX_COMP(*TActivationContext::ActorSystem(), NKikimrServices::BS_DDISK, "TDDiskActor::CheckQuery validation failed",
                    {"reason", reason},
                    {"DDiskId", DDiskId},
                    {"evType", ev.GetTypeRewrite()},
                    {"sender", ev.Sender},
                    {"cookie", ev.Cookie},
                    {"ICSession", ev.InterconnectSession});
            };

            const TQueryCredentials requestCreds(record.GetCredentials());
            TQueryCredentials creds;
            const EConnectionResolution resolution = ResolveConnection(requestCreds, &creds);

            if (resolution != EConnectionResolution::Resolved) {
                logError(DescribeConnectionFailure(requestCreds, resolution));
                auto result = std::make_unique<typename TEvent::TResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::SESSION_MISMATCH
                );
                const TStringBuf errorReason = ConnectionErrorReason(resolution);
                result->Record.SetErrorReason(errorReason.data(), errorReason.size());

                SendReply(ev, std::move(result));
                registerError();
                return false;
            }

            creds.SerializeResolvedForRequest(record.MutableCredentials());

            using TRecord = std::decay_t<decltype(record)>;

            if constexpr (NPrivate::THasSelectorField<TRecord>::value) {
                const TBlockSelector selector(record.GetSelector());

                if (selector.OffsetInBytes % DiskFormat->SectorSize || selector.Size % DiskFormat->SectorSize || !selector.Size) {
                    TStringStream ss;
                    ss << "offset and size must be multiple of sector size and size must be nonzero: ";
                    selector.Print(ss);
                    logError(ss.Str());
                    SendReply(ev, std::make_unique<typename TEvent::TResult>(
                        NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                        ss.Str()));
                    registerError();
                    return false;
                }

                if constexpr (std::is_same_v<TEventType, TEvRead> || std::is_same_v<TEventType, TEvWrite>) {
                    if (selector.OffsetInBytes > DiskFormat->ChunkSize ||
                            selector.Size > DiskFormat->ChunkSize - selector.OffsetInBytes) {
                        TStringStream ss;
                        ss << "request should be within a chunk (chunk size: " << DiskFormat->ChunkSize << "): ";
                        selector.Print(ss);
                        logError(ss.Str());
                        SendReply(ev, std::make_unique<typename TEvent::TResult>(
                            NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                            ss.Str()));
                        registerError();
                        return false;
                    }
                }

                if constexpr (NPrivate::THasWriteInstructionField<TRecord>::value) {
                    const TWriteInstruction instruction(record.GetInstruction());
                    size_t size = 0;
                    if (instruction.PayloadId) {
                        const TRope& data = ev.Get()->GetPayload(*instruction.PayloadId);
                        size = data.size();
                    }
                    // this check is crucial for the code submitting IO
                    if (size != selector.Size) {
                        TStringStream ss;
                        ss << "declared data size must match actually sent one: size="
                            << size << ", selector.Size=" << selector.Size << ", ";
                        selector.Print(ss);
                        logError(ss.Str());
                        SendReply(ev, std::make_unique<typename TEvent::TResult>(
                            NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                            ss.Str()));
                        registerError();
                        return false;
                    }
                }
            }

            return true;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Read/write
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // PDisk read/write fallback
        void SendPDiskWrite(std::unique_ptr<TDirectIoOpBase> op);
        void SendPDiskRead(std::unique_ptr<TDirectIoOpBase> op);

        void Handle(TEvWrite::TPtr ev);
        void Handle(TEvRead::TPtr ev);
        void Handle(TEvPrivate::TEvDDiskIoResult::TPtr ev);

        // Regular direct I/O.
        // Note: releases the op when it is submitted to io_uring or moved to the PDisk fallback.
        void DirectUringOp(std::unique_ptr<TDirectIoOpBase>& op, bool isShort = false);

        // Do not call manually!
        void DirectUringOpImpl(std::unique_ptr<TDirectIoOpBase>& op);

        void HandleShortIO(TEvPrivate::TEvShortIO::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Sync
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TSyncReadRequest {
            NKikimrBlobStorage::NDDisk::TReplyStatus::E Status;
            TBlockSelector Selector;
            ui64 SegmentsInFlight = 0;
            TStringBuilder ErrorReason = {};
        };

        struct TSyncInFlight {
            TActorId Sender;
            ui64 Cookie;
            TActorId InterconnectionSessionId;
            NWilson::TSpan Span;
            TQueryCredentials Creds;
            std::vector<TSyncReadRequest> Requests;
            ui64 RequestsInFlight = 0;
            ui64 VChunkIndex = 0;
            ui64 FirstRequestId = Max<ui64>();
            TStringBuilder ErrorReason;
        };

        using TSyncIt = THashMap<ui64, TSyncInFlight>::iterator;

        ui64 NextSyncId = 1;
        THashMap<ui64, TSyncInFlight> SyncsInFlight; // syncId -> TSyncInFlight
        THashSet<ui64> SyncReadCookiesInFlight;
        TSegmentManager SegmentManager;

        struct TPendingSyncSegment {
            ui64 SyncId = 0;
            ui64 RequestId = 0;
            ui64 Begin = 0;
            ui64 End = 0;
            bool DataCompleted = false;
            bool IntegrityCompleted = false;
            NKikimrBlobStorage::NDDisk::TReplyStatus::E DataStatus =
                NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN;
            TString DataError;
            TIntegrityManager::EOperationStatus IntegrityStatus = TIntegrityManager::EOperationStatus::Ok;
            TString IntegrityError;
        };
        absl::flat_hash_map<ui64, TPendingSyncSegment> PendingSyncSegments; // integrity operation id

        void Handle(TEvSync::TPtr ev);
        void Handle(TEvReadResult::TPtr ev);
        void Handle(TEvReadPersistentBufferResult::TPtr ev);
        void Handle(TEvPrivate::TEvInternalSyncWriteResult::TPtr ev);

        template <typename TEventPtr>
        void InternalSyncReadResult(TEventPtr ev);

        std::unique_ptr<IEventHandle> MakeSyncResult(const TSyncInFlight& sync);

        void ReplySync(TSyncIt it);
        void MaybeReplySync(TSyncIt it);
        void MaybeFinishSyncSegment(ui64 integrityOperationId);
        void FinishSyncSegment(TPendingSyncSegment segment);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Persistent buffer services
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::map<TPersistentBufferId, TPersistentBuffer> PersistentBuffers;
        std::map<TInstant, absl::flat_hash_set<TPersistentBufferRecordId>> PersistentBuffersInMemoryCacheUptime;
        ui64 PersistentBufferInMemoryCacheSize = 0;
        TInstant StartedAt;

        ui64 CalcPersistentBufferInMemoryCacheSize();
        TString PersistentBufferToString();

        void SanitizePersistentBufferInMemoryCache();
        void SanitizePersistentBufferInMemoryCache(ui64 tabletId, ui32 generation, ui64 lsn, TPersistentBuffer::TRecord& record, ui8 directBlockGroupIndex = 0);


        ui32 SectorSize;
        ui32 SectorInChunk;
        ui32 ChunkSize;
        TPersistentBufferFormat PersistentBufferFormat;

        double NormalizedOccupancy = -1;

        bool IssuePersistentBufferChunkAllocationInflight = false;

        struct TEraseLsnId {
            ui32 Generation;
            ui64 Lsn;
        };

        struct TPersistentBufferDiskOperationInFlight {
            struct TRecord {
                TActorId Sender;
                ui64 Cookie;
                TActorId Session;
                NWilson::TSpan Span;

                ui64 TabletId;
                ui32 Generation;
                ui64 VChunkIndex;
                ui64 Lsn;
                ui32 OffsetInBytes;
                ui32 Size;

                std::map<ui64, TRope> DataParts;
                ui32 PartsCount;
                std::vector<TPersistentBufferSectorInfo> Sectors;
                // Sender-supplied per-MinSectorSize-block payload checksums for this record, in order.
                // Empty when the write carried no checksums. See TPersistentBuffer::TRecord::PayloadChecksums.
                std::vector<ui64> PayloadChecksums;
                // Direct block group number this record belongs to. See TPersistentBufferId for
                // rationale; defaults to 0 to preserve the pre-existing single-namespace-per-tablet
                // behavior. Declared last so it never conflicts with designated-initializer ordering
                // at existing call sites that only name fields up to PayloadChecksums.
                ui8 DirectBlockGroupIndex = 0;
                bool ChecksumsDisabled = false;
                ui64 HeaderUniqueId = 0;
                TRope JoinData(ui32 sectorSize);
            };

            std::vector<TRecord> Records;

            absl::flat_hash_set<ui64> OperationCookies;
            // map operationCookie to <lsn, generation> pairs that were erased by this operation
            std::unordered_map<ui64, std::vector<TEraseLsnId>> Erases;
            TRope DataToWrite;

            std::vector<TPersistentBufferSectorInfo> OccupiedSectors;
            NKikimrBlobStorage::NDDisk::TReplyStatus::E Status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            std::optional<TString> ErrorMessage = std::nullopt;

            NHPTimer::STime StartTs{};
        };

        struct TPersistentBufferEraseInflight {
            ui64 EraseCookie;
            std::vector<ui64> OperationsCookie;
        };

        ui64 PersistentBufferBatchWriteCookie = 0;
        ui64 NextPersistentBufferHeaderUniqueId = 0;
        absl::flat_hash_map<TPersistentBufferLocation, absl::flat_hash_set<TPersistentBufferRecordId>> PersistentBufferHeaders;
        absl::flat_hash_map<ui64, TPersistentBufferDiskOperationInFlight> PersistentBufferDiskOperationInflight;

        // map record to operation cookie + record in inflight position
        absl::flat_hash_map<TPersistentBufferRecordId, std::vector<std::tuple<ui64, ui32>>> PersistentBufferWriteInflightsByRecord;
        absl::flat_hash_map<TPersistentBufferRecordId, TPersistentBufferEraseInflight> PersistentBufferEraseInflightsByRecord;

        ui32 PersistentBufferRestoreChunksInflight = 0;
        std::vector<ui32> PersistentBufferChunks;
        ui64 PersistentBufferUniqueId = 0;

        TPersistentBufferSpaceAllocator PersistentBufferSpaceAllocator;
        TPersistentBufferBarriersManager PersistentBufferBarriersManager;

        ui64 PersistentBufferChunkMapSnapshotLsn = Max<ui64>();
        std::queue<TPendingEvent> PendingPersistentBufferEvents;
        bool PersistentBufferReady = false;

        struct TPersistentBufferDataSectorInfo {
            ui64 Checksum;
            ui64 HeaderUniqueId;
        };
        // During restoration every data sector is inspected once for both
        // on-disk formats; the record header flag selects the value to validate.
        absl::flat_hash_map<ui64, std::vector<TPersistentBufferDataSectorInfo>> PersistentBufferDataSectorsInfo;
        absl::flat_hash_set<ui32> PersistentBufferAllocatedChunks;
        absl::flat_hash_set<ui32> PersistentBufferRestoringChunks;

        TActorId WritePersistentBuffersActor;
        TActorId PersistentBufferActorId;

        ui64 CalculateChecksum(const TRope::TIterator begin) {
            return CalculateChecksum(begin, SectorSize);
        }

        ui64 CalculateChecksum(const TRope::TIterator begin, size_t numBytes);

        void CreatePersistentBuffer();
        void InitPersistentBuffer();
        void IssuePersistentBufferChunkAllocation();
        void ProcessDeallocatePersistentBufferChunk(bool forceToNextChunk = false);
        void ProcessPersistentBufferQueue();
        std::vector<std::tuple<ui32, ui32, TRope>> SlicePersistentBuffer(ui64 tabletId, ui32 generation, ui64 vchunkIndex, ui64 lsn, ui32 offsetInBytes, ui32 size, TRcBuf&& payloadWithHeader, std::vector<TPersistentBufferSectorInfo>& sectors, const std::vector<ui64>& payloadChecksums, ui8 directBlockGroupIndex = 0, ui64 headerUniqueId = 0);
        std::vector<std::tuple<ui32, ui32, TRope>> SlicePersistentBufferData(TRope& data, std::vector<TPersistentBufferSectorInfo>& sectors);
        void StartRestorePersistentBuffer();
        void RestorePersistentBufferChunk(TEvPrivate::TEvReadPersistentBufferPart::TPtr ev);
        void ReplyReadPersistentBuffer(ui64 operationCookie);
        void ReplyReadPersistentBuffer(TPersistentBuffer::TRecord& pr, NKikimrBlobStorage::NDDisk::TReplyStatus::E status, std::optional<TString> errorMessage);

        bool PreprocessPersistentBufferWrite(NActors::TEventHandle<TEvWritePersistentBuffer>& ev);
        void ProcessPersistentBufferWrite(TEvWritePersistentBuffer::TPtr ev);
        // ev is taken by reference (not TPtr by value, unlike its sibling above): TPtr is a TAutoPtr
        // with ownership-transferring copy semantics, so a by-value parameter here would null out the
        // caller's ev as soon as this is invoked -- including on the "doesn't fit, fall back" (false)
        // return path, where Handle(TEvWritePersistentBuffer) still needs a valid ev afterwards to retry
        // via ProcessPersistentBufferWrite.
        bool ProcessPersistentBufferBatchWriteData(TEvWritePersistentBuffer::TPtr& ev);
        void ProcessPersistentBufferBatchWrite();
        double GetPersistentBufferFreeSpace();
        void ErasePersistentBuffer(IEventHandle& queryEv, const TQueryCredentials& creds, const std::vector<TEraseLsnId>& erases);
        void BarrierErasePersistentBuffer(IEventHandle& queryEv, const TQueryCredentials& creds, const std::vector<TEraseLsnId>& erases, ui64 lsn);
        void FastErasePersistentBuffer(IEventHandle& queryEv, const TQueryCredentials& creds, const std::vector<TEraseLsnId>& erases, const TFastErase& fastErase);
        void ClearPersistentBufferRecords(TPersistentBufferDiskOperationInFlight& inflight, ui64 partCookie);
        void HandleWritePart(TPersistentBufferDiskOperationInFlight& inflight,  ui64 opCookie, ui64 partCookie);
        void HandleErasePart(TPersistentBufferDiskOperationInFlight& inflight, ui64 opCookie, ui64 partCookie, bool resultStatus);

        void Handle(TEvWritePersistentBuffer::TPtr ev);
        void Handle(TEvReadPersistentBuffer::TPtr ev);
        void Handle(TEvErasePersistentBuffer::TPtr ev);
        void Handle(TEvBatchErasePersistentBuffer::TPtr ev);
        void Handle(TEvWriteResult::TPtr ev);
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void Handle(TEvListPersistentBuffer::TPtr ev);
        void Handle(TEvPrivate::TEvRetryListPersistentBuffer::TPtr ev);
        // Returns true if the given tablet currently has at least one persistent-buffer disk
        // operation (write/erase/read) in flight. TEvListPersistentBuffer must not be answered
        // while this holds, otherwise it could observe a partially-applied write or erase.
        bool HasPersistentBufferInflightForTablet(ui64 tabletId) const;
        void ProcessListPersistentBuffer(TAutoPtr<TEventHandle<TEvListPersistentBuffer>> ev, ui32 retriesLeft);
        void ReplyListPersistentBuffer(TEventHandle<TEvListPersistentBuffer>& ev);
        void Handle(TEvPrivate::TEvIssuePersistentBufferChunkAllocation::TPtr ev);
        void Handle(TEvPrivate::TEvDeallocatePersistentBufferChunk::TPtr ev);
        void Handle(TEvPrivate::TEvDeallocatePersistentBufferChunkResult::TPtr ev);
        void Handle(TEvGetPersistentBufferInfo::TPtr ev);

        template<typename TEventPtr>
        void HandlePersistentBufferWriteRequest(TEventPtr& ev);

        void Handle(TEvReadThenWritePersistentBuffers::TPtr ev);
        void Handle(TEvWritePersistentBuffers::TPtr ev);

        void Handle(TEvPrivate::TEvReadPersistentBufferPart::TPtr ev);
        void Handle(TEvPrivate::TEvWritePersistentBufferPart::TPtr ev);

        void HandleWakeup(TEvents::TEvWakeup::TPtr &ev);
        void Handle(NPDisk::TEvCheckSpaceResult::TPtr ev);
        void UpdateFreeSpaceInfo();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Monitoring page (DDisk mode only)
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void RegisterMonPage();
        void Handle(NMon::TEvHttpInfo::TPtr ev);
    };

} // NKikimr::NDDisk
