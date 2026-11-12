#pragma once

#include "defs.h"

#include "ddisk.h"
#include "tablet_stats_actor.h"
#include "space_metrics.h"
#include "chunk_manager.h"
#include "integrity_manager.h"
#include "persistent_buffer.h"
#include "persistent_buffer_header.h"
#include "persistent_buffer_barriers_manager.h"
#include "persistent_buffer_space_allocator.h"
#include "span_utils.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/subsystems/metric_system.h>
#include <ydb/library/actors/async/event.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

#if defined(__linux__)
#include <ydb/library/pdisk_io/uring_router_client.h>
#endif

#include <ydb/library/pdisk_io/uring_operation.h>

#include <ydb/core/util/spsc_circular_queue.h>

#include <array>
#include <atomic>
#include <coroutine>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <variant>
#include <vector>

#include <util/generic/hash_set.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>
#include <contrib/restricted/abseil-cpp/absl/container/inlined_vector.h>

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
    XX(GetPersistentBufferRegistrationToken) \
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

        class TDirectIoOpBase;
        class TDDiskIoOp;
        class TPersistentBufferPartIoOp;

    public:
        class TBatchedIOAwaiter;

    private:
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

        // Actor thread only. A pooled awaiter is held by exactly one shared_ptr.
        // Return is opportunistic: if a device operation or queued resume still owns
        // the object, it is left untouched. Both ends run on this thread.
        std::vector<std::shared_ptr<TBatchedIOAwaiter>> BatchedIOAwaiterPool;

        std::shared_ptr<TBatchedIOAwaiter> AllocateBatchedIOAwaiter();
        void ReturnBatchedIOAwaiter(std::shared_ptr<TBatchedIOAwaiter> batch);

        template <typename T>
        std::unique_ptr<T> AllocateOp();

        void ReturnOp(TDDiskIoOp* op);
        void ReturnOp(TPersistentBufferPartIoOp* op);

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
                NMonitoring::TDynamicCounters::TCounterPtr UnalignedWritePayloads;
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

                NMonitoring::TDynamicCounters::TCounterPtr RunningCount;
            } DirectIO;

            struct {
                // Registrations are keyed by (TabletId, DirectBlockGroupIndex), including empty ones.
                // In production, a tablet registers only one direct block group per buffer.
                NMonitoring::TDynamicCounters::TCounterPtr RegisteredTablets;
                NMonitoring::TDynamicCounters::TCounterPtr RegisteredTabletsLimit;
                NMonitoring::TDynamicCounters::TCounterPtr AllocatedChunks;
                NMonitoring::TDynamicCounters::TCounterPtr TotalBytes;
                NMonitoring::TDynamicCounters::TCounterPtr PendingEventsQueueSize;
                NMonitoring::TDynamicCounters::TCounterPtr InMemoryCacheSize;
                NMonitoring::THistogramPtr WriteBatchSize;
            } PersistentBuffer;

            struct {
                // Writes rejected because no checksum list was attached.
                NMonitoring::TDynamicCounters::TCounterPtr WritesWithoutChecksums;
                // Payload checksum mismatches (write-time sender list, sync source payload, or
                // disk-read vs stored checksums) reported as TReplyStatus::CORRUPTED.
                NMonitoring::TDynamicCounters::TCounterPtr ChecksumMismatch;
                NMonitoring::TDynamicCounters::TCounterPtr MetadataReads;
                // Checksum updates only; chunk-header and extent-format writes are excluded.
                NMonitoring::TDynamicCounters::TCounterPtr ChecksumWrites;
                NMonitoring::TDynamicCounters::TCounterPtr IntegrityCorruption;
                NMonitoring::TDynamicCounters::TCounterPtr IntegrityLostWriteDetected;
            } Checksums;
        };

        TCounters Counters;

        // Separate from the shared monitoring counters: only this actor's router
        // callbacks contribute, until their last access to actor-owned state.
    private:
        friend class TDDiskActorTestPeer;
        std::function<TMonotonic()> DestructionNow;
        std::function<void()> DestructionSleep;
    public:
        static constexpr ui64 DirectIoStopping = ui64{1} << 63;
        static constexpr ui64 DirectIoFlags = DirectIoStopping;
        std::atomic<ui64> DirectIoState{0};
        ui64 GetDirectIoInflight() const {
            return DirectIoState.load(std::memory_order_acquire) & ~DirectIoFlags;
        }
        void OnDirectIODone(NActors::TActorSystem* actorSystem);
        NMonitoring::TDynamicCounters::TCounterPtr IoStalledCounter;

#if defined(__linux__)
        std::shared_ptr<NPDisk::IUringRouterClient> UringRouter;
#endif

    public:
        struct TEvPrivate {
            enum {
                EvHandleSingleQuery = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvHandlePersistentBufferEventForChunk,
                EvRetryIO,
                EvWritePersistentBufferPart,
                EvReadPersistentBufferPart,
                EvIssuePersistentBufferChunkAllocation,
                EvDeallocatePersistentBufferChunk,
                EvDeallocatePersistentBufferChunkResult,
                EvRetryListPersistentBuffer,
                EvFinishStopping,
                EvStopIoTimeout,
                EvBeginStopping,
                EvCompleteStop,
                EvRetryIODelayed,
                EvProcessPersistentBufferRemoval,
                EvExpirePersistentBufferRegistrationToken,
            };

            struct TEvExpirePersistentBufferRegistrationToken
                : TEventLocal<TEvExpirePersistentBufferRegistrationToken, EvExpirePersistentBufferRegistrationToken> {
            };

            struct TEvProcessPersistentBufferRemoval : TEventLocal<TEvProcessPersistentBufferRemoval, EvProcessPersistentBufferRemoval> {
                TPersistentBufferTabletKey Key;
                explicit TEvProcessPersistentBufferRemoval(TPersistentBufferTabletKey key)
                    : Key(key)
                {}
            };

            struct TEvCompleteStop : TEventLocal<TEvCompleteStop, EvCompleteStop> {};
            struct TEvBeginStopping : TEventLocal<TEvBeginStopping, EvBeginStopping> {};
            struct TEvRetryIODelayed : TEventLocal<TEvRetryIODelayed, EvRetryIODelayed> {
                ui64 Id;
                explicit TEvRetryIODelayed(ui64 id) : Id(id) {}
            };
            struct TEvFinishStopping : TEventLocal<TEvFinishStopping, EvFinishStopping> {};
            struct TEvStopIoTimeout : TEventLocal<TEvStopIoTimeout, EvStopIoTimeout> {};

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

            struct TEvRetryIO : TEventLocal<TEvRetryIO, EvRetryIO> {
                std::unique_ptr<TDirectIoOpBase> Op;

                explicit TEvRetryIO(std::unique_ptr<TDirectIoOpBase> op);
                ~TEvRetryIO();
            };

        };

    private:
        enum EWakeupTag {
            WakeupUpdateFreeSpaceInfo = 2,
            WakeupCollectPbStats = 3,
            WakeupProcessPersistentBufferBatchWrite = 4,
            WakeupProcessDeallocatePersistentBufferChunk = 5,
            WakeupCollectMemoryMetrics = 6,
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

        TLine<TMemoryMetricsFrontend> MemoryMetric;
        TLine<TSpaceMetricsFrontend> SpaceMetric;
        TLine<TOperationMetricsFrontend> OperationMetric;
        void RecordOperationMetrics(TMonotonic sampledAt);
        void InitMemoryMetrics();
        void CollectMemoryMetrics();

        const bool IsPersistentBufferActor = false;

        // Actor-thread-only health state. I/O callbacks communicate status/data exclusively
        // through TEvPrivate callbacks, so Broken ordering is defined by the actor mailbox.
        bool Broken = false;
        TString BrokenReason;

        static constexpr TDuration StopIoTimeout = TDuration::Minutes(1);
        static constexpr TStringBuf StoppingReason = "DDisk is stopping";
        bool Stopping = false;
        bool IoStalled = false;
        bool PoisonReceived = false;
        bool OwnDrainComplete = false;
        bool OwnDrainFinishing = false;
        void CompleteStop();
        bool PersistentBufferGone = true;
        TActorId ParentDDiskId;
        static constexpr ui64 PBShutdownCookie = Max<ui64>();
        void BeginStopping(TString reason);
        void HandleBeginStopping();
        void TryCompleteStop();
        void HandleGone(TEvents::TEvGone::TPtr ev);
        void CancelPendingIo(std::unique_ptr<TDirectIoOpBase> op);
        void CancelRetries();
        void HandleRetryIODelayed(TEvPrivate::TEvRetryIODelayed::TPtr ev);

        void RejectQueryWhenStopping(IEventHandle& ev);
        void RejectQuery(IEventHandle& ev,
            NKikimrBlobStorage::NDDisk::TReplyStatus::E status, const TString& reason);
        void RejectPendingDDiskQueries(
            NKikimrBlobStorage::NDDisk::TReplyStatus::E status, const TString& reason);
        void RejectQueuedQueries();
        void FinishStopping();
        void HandleStopIoTimeout();
        void ClearIoStalled();

        bool IsBroken() const;
        TString GetBrokenReason() const;
        void EnterBroken(TString reason);
        void FailDirectIoOp(std::unique_ptr<TDirectIoOpBase> op, TString reason = {});

    public:
        TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters, bool isPersistentBufferActor = false);

        TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const std::vector<ui32>& initPersistentBufferChunks,
            ui64 persistentBufferUniqueId, TIntrusivePtr<TPDiskParams> pDiskParams, NPDisk::TDiskFormatPtr diskFormat
#if defined(__linux__)
            , std::shared_ptr<NPDisk::IUringRouterClient> uringRouter
#endif
            );

        ~TDDiskActor();
        void Bootstrap();
        STFUNC(StateFuncDDisk);
        STFUNC(StateFuncPersistentBuffer);
        STFUNC(StateFuncStopping);
        void PassAway() override;

        // Mirrors TVDiskContext::CheckPDiskResponse: returns true on OK, returns false and
        // switches to StateFuncStopping on session-loss statuses (ERROR / INVALID_OWNER /
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

            // Keeps the mapping and physical chunk alive through requests and allocation.
            ui32 ChunkRefPins = 0;

            bool AllocationPending = false;
            NActors::TAsyncEvent AllocationReady;

            NActors::TAsyncEvent CommitReady;
        };

        struct TTabletState {
            // Node-stable: waiters hold TChunkRef& and its events across co_await.
            THashMap<ui64, TChunkRef> ChunkRefs;
            TTabletStatsEntry Stats;

            bool CanRetire() const {
                return ChunkRefs.empty();
            }
        };

        ui64 MonMappedDataChunks = 0;
        THashMap<ui64, TTabletState> Tablets; // TabletId -> state
        TIntrusivePtr<TPDiskParams> PDiskParams;
        std::vector<TChunkIdx> OwnedChunksOnBoot;
        ui64 ChunkMapSnapshotLsn = Max<ui64>();
        std::queue<TPendingEvent> PendingQueries;
        bool HandlingQueries = false;
        bool LogReplayComplete = false;
        std::optional<ui64> DeferredCutLogFreeUpToLsn;
        ui64 NextLsn = 1;

        void InitPDiskInterface();
        void Handle(NPDisk::TEvYardInitResult::TPtr ev);
        void Handle(NPDisk::TEvReadLogResult::TPtr ev);
        void ValidateChecksumsModeAfterLogReplay();
        void ReconcileStartupReservations();
        void FinishRecovery();
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

        void SetDataChunkMapping(ui64 tabletId, TChunkRef* ref, TChunkIdx chunkIdx);

        // DDisk may pull an integrity chunk from the same reserve as a data
        // chunk, so it keeps a larger reserve than PersistentBuffer.
        static constexpr ui32 MinChunksReservedDDisk = 4;
        static constexpr ui32 MinChunksReservedPersistentBuffer = 2;
        const ui32 MinChunksReserved;
        TChunkManager ChunkManager;
        // Newly reserved chunks are zeroed in slices before they become allocatable in
        // checksums-disabled mode; each formatting coroutine owns its progress.
        absl::flat_hash_set<TChunkIdx> FormattingChunks;
        std::optional<ui64> ReservationCookie;
        bool HandlingChunkReserved = false;
        bool ChunkReservedAgain = false;
        // Abandoned allocations may still have writes in flight. Never reuse them for PB.
        absl::flat_hash_set<TChunkIdx> PendingChunkRelease;
        absl::flat_hash_set<TChunkIdx> ShutdownChunkReleasesIssued;
        using TChunkForData = TChunkManager::TChunkForData;
        using TChunkForPersistentBuffer = TChunkManager::TChunkForPersistentBuffer;
        using TChunkForIntegrity = TChunkManager::TChunkForIntegrity;
        struct TLogTicket {
            std::optional<bool> Result;
            NActors::TAsyncEvent Changed;
            bool IsDDisk = false;
        };
        class TLogAwaiter {
        public:
            static constexpr bool IsActorAwareAwaiter = true;

            TLogAwaiter(TDDiskActor& actor, std::shared_ptr<TLogTicket> ticket)
                : Actor(actor)
                , Ticket(std::move(ticket))
                , Waiter(Ticket->Changed.Wait()) {
            }

            TLogAwaiter& CoAwaitByValue() && noexcept {
                return *this;
            }

            bool await_ready() const noexcept {
                return Ticket->Result.has_value();
            }

            void await_suspend(std::coroutine_handle<> continuation) noexcept {
                Waiter.await_suspend(continuation);
            }

            bool await_resume() const noexcept {
                return Ticket->Result.value_or(false) && !Actor.Stopping
                    && (!Actor.IsBroken() || !Ticket->IsDDisk);
            }

        private:
            TDDiskActor& Actor;
            std::shared_ptr<TLogTicket> Ticket;
            NActors::NDetail::TAsyncEventAwaiter Waiter;
        };
        struct TLogWaiter {
            ui64 DeliveryCookie = 0;
            bool IsDDisk = false;
            std::shared_ptr<TLogTicket> Ticket;
        };
        absl::flat_hash_map<ui64, TLogWaiter> LogWaiters;
        TLogAwaiter WaitForLog(std::shared_ptr<TLogTicket> ticket) {
            Y_ABORT_UNLESS(ticket);
            return TLogAwaiter(*this, std::move(ticket));
        }

        void CompleteLogTicket(const std::shared_ptr<TLogTicket>& ticket, bool ok);
        void FailLogWaiters(bool ddiskOnly);
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
        THashMap<ui64, TPendingIoOp> DelayedRetries;
        ui64 NextRetryId = 0;

        void IssueChunkAllocation(ui64 tabletId, ui64 vChunkIndex);
        void ReserveChunks(size_t count);
        void Handle(NPDisk::TEvChunkReserveResult::TPtr ev);
        void ReleaseUncommittedChunks();
        void HandleChunkReserved();
        void FormatChunk(TChunkIdx chunkIdx);
        void Handle(NPDisk::TEvLogResult::TPtr ev);
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
            ui64 *startingPointLsnPtr,
            TVector<TChunkIdx> chunksToDelete = {}, std::shared_ptr<TLogTicket> ticket = {});
        void IssuePDiskLogRecord(TLogSignature signature, TVector<TChunkIdx> chunksToCommit,
            const NProtoBuf::Message& data, ui64 *startingPointLsnPtr,
            TVector<TChunkIdx> chunksToDelete = {}, std::shared_ptr<TLogTicket> ticket = {});

        NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord CreatePersistentBufferChunkMapSnapshot();
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord CreateChunkMapSnapshot();
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord CreateChunkMapIncrement(ui64 tabletId, ui64 vChunkIndex,
            TChunkIdx chunkIdx, const TIntegrityManager::TExtentRef* extentRef,
            const TIntegrityManager::TMappingSnapshot::TIntegrityChunkEntry* integrityChunk = nullptr);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Integrity management (DDisk mode only)
        //
        // TIntegrityManager owns ordinary allocation, formatting and checksum records;
        // this actor supplies tokenized allocation and typed device-I/O completions. A reserved
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

        struct TDataChunkAllocationInFlight {
            ui64 Token = 0;
            TChunkIdx ChunkIdx = 0;
            bool LogIssued = false;
            ui32 NewlyCommittedChunks = 0;
        };
        absl::flat_hash_map<std::pair<ui64, ui64>, TDataChunkAllocationInFlight> DataChunkAllocationsInFlight;
        ui64 NextDataAllocationToken = 1;

        void SubmitIntegrityWork(TIntegrityManager::TWork work);
        void SubmitIntegrityWrite(TIntegrityManager::TWriteSubmission write);
        void CountIntegrityResult(const TIntegrityManager::TOperationResult& result);
        bool IsChunkCommitted(ui64 tabletId, ui64 vChunkIndex) const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Data-path coroutine primitives
        //
        // Client reads and writes are flat root coroutines. Their frames own the pins and
        // reply routes, and share callback results with outstanding operations, so forced
        // frame destruction cannot invalidate an I/O completion's destination.
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Completion of one data-path device operation, delivered on the I/O thread.
        struct TIoCompletion {
            NKikimrBlobStorage::NDDisk::TReplyStatus::E Status =
                NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN;
            TString ErrorMessage;
            // Reads only, and only on success.
            TReadPayload Data;
        };

        struct TDDiskReadResult {
            NKikimrBlobStorage::NDDisk::TReplyStatus::E Status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            TString ErrorMessage;
            TReadPayload Data;
            TReadChecksums Checksums;
            ui64 TotalSize = 0;
        };

        // Join point for the device operations of one data-path wait, and the shared class
        // callback of those operations. The frame and every outstanding operation own it,
        // including all result slots, so callbacks remain valid after forced destruction of
        // the frame.
        //
        // The frame prepares operations and Add()s them; nothing is submitted and nothing
        // runs concurrently yet. co_await Wait() then submits them all from await_suspend,
        // knowing their number in advance. Pending counts the outstanding operations plus a
        // submission guard held while await_suspend submits, so a completion landing on the
        // I/O thread (or inline) cannot resume the bridge before submission has finished.
        // Whoever drops Pending to zero owns the wakeup: the last completion resumes the
        // library bridge, which posts the actor resume, or await_suspend releases the guard
        // last and continues the frame directly. Device operations keep their shared_ptr
        // until recycling. The frame returns the object to the pool only when it is the
        // sole owner.
        class TBatchedIOAwaiter : public std::enable_shared_from_this<TBatchedIOAwaiter>
        {
        public:
            explicit TBatchedIOAwaiter(TDDiskActor& actor) noexcept;
            ~TBatchedIOAwaiter();

            TBatchedIOAwaiter(const TBatchedIOAwaiter&) = delete;
            TBatchedIOAwaiter& operator=(const TBatchedIOAwaiter&) = delete;

            TDDiskReadResult DataResult;
            // Slots are reserved before submission and kept stationary until all callbacks
            // retire. Each callback owns one slot; the common singleton needs no container
            // allocation.
            absl::InlinedVector<TIoCompletion, 1> MetadataResults;

            // Actor thread. Reserves the next MetadataResults slot and returns its index.
            // Must happen before the operation owning the slot is submitted.
            size_t ReserveMetadataSlot();

            // Actor thread. Queues a prepared operation for the next Wait(). A null operation
            // means its outcome was already recorded in its result slot without device I/O.
            void Add(std::unique_ptr<TDDiskIoOp> op);

            // Any thread, exactly once per submitted operation. No index denotes client data.
            // The caller keeps its shared_ptr across this call; recycling drops it later.
            void OnComplete(TIoCompletion&& completion, std::optional<size_t> metadataIndex) noexcept;

            // Drops results so the idle object can be reused or sit in the pool.
            void ClearForReuse() noexcept;

            // Generic awaiter. co_await installs the library's off-thread bridge.
            // The device thread resumes that bridge; it does not send or touch the frame.
            class TWaiter {
            public:
                explicit TWaiter(TBatchedIOAwaiter& batch) noexcept
                    : Batch(batch)
                {}

                TWaiter(const TWaiter&) = delete;
                TWaiter& operator=(const TWaiter&) = delete;

                // Forced frame destruction while parked: withdraw an unused bridge.
                ~TWaiter() {
                    if (Suspended) {
                        Batch.AbandonBridge();
                    }
                }

                bool await_ready() const noexcept {
                    return Batch.IsIdle();
                }

                std::coroutine_handle<> await_suspend(std::coroutine_handle<> bridge) noexcept {
                    Suspended = true;
                    return Batch.Submit(bridge);
                }

                void await_resume() noexcept {
                    Suspended = false;
                }

            private:
                TBatchedIOAwaiter& Batch;
                bool Suspended = false;
            };

            // Submits everything added so far and waits for every operation of the batch.
            // Deliberately has no cancellation hook: a frame which handed buffers to the
            // device must not unwind while the device may still write into them.
            TWaiter Wait() noexcept {
                return TWaiter(*this);
            }

        private:
            friend class TDDiskActorTestPeer;

            // Publish one result and report whether this callback released the last count.
            bool RecordCompletion(TIoCompletion&& completion, std::optional<size_t> metadataIndex) noexcept;

            // Callback and forced frame cleanup compete for sole ownership of the bridge.
            std::coroutine_handle<> TakeBridge() noexcept {
                return std::coroutine_handle<>::from_address(Bridge.exchange(nullptr, std::memory_order_acq_rel));
            }

            // Nothing to submit and nothing outstanding.
            bool IsIdle() const noexcept {
                return Prepared.empty() && Pending.load(std::memory_order_acquire) == 0;
            }

            // Actor thread, from await_suspend. Submits Prepared under the guard and returns
            // the bridge when every operation already completed, noop otherwise.
            std::coroutine_handle<> Submit(std::coroutine_handle<> bridge) noexcept;

            // Destroys the bridge unless the last completion already took it.
            void AbandonBridge() noexcept;

            TDDiskActor& Actor;
            std::vector<std::unique_ptr<TDDiskIoOp>> Prepared;
            std::atomic<ui32> Pending{0};
            // Bridge address while a frame is parked; exchanged to null by whoever uses it.
            std::atomic<void*> Bridge{nullptr};
        };

        // Hides the cancellation hooks of an awaiter so that a frame holding accepted
        // device buffers or integrity pins keeps waiting even under latched cancellation.
        // The inner awaiter must be a named lvalue that outlives the co_await.
        template<class TInner>
        class TNonCancellableAwaiter {
        public:
            static constexpr bool IsActorAwareAwaiter = true;

            explicit TNonCancellableAwaiter(TInner& inner) noexcept
                : Inner(inner)
            {}

            bool await_ready() {
                return Inner.await_ready();
            }

            template<class TPromise>
            void await_suspend(std::coroutine_handle<TPromise> parent) {
                Inner.await_suspend(parent);
            }

            decltype(auto) await_resume() {
                return Inner.await_resume();
            }

        private:
            TInner& Inner;
        };

        template<class TInner>
        static TNonCancellableAwaiter<TInner> NonCancellable(TInner& inner) noexcept {
            return TNonCancellableAwaiter<TInner>(inner);
        }

        // Where a reply goes once the originating request handle has been dropped.
        struct TClientReplyRoute {
            TActorId OriginalRequester;
            TActorId InterconnectSession;
            ui64 Cookie = 0;
        };

        // Actor work through physical submission, result processing, notifications and replies.
        // Client, metadata and format runners all retain a guard until their cleanup finishes.
        // Forced teardown can destroy frames first; shared callbacks retain their results.
        size_t DataRequestsInFlight = 0;
        class TDataRequestGuard {
        public:
            explicit TDataRequestGuard(TDDiskActor& self) noexcept
                : Self(&self)
            {
                ++Self->DataRequestsInFlight;
            }

            TDataRequestGuard(const TDataRequestGuard&) = delete;
            TDataRequestGuard& operator=(const TDataRequestGuard&) = delete;

            ~TDataRequestGuard() {
                Release();
            }

            // Normal completion releases explicitly, before checking the stop barrier.
            // Forced frame destruction only decrements, without touching the actor.
            void Release() noexcept {
                if (auto* self = std::exchange(Self, nullptr)) {
                    Y_ABORT_UNLESS(self->DataRequestsInFlight);
                    --self->DataRequestsInFlight;
                }
            }

        private:
            TDDiskActor* Self;
        };

        // Everything a read coroutine needs after the originating request is dropped.
        struct TDataRead {
            TQueryCredentials ResolvedCredentials;
            TBlockSelector Selector;
            TClientReplyRoute Reply;
            NWilson::TSpan Span;
            NHPTimer::STime StartTs = 0;
        };

        // Reading metadata first costs one extra latency hop, but avoids a large useless data
        // read when the range turns out to be a hole or its checksums cannot be read.
        static constexpr ui32 MetadataFirstReadThreshold = 32u << 10;

        // Owns the read and the caller's pin on pinnedChunk for its whole lifetime.
        void ExecuteDataRead(TDataRead read, TChunkRef* pinnedChunk);
        void FinishDDiskRead(TDataRead& read, TDDiskReadResult& result);
        // The Prepare* functions only build an operation: nothing is submitted until the batch
        // is awaited, and the operation is bound to its batch at that point. A null result
        // means the outcome was recorded in the batch without device I/O; batch->Add() accepts it.
        //
        // Prepares the client data read of one request. Fills DataResult and returns null
        // when the disk is already stopping or broken.
        std::unique_ptr<TDDiskIoOp> PrepareDataRead(TBatchedIOAwaiter& batch, TChunkIdx chunkIdx,
            const TBlockSelector& selector);
        // Prepares one claimed metadata read into the next MetadataResults slot. Fills that
        // slot and returns null when stopping. Broken operations complete inline on submission.
        std::unique_ptr<TDDiskIoOp> PrepareMetadataRead(TBatchedIOAwaiter& batch,
            const TIntegrityManager::TMetadataRead& read);
        // Hands the loaded images to TIntegrityManager, latching Broken first when a critical
        // load failed. The caller clears the consumed slots before its next suspension.
        void CompleteMetadataReads(TConstArrayRef<TIntegrityManager::TMetadataRead> reads,
            TArrayRef<TIoCompletion> completions);

        // Critical (retried on overload) write at an offset inside the chunk. A metadata
        // index routes the completion to that MetadataResults slot instead of DataResult.
        std::unique_ptr<TDDiskIoOp> PrepareCriticalWrite(TChunkIdx chunk, ui32 offset, TRcBuf data,
            std::optional<size_t> metadataIndex = std::nullopt);

        struct TSyncInFlight;

        // Everything a write coroutine needs after the originating request is dropped. A Sync
        // carries its source inputs in Sync, and Selector, Data and Checksums describe the
        // piece currently being written.
        struct TDataWrite {
            TQueryCredentials OriginalCredentials;
            TQueryCredentials ResolvedCredentials;
            TBlockSelector Selector;
            TRope Data;
            std::vector<ui64> Checksums;
            TClientReplyRoute Reply;
            NWilson::TSpan Span;
            NHPTimer::STime StartTs = 0;
            std::shared_ptr<TSyncInFlight> Sync;
        };

        // The destination write of both Write and Sync, as one flat coroutine. A Write is a
        // single piece whose payload came with the request; a Sync reads every piece from its
        // source first. Each piece is validated and allocated as needed. With checksums it then
        // waits for exclusive ownership of the metadata pairs and rechecks the session; only then
        // is data admitted. A cold pair is loaded and transformed on the actor thread, and the
        // data write and the metadata image write go to the device as one batch, which drains
        // before the next piece starts.
        //
        // Owns the request and the caller's pin on chunk, if any. A Sync passes no chunk and
        // pins it on its first valid source piece.
        void ExecuteDataWrite(TDataWrite write, TChunkRef* chunk);
        bool WriteSessionMatches(const TQueryCredentials& original, const TQueryCredentials& resolved) const;
        std::unique_ptr<TDDiskIoOp> PrepareDataWrite(TChunkIdx chunk, const TBlockSelector& selector,
            TRope data);
        void CompleteMetadataWrite(TIntegrityManager::TWriteOperation& operation,
            const std::shared_ptr<TIntegrityManager::TMetadataWrite>& context, const TIoCompletion& completion);
        void FinishDDiskWrite(TDataWrite& write,
            NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString error);

        void ApplyDDiskReadMetadata(TDDiskReadResult& result,
            TIntegrityManager::TOperationResult&& metadata);
        void ApplyDDiskReadMetadata(TDDiskReadResult& result,
            const TIntegrityManager::TOperationResult& metadata);
        template<class TMetadata>
        void ApplyDDiskReadMetadataImpl(TDDiskReadResult& result, TMetadata&& metadata);
        struct TIntegrityReclamation {
            bool Ok;
            std::shared_ptr<TLogTicket> Ticket;
        };
        // Assigns free slots, returns never-logged chunks to the reserve, and submits an
        // optional snapshot ticket for reclaiming completely unused committed integrity chunks.
        TIntegrityReclamation PrepareIntegrityReclamation(bool wait = false);
        std::shared_ptr<TLogTicket> CommitDataChunk(ui64 tabletId, ui64 vChunkIndex, ui64 token);
        void AllocatePersistentBufferChunk(TChunkIdx chunkIdx);
        void AllocateChunk(TChunkManager::TAllocation allocation, TChunkIdx chunkIdx);
        void AllocateDataChunk(ui64 tabletId, ui64 vChunkIndex, ui64 token, TChunkIdx chunkIdx);
        void CompleteDataChunkAllocation(ui64 tabletId, ui64 vChunkIndex, ui64 token);
        bool IsIntegrityChunkCommitted(TChunkIdx chunkIdx) const;

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
            TQueryCredentials creds;
            return CheckQueryImpl<true>(ev, counters, creds);
        }

        template<typename TCountersPtr>
        bool CheckQuery(TEventHandle<TEvRead>& ev, TCountersPtr counters, TQueryCredentials& creds) const {
            return CheckQueryImpl<false>(ev, counters, creds);
        }

        template<bool RewriteCredentials, typename TEvent, typename TCountersPtr>
        bool CheckQueryImpl(TEventHandle<TEvent>& ev, TCountersPtr counters, TQueryCredentials& creds) const {
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

            if constexpr (RewriteCredentials) {
                creds.SerializeResolvedForRequest(record.MutableCredentials());
            }

            if constexpr (std::is_same_v<TEventType, TEvWritePersistentBuffer>
                    || std::is_same_v<TEventType, TEvReadPersistentBuffer>
                    || std::is_same_v<TEventType, TEvErasePersistentBuffer>
                    || std::is_same_v<TEventType, TEvBatchErasePersistentBuffer>
                    || std::is_same_v<TEventType, TEvListPersistentBuffer>) {
                // NOTE: durable registration (TEvRegisterPersistentBuffer) is a hard precondition
                // for all persistent-buffer reads/writes/erases below. This requires lockstep
                // upgrades of client and DDisk: an older client without register support loses
                // all PB operations against a DDisk running this code, and this client build
                // connecting to an older DDisk gets its register event undelivered and the
                // connect fails. There is no fallback or version negotiation, so mixed fleets
                // running old/new builds simultaneously are not supported for PB traffic.
                if (PersistentBufferReady) {
                    const auto status = CheckPersistentBufferOwnership(creds);
                    if (status != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
                        SendReply(ev, std::make_unique<typename TEvent::TResult>(status,
                            "persistent buffer registration is not registered, not yet durable, or closed"));
                        registerError();
                        return false;
                    }
                }
            }

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

        // Regular direct I/O.
        // Note: releases the op when it is submitted to io_uring or moved to the PDisk fallback.
        void DirectUringOp(std::unique_ptr<TDirectIoOpBase>& op, bool isRetry = false);

        // Do not call manually!
        void DirectUringOpImpl(std::unique_ptr<TDirectIoOpBase>& op);

        void HandleRetryIO(TEvPrivate::TEvRetryIO::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Sync
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        static constexpr ui32 SyncPieceSize = 512u << 10;

        struct TSyncReadRequest {
            NKikimrBlobStorage::NDDisk::TReplyStatus::E Status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            TBlockSelector Selector;
            TString ErrorReason;
            TActorId Source;
            TQueryCredentials Credentials;
            ui64 Lsn = 0;
            ui64 Generation = 0;
            ui32 Retired = 0;
            bool PersistentBufferSource = false;
        };
        struct TSyncSourceResult {
            NKikimrBlobStorage::NDDisk::TReplyStatus::E Status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            TString ErrorReason;
            bool HasPayload = false;
            TRope Data;
            std::vector<ui64> Checksums;
        };
        struct TSyncInFlight {
            ui64 Id = 0;
            TQueryCredentials Creds;
            ui64 VChunkIndex = 0;
            ui64 RequestedBytes = 0;
            std::vector<TSyncReadRequest> Requests;
            ui64 SourceCookie = 0;
            TBlockSelector Selector;
            std::optional<TSyncSourceResult> SourceResult;
            NActors::TAsyncEvent Changed;
        };
        ui64 NextSyncId = 1;
        THashMap<ui64, std::shared_ptr<TSyncInFlight>> SyncsInFlight;
        struct TSyncSourceCookie {
            ui64 SyncId;
            bool PersistentBuffer;
        };
        THashMap<ui64, TSyncSourceCookie> SyncSourceCookies;
        void Handle(TEvSync::TPtr ev);
        // Requests write.Selector of the input from its source.
        void SendSyncSourceRead(TDataWrite& write, const TSyncReadRequest& input);
        // Validates the received source piece and moves its payload into write. On failure,
        // records the outcome in input and returns false.
        bool AcceptSyncSource(TDataWrite& write, TSyncReadRequest& input);
        void FinishSync(TDataWrite& write);
        void Handle(TEvReadResult::TPtr ev);
        void Handle(TEvReadPersistentBufferResult::TPtr ev);
        template<class TResult>
        void HandleSyncSourceResult(typename TResult::TPtr ev, bool persistentBuffer);
        void HandleSyncSourceUndelivered(ui64 cookie, bool persistentBuffer);
        void CompleteSyncSource(ui64 cookie, bool persistentBuffer,
            NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString reason,
            bool hasPayload, TRope data, std::vector<ui64> checksums);
        void CancelPendingSyncSources();
        void QueueSync(ui64 id);
        // Sync roots wake when the mapping commit progresses.
        void QueueSyncsForChunk(ui64 tabletId, ui64 vChunkIndex);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Persistent buffer services
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::map<TPersistentBufferId, TPersistentBuffer> PersistentBuffers;
        std::map<TInstant, absl::flat_hash_set<TPersistentBufferRecordId>> PersistentBuffersInMemoryCacheUptime;
        ui64 PersistentBufferInMemoryCacheSize = 0;
        TInstant StartedAt;

        ui64 CalcPersistentBufferInMemoryCacheSize();

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
            enum class EBarrierOperation { None, Erase, Register, Close, Remove };
            EBarrierOperation BarrierOperation = EBarrierOperation::None;
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

        struct TPersistentBufferRemoval {
            enum class EStage { Drain, Close, Wait, Remove };
            EStage Stage = EStage::Drain;
            TInstant Deadline;
            TAutoPtr<IEventHandle> Request;
        };
        std::map<TPersistentBufferTabletKey, TPersistentBufferRemoval> PersistentBufferRemovals;
        std::set<TPersistentBufferTabletKey> PersistentBufferRegistrations;
        // In-flight barrier sectors stay occupied even if a newer version is durable.
        // The flag requests reclamation once this sector's own write has completed.
        absl::flat_hash_map<TPersistentBufferLocation, bool> PersistentBufferBarrierWrites;
        void ReleasePersistentBufferBarrierSector(TPersistentBufferSectorInfo sector);
        void UpdateRegisteredTabletsCounter();
        void CompletePersistentBufferBarrierWrite(TPersistentBufferDiskOperationInFlight& inflight);
        NKikimrBlobStorage::NDDisk::TReplyStatus::E CheckPersistentBufferOwnership(const TQueryCredentials& creds) const;
        struct TPersistentBufferRegistrationToken {
            ui64 Token = 0;
            TMonotonic IssuedAt = TMonotonic::Zero();
            TPersistentBufferTabletKey Key{};
            ui32 Generation = 0;

            TPersistentBufferRegistrationToken() = default;
            TPersistentBufferRegistrationToken(TMonotonic now, const TQueryCredentials& creds);
            static ui64 Generate(TMonotonic now);
        };
        // Actor-local, ordered by token and issue time; never survives a PB restart.
        std::deque<TPersistentBufferRegistrationToken> PersistentBufferRegistrationTokens;
        bool PersistentBufferRegistrationTokenExpiryScheduled = false;
        void Handle(TEvGetPersistentBufferRegistrationToken::TPtr ev);
        void Handle(TEvPrivate::TEvExpirePersistentBufferRegistrationToken::TPtr ev);
        void Handle(TEvRegisterPersistentBuffer::TPtr ev);
        void Handle(TEvUnregisterPersistentBuffer::TPtr ev);
        void Handle(TEvPrivate::TEvProcessPersistentBufferRemoval::TPtr ev);
        void ProcessPersistentBufferRemoval(TPersistentBufferTabletKey key);

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
        void BarrierErasePersistentBuffer(IEventHandle& queryEv, const TQueryCredentials& creds, const std::vector<TEraseLsnId>& erases, ui64 lsn,
            TPersistentBufferDiskOperationInFlight::EBarrierOperation operation = TPersistentBufferDiskOperationInFlight::EBarrierOperation::Erase);
        void FastErasePersistentBuffer(IEventHandle& queryEv, const TQueryCredentials& creds, const std::vector<TEraseLsnId>& erases, const TFastErase& fastErase);
        void ClearPersistentBufferRecords(TPersistentBufferDiskOperationInFlight& inflight, ui64 partCookie);
        void HandleWritePart(TPersistentBufferDiskOperationInFlight& inflight,  ui64 opCookie, ui64 partCookie);
        void FinishPersistentBufferWrite(ui64 opCookie);
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
        // Per-tablet statistics (DDisk mode only)
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TTabletStatsTracker<TTabletState> TabletStats{&Tablets};
        TActorId TabletStatsActor;
        bool TabletStatsActive = false;

        void NotifyTabletStats();
        void Handle(TEvCollectTabletStats::TPtr ev);
        void Handle(TEvGetTabletStats::TPtr ev);
        void CountTabletIo(ui64 tabletId, ETabletOperation operation, ui64 requests, ui64 bytes);
        void CountTabletIo(ui64 tabletId, TTabletStatsEntry* entry, ETabletOperation operation, ui64 requests, ui64 bytes);
        void CountTabletChunks(ui64 tabletId, i64 delta);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Monitoring page (DDisk mode only)
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void RegisterMonPage();
        void Handle(NMon::TEvHttpInfo::TPtr ev);
    };

} // NKikimr::NDDisk
