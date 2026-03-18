#pragma once

#include "defs.h"

#include "ddisk.h"
#include "persistent_buffer_space_allocator.h"
#include "segment_manager.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

#if defined(__linux__)
#include <ydb/library/pdisk_io/uring_router.h>
#endif

#include <ydb/library/pdisk_io/uring_operation.h>

#include <atomic>
#include <queue>

namespace NKikimrBlobStorage::NDDisk::NInternal {
    class TChunkMapLogRecord;
    class TPersistentBufferChunkMapLogRecord;
}

#define LIST_COUNTERS_INTERFACE_OPS(XX) \
    XX(Write) \
    XX(Read) \
    XX(SyncWithPersistentBuffer) \
    XX(SyncWithDDisk) \
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
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TIntrusivePtr<NMonitoring::TDynamicCounters> CountersBase;
        std::vector<std::pair<TString, TString>> CountersChain;
        ui64 DDiskInstanceGuid = RandomNumber<ui64>();

        static constexpr ui32 BlockSize = 4096;

#if defined(__linux__)
        std::unique_ptr<NPDisk::TUringRouter> UringRouter;
#endif

        static constexpr ui32 MaxInFlight = 256; // TODO: make configurable

        class TDirectIoOpBase;
        class TDDiskIoOp;
        class TPersistentBufferPartIoOp;
        class TInternalSyncWriteOp;

        std::queue<std::unique_ptr<TDirectIoOpBase>> DirectIoQueue;

        NPDisk::TDiskFormatPtr DiskFormat{nullptr, nullptr};

    private:
        struct TOpCountersBase {
            NMonitoring::TDynamicCounters::TCounterPtr Requests;
            NMonitoring::TDynamicCounters::TCounterPtr Bytes;
            NMonitoring::TDynamicCounters::TCounterPtr BytesInFlight;
            NMonitoring::THistogramPtr RequestSizeKiB;
            NMonitoring::THistogramPtr ResponseTime;

            void Request(ui32 bytes = 0) {
                ++*Requests;
                if (bytes) {
                    *Bytes += bytes;
                    *BytesInFlight += bytes;
                    RequestSizeKiB->Collect(bytes >> 10);
                }
            }

            void Done(ui32 bytes, double durationMs = 0) {
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

                NMonitoring::TDynamicCounters::TCounterPtr QueueSize;
                NMonitoring::TDynamicCounters::TCounterPtr RunningCount;
                NMonitoring::THistogramPtr QueueTime;
            } DirectIO;
        };

        TCounters Counters;

    private:
        struct TEvPrivate {
            enum {
                EvHandleSingleQuery = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvHandleEventForChunk,
                EvHandlePersistentBufferEventForChunk,
                EvShortIO,
                EvWritePersistentBufferPart,
                EvReadPersistentBufferPart,
                EvInternalSyncWriteResult,
            };

            struct TEvHandleEventForChunk : TEventLocal<TEvHandleEventForChunk, EvHandleEventForChunk> {
                ui64 TabletId;
                ui64 VChunkIndex;

                TEvHandleEventForChunk(ui64 tabletId, ui64 vChunkIndex)
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

            struct TEvInternalSyncWriteResult : TEventLocal<TEvInternalSyncWriteResult, EvInternalSyncWriteResult> {
                ui64 SyncId = 0;
                ui64 RequestId = 0;
                ui64 SegmentBegin = 0;
                ui64 SegmentEnd = 0;
                NKikimrBlobStorage::NDDisk::TReplyStatus::E Status = NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN;
                TString ErrorMessage;

                TEvInternalSyncWriteResult(ui64 syncId, ui64 requestId, ui64 segmentBegin, ui64 segmentEnd,
                    NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString errorMessage = {})
                    : SyncId(syncId)
                    , RequestId(requestId)
                    , SegmentBegin(segmentBegin)
                    , SegmentEnd(segmentEnd)
                    , Status(status)
                    , ErrorMessage(std::move(errorMessage))
                {}
            };
        };

        enum EWakeupTag {
            WakeupIoSubmitQueue = 1,
            WakeupUpdateFreeSpaceInfo = 2,
        };

    public:
        TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TIntrusivePtr<NMonitoring::TDynamicCounters> counters);
        ~TDDiskActor();
        void Bootstrap();
        STFUNC(StateFunc);
        void PassAway() override;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Boot sequence and PDisk management
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TPendingEvent {
            std::unique_ptr<IEventHandle> Ev;
            NWilson::TSpan QueueSpan;

            template<typename TEvent>
            TPendingEvent(TAutoPtr<TEventHandle<TEvent>> ev, const char *name)
                : Ev(ev.Release())
                , QueueSpan(TWilson::DDiskInternals, NWilson::TTraceId(Ev->TraceId), name, NWilson::EFlags::AUTO_END,
                    TActivationContext::ActorSystem())
            {}

            TAutoPtr<IEventHandle> Release() {
                return Ev.release();
            }
        };

        struct TChunkRef {
            TChunkIdx ChunkIdx;
            std::queue<TPendingEvent> PendingEventsForChunk;
        };

        THashMap<ui64, THashMap<ui64, TChunkRef>> ChunkRefs; // TabletId -> (VChunkIndex -> ChunkIdx)
        TIntrusivePtr<TPDiskParams> PDiskParams;
        TFileHandle DiskFd;
        std::vector<TChunkIdx> OwnedChunksOnBoot;
        ui64 ChunkMapSnapshotLsn = Max<ui64>();
        std::queue<TPendingEvent> PendingQueries;
        bool HandlingQueries = false;
        ui64 NextLsn = 1;
        std::set<std::tuple<ui64, ui64, ui32>> ChunkMapIncrementsInFlight;

        void InitPDiskInterface();
        void Handle(NPDisk::TEvYardInitResult::TPtr ev);
        void Handle(NPDisk::TEvReadLogResult::TPtr ev);
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

        static constexpr ui32 MinChunksReserved = 2;
        std::queue<TChunkIdx> ChunkReserve;
        bool ReserveInFlight = false;

        struct TChunkForData {
            ui64 TabletId;
            ui64 VChunkIndex;
        };

        struct TChunkForPersistentBuffer {};

        std::queue<std::variant<TChunkForData, TChunkForPersistentBuffer>> ChunkAllocateQueue;
        THashMap<ui64, std::function<void()>> LogCallbacks;
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
        void Handle(NPDisk::TEvLogResult::TPtr ev);
        void Handle(TEvPrivate::TEvHandleEventForChunk::TPtr ev);
        void Handle(TEvPrivate::TEvHandlePersistentBufferEventForChunk::TPtr ev);

        void Handle(NPDisk::TEvCutLog::TPtr ev);

        void Handle(NPDisk::TEvChunkWriteRawResult::TPtr ev);
        void Handle(NPDisk::TEvChunkReadRawResult::TPtr ev);

        ui64 GetFirstLsnToKeep() const;

        void IssuePDiskLogRecord(TLogSignature signature, TChunkIdx chunkIdxToCommit, const NProtoBuf::Message& data,
            ui64 *startingPointLsnPtr, std::function<void()> callback);

        NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord CreatePersistentBufferChunkMapSnapshot(const std::vector<ui64>& newChunkIdxs = {});
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord CreateChunkMapSnapshot();
        NKikimrBlobStorage::NDDisk::NInternal::TChunkMapLogRecord CreateChunkMapIncrement(ui64 tabletId, ui64 vChunkIndex,
            TChunkIdx chunkIdx);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Connection management
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TConnectionInfo {
            ui64 TabletId;
            ui32 Generation;
            ui32 NodeId;
            TActorId InterconnectSessionId;
        };
        THashMap<ui64, TConnectionInfo> Connections;

        void Handle(TEvConnect::TPtr ev);
        void Handle(TEvDisconnect::TPtr ev);

        // validate query credentials against registered connections
        bool ValidateConnection(const IEventHandle& ev, const TQueryCredentials& creds) const;

        // a general way to send reply to any incoming message
        void SendReply(const IEventHandle& queryEv, std::unique_ptr<IEventBase> replyEv) const;

        // common function to validate any incoming event's credentials
        template<typename TEvent, typename TCountersPtr>
        bool CheckQuery(TEventHandle<TEvent>& ev, TCountersPtr counters) const {
            const auto& record = ev.Get()->Record;

            auto registerError = [&] {
                if constexpr (!std::is_same_v<TCountersPtr, std::nullptr_t>) {
                    counters->Request(0);
                    counters->Reply(false);
                }
            };

            const TQueryCredentials creds(record.GetCredentials());
            if (!ValidateConnection(ev, creds)) {
                SendReply(ev, std::make_unique<typename TEvent::TResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::SESSION_MISMATCH));
                registerError();
                return false;
            }

            using TRecord = std::decay_t<decltype(record)>;

            if constexpr (NPrivate::THasSelectorField<TRecord>::value) {
                const TBlockSelector selector(record.GetSelector());
                if (selector.OffsetInBytes % BlockSize || selector.Size % BlockSize || !selector.Size) {
                    SendReply(ev, std::make_unique<typename TEvent::TResult>(
                        NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                        "offset and size must be multiple of block size and size must be nonzero"));
                    registerError();
                    return false;
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
                        SendReply(ev, std::make_unique<typename TEvent::TResult>(
                            NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                            "declared data size must match actually sent one"));
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
        // Note: releases the op on success (returns true).
        void DirectUringOp(std::unique_ptr<TDirectIoOpBase>& op, bool flush = true, bool isShort = false);

        // Do not call manually!
        bool DirectUringOpImpl(std::unique_ptr<TDirectIoOpBase>& op, bool flush = true);

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
            enum ESourceKind {
                ESK_DDISK,
                ESK_PERSISTENT_BUFFER
            };

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
            ESourceKind SourceKind;
        };

        using TSyncIt = THashMap<ui64, TSyncInFlight>::iterator;

        ui64 NextSyncId = 1;
        THashMap<ui64, TSyncInFlight> SyncsInFlight; // syncId -> TSyncInFlight
        TSegmentManager SegmentManager;

        void Handle(TEvSyncWithPersistentBuffer::TPtr ev);
        void Handle(TEvSyncWithDDisk::TPtr ev);
        void Handle(TEvReadResult::TPtr ev);
        void Handle(TEvReadPersistentBufferResult::TPtr ev);
        void Handle(TEvPrivate::TEvInternalSyncWriteResult::TPtr ev);

        struct TSyncWithPersistentBufferPolicy;
        struct TSyncWithDDiskPolicy;

        template <typename TPolicy, typename TEventPtr>
        void HandleSync(TEventPtr ev);

        template <typename TEventPtr>
        void InternalSyncReadResult(TEventPtr ev);

        template <typename TResultEvent, typename TCounters>
        std::unique_ptr<IEventHandle> MakeSyncResult(const TSyncInFlight& sync, TCounters& counters) const;

        void ReplySync(TSyncIt it);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Persistent buffer services
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TPersistentBuffer {
            struct TRecord {
                ui32 OffsetInBytes;
                ui32 Size;
                std::vector<TPersistentBufferSectorInfo> Sectors;
                std::map<ui64, TRope> DataParts;
                ui32 PartsCount;

                TRope JoinData(ui32 sectorSize);
            };

            std::map<ui64, TRecord> Records;
        };

        std::map<std::tuple<ui64, ui32, ui64>, TPersistentBuffer> PersistentBuffers;
        ui64 PersistentBufferInMemoryCacheSize = 0;

        ui64 CalcPersistentBufferInMemoryCacheSize();
        TString PersistentBufferToString();

        void SanitizePersistentBufferInMemoryCache(TPersistentBuffer::TRecord& record, bool force = false);

        static constexpr ui32 MaxSectorsPerBufferRecord = 128;

        ui32 SectorSize;
        ui32 SectorInChunk;
        ui32 ChunkSize;
        TPersistentBufferFormat PersistentBufferFormat;

        double NormalizedOccupancy = -1;

        struct TPersistentBufferHeader {
            static constexpr ui8 PersistentBufferHeaderSignature[16] = {249, 173, 163, 160, 196, 193, 69, 133, 83, 38, 34, 104, 170, 146, 237, 156};
            static constexpr ui32 HeaderChecksumOffset = 24;
            static constexpr ui32 HeaderChecksumSize = 8;

            ui8 Signature[16];
            ui64 HeaderChecksum;
            ui64 TabletId;
            ui32 Generation;
            ui64 VChunkIndex;
            ui32 OffsetInBytes;
            ui32 Size;
            ui64 Lsn;
            TPersistentBufferSectorInfo Locations[MaxSectorsPerBufferRecord];
        };

        bool IssuePersistentBufferChunkAllocationInflight = false;

        struct TPersistentBufferDiskOperationInFlight {
            TActorId Sender;
            ui64 Cookie;
            TActorId Session;
            NWilson::TSpan Span;
            std::set<ui64> OperationCookies;

            ui64 TabletId;
            ui32 Generation;
            ui64 VChunkIdx;
            ui64 Lsn;
            ui32 OffsetInBytes;
            ui32 Size;
            std::vector<TPersistentBufferSectorInfo> Sectors;
            TRope Data;

            NKikimrBlobStorage::NDDisk::TReplyStatus::E Status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            std::optional<TString> ErrorMessage = std::nullopt;

            NHPTimer::STime StartTs{};
        };

        std::unordered_map<ui64, TPersistentBufferDiskOperationInFlight> PersistentBufferDiskOperationInflight;

        ui32 PersistentBufferRestoreChunksInflight = 0;

        TPersistentBufferSpaceAllocator PersistentBufferSpaceAllocator;

        ui64 PersistentBufferChunkMapSnapshotLsn = Max<ui64>();
        std::queue<TPendingEvent> PendingPersistentBufferEvents;
        bool PersistentBufferReady = false;

        std::unordered_map<ui64, std::vector<ui64>> PersistentBufferSectorsChecksum;
        std::unordered_set<ui32> PersistentBufferAllocatedChunks;
        std::unordered_set<ui32> PersistentBufferRestoringChunks;

        TActorId WritePersistentBuffersActor;

        void InitPersistentBuffer();
        void IssuePersistentBufferChunkAllocation();
        void ProcessPersistentBufferQueue();
        std::vector<std::tuple<ui32, ui32, TRope>> SlicePersistentBuffer(ui64 tabletId, ui32 generation, ui64 vchunkIndex, ui64 lsn, ui32 offsetInBytes, ui32 size, TRope&& data, const std::vector<TPersistentBufferSectorInfo>& sectors);
        void StartRestorePersistentBuffer();
        void RestorePersistentBufferChunk(TEvPrivate::TEvReadPersistentBufferPart::TPtr ev);
        void ReplyReadPersistentBuffer(ui64 operationCookie, TRope&& data);
        void ProcessPersistentBufferWrite(TEvWritePersistentBuffer::TPtr ev);
        double GetPersistentBufferFreeSpace();

        void Handle(TEvWritePersistentBuffer::TPtr ev);
        void Handle(TEvReadPersistentBuffer::TPtr ev);
        void Handle(TEvErasePersistentBuffer::TPtr ev);
        void Handle(TEvBatchErasePersistentBuffer::TPtr ev);
        void Handle(TEvWriteResult::TPtr ev);
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void Handle(TEvListPersistentBuffer::TPtr ev);

        void Handle(TEvWritePersistentBuffers::TPtr ev);

        void Handle(TEvPrivate::TEvReadPersistentBufferPart::TPtr ev);
        void Handle(TEvPrivate::TEvWritePersistentBufferPart::TPtr ev);

        void HandleWriteInFlight(ui64 cookie, const std::function<std::unique_ptr<IEventBase>()>& factory);

        void ProcessIoSubmitQueue();
        void ScheduleIoSubmitWakeup();
        void HandleWakeup(TEvents::TEvWakeup::TPtr &ev);
        void Handle(NPDisk::TEvCheckSpaceResult::TPtr ev);
        void UpdateFreeSpaceInfo();
    };

} // NKikimr::NDDisk
