#pragma once

#include "defs.h"

#include "ddisk.h"
#include "persistent_buffer_space_allocator.h"
#include "segment_manager.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

#if defined(__linux__)
#include <ydb/library/pdisk_io/uring_router.h>
#endif

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
        std::atomic<ui32> InFlightCount{0};
        static constexpr ui32 MaxInFlight = 128; // TODO: make configurable
#endif

        NPDisk::TDiskFormatPtr DiskFormat{nullptr, nullptr};

    private:
        struct TInterfaceOpCounters {
            NMonitoring::TDynamicCounters::TCounterPtr Requests;
            NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
            NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;
            NMonitoring::TDynamicCounters::TCounterPtr Bytes;

            void Request(ui32 bytes = 0) {
                ++*Requests;
                if (bytes) {
                    *Bytes += bytes;
                }
            }

            void Reply(bool ok, ui32 bytes = 0) {
                ++*(ok ? ReplyOk : ReplyErr);
                if (bytes) {
                    *Bytes += bytes;
                }
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
                NMonitoring::TDynamicCounters::TCounterPtr ShortReads;
                NMonitoring::TDynamicCounters::TCounterPtr ShortWrites;
            } DirectIO;
        };

        TCounters Counters;

    public:
#if defined(__linux__)
        struct TDirectIoOp;
#endif

    private:
        struct TEvPrivate {
            enum {
                EvHandleSingleQuery = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvHandleEventForChunk,
                EvHandlePersistentBufferEventForChunk,
                EvShortIO,
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

#if defined(__linux__)
            struct TEvShortIO : TEventLocal<TEvShortIO, EvShortIO> {
                std::unique_ptr<TDirectIoOp> Op;
                explicit TEvShortIO(std::unique_ptr<TDirectIoOp> op);
                ~TEvShortIO();
            };
#endif
        };

    public:
        TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters);
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

        struct TPendingWrite {
            NWilson::TSpan Span;
            std::function<void(NPDisk::TEvChunkWriteRawResult&, NWilson::TSpan&&)> Callback;
        };

        using TPersistentBufferPendingWrite = std::function<void(NPDisk::TEvChunkWriteRawResult&)>;

        THashMap<ui64, std::variant<TPendingWrite, TPersistentBufferPendingWrite>> WriteCallbacks;

        struct TPendingRead {
            NWilson::TSpan Span;
            std::function<void(NPDisk::TEvChunkReadRawResult&, NWilson::TSpan&&)> Callback;
        };

        using TPersistentBufferPendingRead = std::function<void(NPDisk::TEvChunkReadRawResult&)>;

        THashMap<ui64, std::variant<TPendingRead, TPersistentBufferPendingRead>> ReadCallbacks;

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
                    counters->Request();
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

        void SendInternalWrite(
            TChunkRef& chunkRef,
            const TBlockSelector &selector,
            NWilson::TSpan&& span,
            TRope &&data,
            std::function<void(NPDisk::TEvChunkWriteRawResult&, NWilson::TSpan&&)> callback
        );

        void Handle(TEvWrite::TPtr ev);
        void Handle(TEvRead::TPtr ev);

#if defined(__linux__)
        void DirectWrite(TEvWrite::TPtr ev, const TBlockSelector& selector, const TWriteInstruction& instr,
            TChunkRef& chunkRef, NWilson::TSpan span);
        void DirectRead(TEvRead::TPtr ev, const TBlockSelector& selector, TChunkRef& chunkRef,
            NWilson::TSpan span);
        void HandleShortIO(TEvPrivate::TEvShortIO::TPtr ev);
#endif

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
        void Handle(TEvReadPersistentBufferResult::TPtr ev);
        void Handle(TEvReadResult::TPtr ev);

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
                std::map<ui32, TRope> DataParts;
                ui32 PartsCount;

                TRope JoinData(ui32 sectorSize);
            };

            std::map<ui64, TRecord> Records;
        };

        std::map<std::tuple<ui64, ui64>, TPersistentBuffer> PersistentBuffers;
        ui64 PersistentBufferInMemoryCacheSize = 0;

        TString PersistentBufferToString();

        void SanitizePersistentBufferInMemoryCache(TPersistentBuffer::TRecord& record, bool force = false);

        static constexpr ui32 MaxSectorsPerBufferRecord = 128;

        ui32 SectorSize;
        ui32 SectorInChunk;
        ui32 ChunkSize;
        ui32 MaxChunks;
        ui32 PersistentBufferInitChunks;
        ui32 MaxPersistentBufferInMemoryCache;
        ui32 MaxPersistentBufferChunkRestoreInflight;

        struct TPersistentBufferHeader {
            static constexpr ui8 PersistentBufferHeaderSignature[16] = {249, 173, 163, 160, 196, 193, 69, 133, 83, 38, 34, 104, 170, 146, 237, 156};
            static constexpr ui32 HeaderChecksumOffset = 24;
            static constexpr ui32 HeaderChecksumSize = 8;

            ui8 Signature[16];
            ui64 HeaderChecksum;
            ui64 TabletId;
            ui64 VChunkIndex;
            ui32 OffsetInBytes;
            ui32 Size;
            ui64 Lsn;
            TPersistentBufferSectorInfo Locations[MaxSectorsPerBufferRecord];
        };

        bool IssuePersistentBufferChunkAllocationInflight = false;
        struct TPersistentBufferToDiskWriteInFlight {
            TActorId Sender;
            ui64 Cookie;
            TActorId Session;
            ui32 OffsetInBytes;
            ui32 Size;
            std::set<ui64> WriteCookies;
            std::vector<TPersistentBufferSectorInfo> Sectors;
            TRope Data;
            NWilson::TSpan Span;
        };
        std::map<std::tuple<ui64, ui64, ui64>, TPersistentBufferToDiskWriteInFlight> PersistentBufferWriteInflight;

        struct TPersistentBufferDiskOperationInFlight {
            TActorId Sender;
            ui64 Cookie;
            TActorId Session;
            NWilson::TSpan Span;
            std::set<ui64> OperationCookies;
        };

        std::map<ui64, TPersistentBufferDiskOperationInFlight> PersistentBufferDiskOperationInflight;

        ui32 PersistentBufferRestoreChunksInflight = 0;

        TPersistentBufferSpaceAllocator PersistentBufferSpaceAllocator;

        ui64 PersistentBufferChunkMapSnapshotLsn = Max<ui64>();
        std::queue<TPendingEvent> PendingPersistentBufferEvents;
        bool PersistentBufferReady = false;

        std::unordered_map<ui64, std::vector<ui64>> PersistentBufferSectorsChecksum;
        std::unordered_set<ui32> PersistentBufferAllocatedChunks;
        std::unordered_set<ui32> PersistentBufferRestoredChunks;

        void InitPersistentBuffer(NPDisk::TPersistentBufferFormatPtr&& format);
        void IssuePersistentBufferChunkAllocation();
        void ProcessPersistentBufferQueue();
        std::vector<std::tuple<ui32, ui32, TRope>> SlicePersistentBuffer(ui64 tabletId, ui64 vchunkIndex, ui64 lsn, ui32 offsetInBytes, ui32 size, TRope&& data, const std::vector<TPersistentBufferSectorInfo>& sectors);
        void StartRestorePersistentBuffer(ui32 pos = 0);
        void GetPersistentBufferRecordData(TDDiskActor::TPersistentBuffer::TRecord& pr, std::function<void(TRope data)> callback);
        void ProcessPersistentBufferWrite(TEvWritePersistentBuffer::TPtr ev);
        double GetPersistentBufferFreeSpace();

        void Handle(TEvWritePersistentBuffer::TPtr ev);
        void Handle(TEvReadPersistentBuffer::TPtr ev);
        void Handle(TEvErasePersistentBuffer::TPtr ev);
        void Handle(TEvBatchErasePersistentBuffer::TPtr ev);
        void Handle(TEvWriteResult::TPtr ev);
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void Handle(TEvListPersistentBuffer::TPtr ev);

        void HandleWriteInFlight(ui64 cookie, const std::function<std::unique_ptr<IEventBase>()>& factory);
    };

} // NKikimr::NDDisk
