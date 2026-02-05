#pragma once

#include "defs.h"

#include "ddisk.h"
#include "persistent_buffer_space_allocator.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <queue>

namespace NKikimrBlobStorage::NDDisk::NInternal {
    class TChunkMapLogRecord;
    class TPersistentBufferChunkMapLogRecord;
}

#define LIST_COUNTERS_INTERFACE_OPS(XX) \
    XX(Write) \
    XX(Read) \
    XX(WritePersistentBuffer) \
    XX(ReadPersistentBuffer) \
    XX(FlushPersistentBuffer) \
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
                std::decay_t<decltype(std::declval<T>().GetWriteInstruction())>,
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

    private:
        struct {
            struct {
#define DECLARE_COUNTERS_INTERFACE(NAME) \
                struct { \
                    NMonitoring::TDynamicCounters::TCounterPtr Requests; \
                    NMonitoring::TDynamicCounters::TCounterPtr ReplyOk; \
                    NMonitoring::TDynamicCounters::TCounterPtr ReplyErr; \
                    NMonitoring::TDynamicCounters::TCounterPtr Bytes; \
                    \
                    void Request(ui32 bytes = 0) { \
                        ++*Requests; \
                        if (bytes) { \
                            *Bytes += bytes; \
                        } \
                    } \
                    \
                    void Reply(bool ok, ui32 bytes = 0) { \
                        ++*(ok ? ReplyOk : ReplyErr); \
                        if (bytes) { \
                            *Bytes += bytes; \
                        } \
                    } \
                } NAME;

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
        } Counters;

    private:
        struct TEvPrivate {
            enum {
                EvHandleSingleQuery = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvHandleEventForChunk,
                EvHandlePersistentBufferEventForChunk,
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
                ui64 ChunkIndex;

                TEvHandlePersistentBufferEventForChunk(ui64 chunkIndex)
                    : ChunkIndex(chunkIndex)
                {}
            };
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
        THashMap<ui64, std::tuple<NWilson::TSpan, std::function<void(NPDisk::TEvChunkWriteRawResult&, NWilson::TSpan&&)>>> WriteCallbacks;
        THashMap<ui64, std::tuple<NWilson::TSpan, std::function<void(NPDisk::TEvChunkReadRawResult&, NWilson::TSpan&&)>>> ReadCallbacks;

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

        NKikimrBlobStorage::NDDisk::NInternal::TPersistentBufferChunkMapLogRecord CreatePersistentBufferChunkMapSnapshot();
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
                        "offset and must must be multiple of block size and size must be nonzero"));
                    registerError();
                    return false;
                }

                if constexpr (NPrivate::THasWriteInstructionField<TRecord>::value) {
                    const TWriteInstruction instruction(record.GetWriteInstruction());
                    size_t size = 0;
                    if (instruction.PayloadId) {
                        const TRope& data = ev.Get()->GetPayload(*instruction.PayloadId);
                        size = data.size();
                    }
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

        void Handle(TEvWrite::TPtr ev);
        void Handle(TEvRead::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Persistent buffer services
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TPersistentBuffer {
            struct TRecord {
                ui32 OffsetInBytes;
                ui32 Size;
                TRope Data;
            };

            std::map<ui64, TRecord> Records;
        };

        std::map<std::tuple<ui64, ui64>, TPersistentBuffer> PersistentBuffers;

        static constexpr ui32 SectorSize = 4096;
        static constexpr ui32 SectorInChunk = 32768;
        static constexpr ui32 ChunkSize = SectorSize * SectorInChunk;
        static constexpr ui32 MaxChunks = 128;
        static constexpr ui32 MaxSectorsPerBuffer = 128;


        struct TPersistentBufferHeader {
            static constexpr ui64 PersistentBufferHeaderSignature[2] = {17823859641143956470ull, 2161636001838356059ull};

            ui64 Signature[2];
            ui64 TabletId;
            ui64 VChunkIndex;
            ui32 OffsetInBytes;
            ui32 Size;
            ui64 Lsn;
            TPersistentBufferSectorInfo HeaderLocation;
            TPersistentBufferSectorInfo Locations[MaxSectorsPerBuffer];
        };
        static_assert(sizeof(TPersistentBufferHeader) <= ChunkSize);

        bool IssuePersistentBufferChunkAllocationInflight = false;
        struct TPersistentBufferToDiskWriteInFlight {
            TActorId Sender;
            ui64 Cookie;
            TActorId Session;
            ui32 OffsetInBytes;
            ui32 Size;
            std::set<ui64> WriteCookies;
            TRope Data;
        };
        std::map<std::tuple<ui64, ui64, ui64>, TPersistentBufferToDiskWriteInFlight> PersistentBufferWriteInflight;
        TPersistentBufferSpaceAllocator PersistentBufferSpaceAllocator;

        ui64 PersistentBufferChunkMapSnapshotLsn = Max<ui64>();
        std::queue<TPendingEvent> PendingPersistentBufferEvents;

        void IssuePersistentBufferChunkAllocation();
        void ProcessPersistentBufferQueue();
        std::vector<std::tuple<ui32, ui32, ui32, TRope>> SlicePersistentBuffer(ui64 tabletId, ui64 vchunkIndex, ui64 lsn, ui32 offsetInBytes, ui32 size, TRope&& data, const std::vector<TPersistentBufferSectorInfo>& sectors);

        struct TWriteInFlight {
            TActorId Sender;
            ui64 Cookie;
            TActorId InterconnectionSessionId;
            NWilson::TSpan Span;
            ui32 Size;
        };

        ui64 NextWriteCookie = 1;
        THashMap<ui64, TWriteInFlight> WritesInFlight;

        void Handle(TEvWritePersistentBuffer::TPtr ev);
        void Handle(TEvReadPersistentBuffer::TPtr ev);
        void Handle(TEvFlushPersistentBuffer::TPtr ev);
        void Handle(TEvErasePersistentBuffer::TPtr ev);
        void Handle(TEvWriteResult::TPtr ev);
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void Handle(TEvListPersistentBuffer::TPtr ev);
        void HandleWriteInFlight(ui64 cookie, const std::function<std::unique_ptr<IEventBase>()>& factory);
    };

} // NKikimr::NDDisk
