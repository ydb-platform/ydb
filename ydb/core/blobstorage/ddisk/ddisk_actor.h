#pragma once

#include "defs.h"

#include "ddisk.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <queue>

namespace NKikimrBlobStorage::NDDisk::NInternal {
    class TChunkMapLogRecord;
}

namespace NKikimr::NDDisk {

    namespace NPrivate {
        template<typename TRecord>
        struct THasSelectorField {
            template<typename T> static constexpr auto check(T*) -> typename std::is_same<
                std::decay_t<decltype(std::declval<T>().GetSelector())>,
                NKikimrBlobStorage::TDDiskBlockSelector
            >::type;

            template<typename> static constexpr std::false_type check(...);

            static constexpr bool value = decltype(check<TRecord>(nullptr))::value;
        };

        template<typename TRecord>
        struct THasWriteInstructionField {
            template<typename T> static constexpr auto check(T*) -> typename std::is_same<
                std::decay_t<decltype(std::declval<T>().GetWriteInstruction())>,
                NKikimrBlobStorage::TDDiskWriteInstruction
            >::type;

            template<typename> static constexpr std::false_type check(...);

            static constexpr bool value = decltype(check<TRecord>(nullptr))::value;
        };
    }

    class TDDiskActor : public TActorBootstrapped<TDDiskActor> {
        TString DDiskId;
        TVDiskConfig::TBaseInfo BaseInfo;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
        ui64 DDiskInstanceGuid = RandomNumber<ui64>();

        static constexpr ui32 BlockSize = 4096;

    private:
        struct TEvPrivate {
            enum {
                EvHandleSingleQuery = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvHandleEventForChunk,
            };

            struct TEvHandleEventForChunk : TEventLocal<TEvHandleEventForChunk, EvHandleEventForChunk> {
                ui64 TabletId;
                ui64 VChunkIndex;

                TEvHandleEventForChunk(ui64 tabletId, ui64 vChunkIndex)
                    : TabletId(tabletId)
                    , VChunkIndex(vChunkIndex)
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

        struct TChunkRef {
            TChunkIdx ChunkIdx;
            std::queue<std::unique_ptr<IEventHandle>> PendingEventsForChunk;
        };

        THashMap<ui64, THashMap<ui64, TChunkRef>> ChunkRefs; // TabletId -> (VChunkIndex -> ChunkIdx)
        TIntrusivePtr<TPDiskParams> PDiskParams;
        std::vector<TChunkIdx> OwnedChunksOnBoot;
        ui64 ChunkMapSnapshotLsn = Max<ui64>();
        std::queue<std::unique_ptr<IEventHandle>> PendingQueries;
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
            PendingQueries.emplace(ev.Release());
            return false;
        }

        // Chunk management code

        std::queue<TChunkIdx> ChunkReserve;
        bool ReserveInFlight = false;
        std::queue<std::tuple<ui64, ui64>> ChunkAllocateQueue;
        THashMap<ui64, std::function<void()>> LogCallbacks;
        ui64 NextCookie = 1;
        THashMap<void*, std::function<void(NPDisk::TEvChunkWriteResult&)>> WriteCallbacks;
        THashMap<void*, std::function<void(NPDisk::TEvChunkReadResult&)>> ReadCallbacks;

        void IssueChunkAllocation(ui64 tabletId, ui64 vChunkIndex);
        void Handle(NPDisk::TEvChunkReserveResult::TPtr ev);
        void HandleChunkReserved();
        void Handle(NPDisk::TEvLogResult::TPtr ev);
        void Handle(TEvPrivate::TEvHandleEventForChunk::TPtr ev);

        void Handle(NPDisk::TEvCutLog::TPtr ev);

        void Handle(NPDisk::TEvChunkWriteResult::TPtr ev);
        void Handle(NPDisk::TEvChunkReadResult::TPtr ev);

        ui64 GetFirstLsnToKeep() const;

        void IssuePDiskLogRecord(TLogSignature signature, TChunkIdx chunkIdxToCommit, const NProtoBuf::Message& data,
            ui64 *startingPointLsnPtr, std::function<void()> callback);

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

        void Handle(TEvDDiskConnect::TPtr ev);
        void Handle(TEvDDiskDisconnect::TPtr ev);

        // validate query credentials against registered connections
        bool ValidateConnection(const IEventHandle& ev, const TQueryCredentials& creds) const;

        // a general way to send reply to any incoming message
        void SendReply(const IEventHandle& queryEv, std::unique_ptr<IEventBase> replyEv) const;

        // common function to validate any incoming event's credentials
        template<typename TEvent>
        bool CheckQuery(TEventHandle<TEvent>& ev) const {
            const auto& record = ev.Get()->Record;

            const TQueryCredentials creds(record.GetCredentials());
            if (!ValidateConnection(ev, creds)) {
                SendReply(ev, std::make_unique<typename TEvent::TResult>(
                    NKikimrBlobStorage::TDDiskReplyStatus::SESSION_MISMATCH));
                return false;
            }

            using TRecord = std::decay_t<decltype(record)>;

            if constexpr (NPrivate::THasSelectorField<TRecord>::value) {
                const TBlockSelector selector(record.GetSelector());
                if (selector.OffsetInBytes % BlockSize || selector.Size % BlockSize || !selector.Size) {
                    SendReply(ev, std::make_unique<typename TEvent::TResult>(
                        NKikimrBlobStorage::TDDiskReplyStatus::INCORRECT_REQUEST,
                        "offset and must must be multiple of block size and size must be nonzero"));
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
                            NKikimrBlobStorage::TDDiskReplyStatus::INCORRECT_REQUEST,
                            "declared data size must match actually sent one"));
                        return false;
                    }
                }
            }

            return true;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Read/write
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        THashMap<TString, size_t> BlockRefCount;
        THashMap<std::tuple<ui64, ui64, ui32>, const TString*> Blocks;

        void Handle(TEvDDiskWrite::TPtr ev);
        void Handle(TEvDDiskRead::TPtr ev);

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

        struct TWriteInFlight {
            TActorId Sender;
            ui64 Cookie;
            TActorId InterconnectionSessionId;
        };

        ui64 NextWriteCookie = 1;
        THashMap<ui64, TWriteInFlight> WritesInFlight;

        void Handle(TEvDDiskWritePersistentBuffer::TPtr ev);
        void Handle(TEvDDiskReadPersistentBuffer::TPtr ev);
        void Handle(TEvDDiskFlushPersistentBuffer::TPtr ev);
        void Handle(TEvDDiskWriteResult::TPtr ev);
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void Handle(TEvDDiskListPersistentBuffer::TPtr ev);
        void HandleWriteInFlight(ui64 cookie, const std::function<std::unique_ptr<IEventBase>()>& factory);
    };

} // NKikimr::NDDisk
