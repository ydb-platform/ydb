#pragma once

#include "defs.h"

#include "ddisk.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>

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

    public:
        TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters);
        void Bootstrap();
        STFUNC(StateFunc);
        void PassAway() override;

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
