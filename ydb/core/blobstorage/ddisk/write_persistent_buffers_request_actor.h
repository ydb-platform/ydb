#pragma once

#include "defs.h"

#include "ddisk.h"

namespace NKikimr::NDDisk {
    class TWritePersistentBuffersRequestActor : public TActor<TWritePersistentBuffersRequestActor> {
        struct TReadInflight {
            TActorId Sender;
            ui64 Cookie;
            ui64 TabletId;
            ui32 TabletGeneration;
            ui32 RequestGeneration;
            ui64 Lsn;
            ui32 Timeout;
            std::vector<std::tuple<ui32, ui32, ui32>> PersistentBufferIds;
        };

        struct TInflight {
            struct TPersistentBufferInflight {
                ui32 NodeId;
                ui32 PDiskId;
                ui32 DDiskSlotId;
                bool Received;
                bool Replied;
                NKikimrBlobStorage::NDDisk::TReplyStatus::E Status;
                TString ErrorReason;
                double FreeSpace;
                double PDiskNormalizedOccupancy;
            };

            TActorId Sender;
            ui64 Cookie;
            std::unordered_map<ui64, TPersistentBufferInflight> Inflights;
            ui32 Received = 0;
        };

        std::unordered_map<ui64, ui64> InflightParts;
        std::unordered_map<ui64, TInflight> Inflights;
        std::unordered_map<ui64, TReadInflight> ReadInflights;
        ui64 NextCookie = 0;
        TActorId ParentId;

        void PassAway() override;
        void Timeout(TEvents::TEvWakeup::TPtr &ev);
        void CheckReply(ui64 cookie);
        void Reply(ui64 cookie);
        void ReplyAndFinish(ui64 cookie);
        void Handle(TEvReadPersistentBufferResult::TPtr ev);
        void Handle(TEvWritePersistentBufferResult::TPtr ev);
        void Handle(TEvWritePersistentBuffers::TPtr ev);
        void Handle(TEvReadThenWritePersistentBuffers::TPtr ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev);

    public:
        TWritePersistentBuffersRequestActor(TActorId parentId);

        STFUNC(StateFunc);
    };
} // NKikimr::NDDisk

