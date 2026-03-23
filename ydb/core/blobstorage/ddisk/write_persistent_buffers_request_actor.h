#pragma once

#include "defs.h"

#include "ddisk.h"

namespace NKikimr::NDDisk {
    class TWritePersistentBuffersRequestActor : public TActor<TWritePersistentBuffersRequestActor> {
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
        ui64 NextCookie = 0;

        void PassAway() override;
        void Timeout(TEvents::TEvWakeup::TPtr &ev);
        void CheckReply(ui64 cookie);
        void Reply(ui64 cookie);
        void ReplyAndFinish(ui64 cookie);
        void Handle(TEvWritePersistentBufferResult::TPtr ev);
        void Handle(TEvWritePersistentBuffers::TPtr ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev);

    public:
        TWritePersistentBuffersRequestActor();

        STFUNC(StateFunc);
    };
} // NKikimr::NDDisk

