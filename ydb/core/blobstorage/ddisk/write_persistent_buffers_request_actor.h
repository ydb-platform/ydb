#pragma once

#include "defs.h"

#include "ddisk.h"

namespace NKikimr::NDDisk {
    class TWritePersistentBuffersRequestActor : public TActor<TWritePersistentBuffersRequestActor> {
        struct TPersistentBufferInflight {
            ui32 NodeId;
            ui32 PDiskId;
            ui32 DDiskSlotId;
            bool Received;
            bool Replied;
            NKikimrBlobStorage::NDDisk::TReplyStatus::E Status;
            TString ErrorReason;
            double FreeSpace;
        };

        TActorId Sender;
        ui64 Cookie;
        std::vector<TPersistentBufferInflight> Inflights;
        ui32 Received = 0;


        void PassAway() override;
        void Timeout();
        void CheckReply();
        void Reply();
        void ReplyAndDie();
        void Handle(TEvWritePersistentBufferResult::TPtr ev);
        void Handle(TEvWritePersistentBuffers::TPtr ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev);

    public:
        TWritePersistentBuffersRequestActor();

        STFUNC(StateFunc);
    };
} // NKikimr::NDDisk

