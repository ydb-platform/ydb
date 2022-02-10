#pragma once

#include "defs.h"

#include "types.h"

namespace NKikimr::NBsController {

    struct TEvControllerUpdateSelfHealInfo : TEventLocal<TEvControllerUpdateSelfHealInfo, TEvBlobStorage::EvControllerUpdateSelfHealInfo> {
        struct TGroupContent {
            struct TVDiskInfo {
                TVSlotId Location;
                bool Faulty;
                bool Bad;
                NKikimrBlobStorage::EVDiskStatus VDiskStatus;
            };
            ui32 Generation;
            TBlobStorageGroupType Type;
            TMap<TVDiskID, TVDiskInfo> VDisks;
        };

        THashMap<TGroupId, std::optional<TGroupContent>> GroupsToUpdate; // groups with faulty groups that are changed or got faulty PDisks for the first time
        TVector<std::pair<TVDiskID, NKikimrBlobStorage::EVDiskStatus>> VDiskStatusUpdate;
    };

    // Self-heal actor's main purpose is to monitor FAULTY pdisks and to slightly move groups out of them; every move
    // should not render group unusable, also it should not exceed its fail model. It also takes into account replication
    // broker features such as only one vslot over PDisk is being replicated at a moment.
    //
    // It interacts with BS_CONTROLLER and group observer (which provides information about group state on a per-vdisk
    // basis). BS_CONTROLLER reports faulty PDisks and all involved groups in a push notification manner.
    IActor *CreateSelfHealActor(ui64 tabletId, std::shared_ptr<std::atomic_uint64_t> unreassignableGroups);

} // NKikimr::NBsController
