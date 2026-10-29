#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>
#include <array>
#include <optional>

namespace NKikimr::NBsController {

using TFailRealmStates = std::array<EMirror3dcFailRealmState, 3>;

// The local target follows D; a recovering realm is used only when D needs it.
std::optional<EMirror3dcFailRealmState> GetReplicatedRealmState(const TFailRealmStates& states, ui32 realm);

struct TGroupReplicationInfo {
    ui32 Generation = 0;
    TFailRealmStates States;
    ui32 RecoveryHoldMask = 0;
    struct TVDisk {
        TVDiskID Id;
        TActorId ActorId;
    };
    TVector<TVDisk> VDisks;
};

struct TEvGroupState {
    enum EEv {
        EvUpdate = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 1000,
    };

    struct TEvUpdate : TEventLocal<TEvUpdate, EvUpdate> {
        ui32 GroupId;
        std::optional<TGroupReplicationInfo> Info;
        explicit TEvUpdate(ui32 groupId) : GroupId(groupId) {}
    };
};

IActor* CreateGroupStateActor(const TActorId& controllerId);

} // namespace NKikimr::NBsController
