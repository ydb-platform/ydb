#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>
#include <array>
#include <optional>

namespace NKikimr::NBsController {

using TFailRealmStates = std::array<EMirror3dcFailRealmState, 3>;

// Query only realms selected by the authoritative group state's replication policy.
std::optional<EMirror3dcFailRealmState> GetReplicatedRealmState(
    const TMirror3dcGroupConfiguration& config, ui32 realm);

struct TGroupReplicationInfo {
    ui32 Generation = 0;
    TFailRealmStates States;
    std::optional<EMirror3dcGroupState> GroupState;
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
