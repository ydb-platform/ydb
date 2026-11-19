#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>
#include <array>
#include <optional>

namespace NKikimr::NBsController {

using TFailRealmStates = std::array<EMirror3dcFailRealmState, 3>;

using TExcludedRealms = std::array<bool, 3>;

inline std::optional<TMirror3dcGroupConfiguration> NextDataCenterTransition(
        const TMirror3dcGroupConfiguration& current, const TExcludedRealms& excluded) {
    // Return available realms first when the target swaps two data centers.
    for (bool takeDown : {false, true}) {
        for (ui32 realm = 0; realm < excluded.size(); ++realm) {
            if (excluded[realm] == takeDown && IsDown(current.Realms[realm]) != takeDown) {
                if (auto next = ChangeMirror3dcRealmAvailability(current, realm, takeDown)) {
                    return next;
                }
            }
        }
    }
    return std::nullopt;
}

struct TGroupReplicationInfo {
    ui32 Generation = 0;
    std::shared_ptr<TBlobStorageGroupInfo::TTopology> Topology;
    TFailRealmStates States;
    std::optional<EMirror3dcGroupState> GroupState;
    std::optional<TExcludedRealms> TargetExcluded;
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
