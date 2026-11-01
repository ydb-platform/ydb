#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>
#include <array>
#include <optional>

namespace NKikimr::NBsController {

using TFailRealmStates = std::array<EMirror3dcFailRealmState, 3>;

// The sustainable target follows the minimum requirements of W and D after this realm becomes normal.
std::optional<EMirror3dcFailRealmState> GetReplicatedRealmState(const TFailRealmStates& states, ui32 realm);

struct TGroupReplicationInfo {
    ui32 Generation = 0;
    TFailRealmStates States;
    struct TVDisk {
        TVDiskID Id;
        TActorId ActorId;
        ui64 ObservationEpoch = 0;
    };
    TVector<TVDisk> VDisks;
};

struct TEvGroupState {
    enum EEv {
        EvUpdate = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 1000,
        EvConfigObserved,
    };

    struct TEvUpdate : TEventLocal<TEvUpdate, EvUpdate> {
        ui32 GroupId;
        std::optional<TGroupReplicationInfo> Info;
        explicit TEvUpdate(ui32 groupId) : GroupId(groupId) {}
    };
    struct TEvConfigObserved : TEventLocal<TEvConfigObserved, EvConfigObserved> {
        ui32 GroupId;
        ui32 RequestedGeneration;
        TVDiskID VDiskId;
        TActorId ServiceId;
        ui64 ObservationEpoch;
        TEvConfigObserved(ui32 groupId, ui32 requestedGeneration, const TVDiskID& vdiskId, const TActorId& serviceId, ui64 observationEpoch)
            : GroupId(groupId), RequestedGeneration(requestedGeneration), VDiskId(vdiskId), ServiceId(serviceId)
            , ObservationEpoch(observationEpoch) {}
    };
};

IActor* CreateGroupConfigActor(const TActorId& controllerId);
IActor* CreateGroupStateActor(const TActorId& controllerId);

} // namespace NKikimr::NBsController
