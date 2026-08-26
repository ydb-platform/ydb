#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller.pb.h>

#include <ydb/core/base/events.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

// Offset for the DbsController tablet's private events within
// ES_NBS_V2_SERVICE, kept clear of the public TEvService event IDs.
constexpr ui32 PrivateEventsOffset = 2000;

struct TEvDbsControllerPrivate
{
    enum EEvents
    {
        EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_NBS_V2_SERVICE) +
                  PrivateEventsOffset,

        EvUpdateDDiskMapRequest,
        EvUpdateDDiskMapResponse,

        EvGetPartitionsForNodeRequest,
        EvGetPartitionsForNodeResponse,

        EvRemoveTabletDDiskMapRequest,
        EvRemoveTabletDDiskMapResponse,

        EvEnd,
    };

    BLOCKSTORE_DECLARE_PROTO_EVENTS(UpdateDDiskMap)
    BLOCKSTORE_DECLARE_PROTO_EVENTS(GetPartitionsForNode)
    BLOCKSTORE_DECLARE_PROTO_EVENTS(RemoveTabletDDiskMap)
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
