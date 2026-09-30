#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/base/events.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos/volume.pb.h>

namespace NYdb::NBS::NNbs1CompatApi::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The NBS1 TEvService requests that nbsd sends to a volume tablet over a tablet
// pipe. The ids repeat cloud/blockstore/libs/storage/api/service.h of NBS1.
//
// ES_BLOCKSTORE is the event space of NBS1 in ydb/core/base/events.h. NBS1
// splits the space between the components of BLOCKSTORE_ACTORS
// (cloud/blockstore/libs/kikimr/components.h): each one owns 101 ids starting
// one past the space begin. These events belong to TEvService, whose component
// SERVICE is the second one, so its ids start at
// EventSpaceBegin(ES_BLOCKSTORE) + 1 + 1 * 101 = + 102.
struct TEvService
{
    enum EEvents
    {
        SERVICE_START =
            EventSpaceBegin(NKikimr::TKikimrEvents::ES_BLOCKSTORE) + 102,

        EvStatVolumeRequest = SERVICE_START + 9,
        EvStatVolumeResponse = SERVICE_START + 10,
    };

    static_assert(
        EvStatVolumeRequest ==
            EventSpaceBegin(NKikimr::TKikimrEvents::ES_BLOCKSTORE) + 111,
        "EvStatVolumeRequest expected to be == EventSpaceBegin(ES_BLOCKSTORE) "
        "+ 111");
    static_assert(
        EvStatVolumeResponse ==
            EventSpaceBegin(NKikimr::TKikimrEvents::ES_BLOCKSTORE) + 112,
        "EvStatVolumeResponse expected to be == EventSpaceBegin(ES_BLOCKSTORE) "
        "+ 112");

    using TEvStatVolumeRequest = NYdb::NBS::NBlockStore::
        TProtoRequestEvent<NProto::TStatVolumeRequest, EvStatVolumeRequest>;

    using TEvStatVolumeResponse = NYdb::NBS::NBlockStore::
        TProtoResponseEvent<NProto::TStatVolumeResponse, EvStatVolumeResponse>;
};

}   // namespace NYdb::NBS::NNbs1CompatApi::NBlockStore
