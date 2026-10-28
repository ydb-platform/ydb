#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/base/events.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/protos/volume.pb.h>

namespace NYdb::NBS::NNbs1CompatApi::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The NBS1 TEvVolume requests that nbsd sends to a volume tablet over a tablet
// pipe. The ids repeat cloud/blockstore/libs/storage/api/volume.h of NBS1.
//
// ES_BLOCKSTORE is the event space of NBS1 in ydb/core/base/events.h. NBS1
// splits the space between the components of BLOCKSTORE_ACTORS
// (cloud/blockstore/libs/kikimr/components.h): each one owns 101 ids starting
// one past the space begin. These events belong to TEvVolume, whose component
// VOLUME is the fourth one, so its ids start at
// EventSpaceBegin(ES_BLOCKSTORE) + 1 + 3 * 101 = + 304.
struct TEvVolume
{
    enum EEvents
    {
        VOLUME_START =
            EventSpaceBegin(NKikimr::TKikimrEvents::ES_BLOCKSTORE) + 304,

        EvWaitReadyRequest = VOLUME_START + 9,
        EvWaitReadyResponse = VOLUME_START + 10,
    };

    static_assert(
        EvWaitReadyRequest ==
            EventSpaceBegin(NKikimr::TKikimrEvents::ES_BLOCKSTORE) + 313,
        "EvWaitReadyRequest expected to be == EventSpaceBegin(ES_BLOCKSTORE) + "
        "313");
    static_assert(
        EvWaitReadyResponse ==
            EventSpaceBegin(NKikimr::TKikimrEvents::ES_BLOCKSTORE) + 314,
        "EvWaitReadyResponse expected to be == EventSpaceBegin(ES_BLOCKSTORE) "
        "+ 314");

    using TEvWaitReadyRequest = NYdb::NBS::NBlockStore::
        TProtoRequestEvent<NProto::TWaitReadyRequest, EvWaitReadyRequest>;

    using TEvWaitReadyResponse = NYdb::NBS::NBlockStore::
        TProtoResponseEvent<NProto::TWaitReadyResponse, EvWaitReadyResponse>;
};

}   // namespace NYdb::NBS::NNbs1CompatApi::NBlockStore
