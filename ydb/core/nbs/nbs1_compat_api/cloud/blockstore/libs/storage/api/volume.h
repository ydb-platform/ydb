#pragma once

#include "events.h"

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/protos/volume.pb.h>

namespace NYdb::NBS::NNbs1CompatApi::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The NBS1 TEvVolume requests that nbsd sends to a volume tablet over a tablet
// pipe. The ids repeat cloud/blockstore/libs/storage/api/volume.h of NBS1.
struct TEvVolume
{
    enum EEvents
    {
        EvBegin = TBlockStoreEvents::VOLUME_START,

        EvWaitReadyRequest = EvBegin + 9,
        EvWaitReadyResponse = EvBegin + 10,
    };

    static_assert(
        EvWaitReadyRequest == TBlockStoreEvents::START + 313,
        "EvWaitReadyRequest expected to be == START + 313");
    static_assert(
        EvWaitReadyResponse == TBlockStoreEvents::START + 314,
        "EvWaitReadyResponse expected to be == START + 314");

    using TEvWaitReadyRequest = NYdb::NBS::NBlockStore::
        TProtoRequestEvent<NProto::TWaitReadyRequest, EvWaitReadyRequest>;

    using TEvWaitReadyResponse = NYdb::NBS::NBlockStore::
        TProtoResponseEvent<NProto::TWaitReadyResponse, EvWaitReadyResponse>;
};

}   // namespace NYdb::NBS::NNbs1CompatApi::NBlockStore
