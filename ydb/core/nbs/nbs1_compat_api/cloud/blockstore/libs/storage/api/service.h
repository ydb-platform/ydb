#pragma once

#include "events.h"

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos/volume.pb.h>

namespace NYdb::NBS::NNbs1CompatApi::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The NBS1 TEvService requests that nbsd sends to a volume tablet over a tablet
// pipe. The ids repeat cloud/blockstore/libs/storage/api/service.h of NBS1.
struct TEvService
{
    enum EEvents
    {
        EvBegin = TBlockStoreEvents::SERVICE_START,

        EvStatVolumeRequest = EvBegin + 9,
        EvStatVolumeResponse = EvBegin + 10,
    };

    static_assert(
        EvStatVolumeRequest == TBlockStoreEvents::START + 111,
        "EvStatVolumeRequest expected to be == START + 111");
    static_assert(
        EvStatVolumeResponse == TBlockStoreEvents::START + 112,
        "EvStatVolumeResponse expected to be == START + 112");

    using TEvStatVolumeRequest = NYdb::NBS::NBlockStore::
        TProtoRequestEvent<NProto::TStatVolumeRequest, EvStatVolumeRequest>;

    using TEvStatVolumeResponse = NYdb::NBS::NBlockStore::
        TProtoResponseEvent<NProto::TStatVolumeResponse, EvStatVolumeResponse>;
};

}   // namespace NYdb::NBS::NNbs1CompatApi::NBlockStore
