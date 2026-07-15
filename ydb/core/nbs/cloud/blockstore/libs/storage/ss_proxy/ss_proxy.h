#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/public.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateSSProxy(
    const NProto::TStorageServiceConfig& nbsStorageConfig);

}   // namespace NYdb::NBS::NStorage
