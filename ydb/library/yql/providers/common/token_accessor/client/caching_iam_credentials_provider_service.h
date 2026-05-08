#pragma once

#include <ydb/library/actors/core/actor.h>
#include <util/generic/fwd.h>

namespace NYql {

    std::unique_ptr<NActors::IActor> NewCachingIamServiceCredentialsProviderService();
    NActors::TActorId MakeCachingIamServiceCredentialsProviderServiceId();
}
