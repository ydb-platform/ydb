#include "ss_proxy.h"

#include "ss_proxy_actor.h"

namespace NYdb::NBS::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateSSProxy(const NProto::TStorageServiceConfig& nbsStorageConfig)
{
    // if (config->GetSSProxyFallbackMode()) {
    //     return std::make_unique<TSSProxyFallbackActor>(std::move(config));
    // }

    return std::make_unique<TSSProxyActor>(nbsStorageConfig);
}

}   // namespace NYdb::NBS::NStorage
