#include "cache_base.h"

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

NApi::IClientPtr TClientsCacheBase::GetClient(TStringBuf clusterUrl)
{
    {
        auto guard = ReaderGuard(Lock_);
        auto clientIt = Clients_.find(clusterUrl);
        if (clientIt != Clients_.end()) {
            return clientIt->second;
        }
    }

    auto client = CreateClient(clusterUrl);

    {
        auto guard = WriterGuard(Lock_);
        return Clients_.try_emplace(clusterUrl, client).first->second;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
