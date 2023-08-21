#include "client_cache.h"
#include "connection.h"

namespace NYT::NApi {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCachedClient::TCachedClient(
    const TAuthenticationIdentity& identity,
    IClientPtr client)
    : TSyncCacheValueBase(identity)
    , Client_(std::move(client))
{ }

const IClientPtr& TCachedClient::GetClient()
{
    return Client_;
}

////////////////////////////////////////////////////////////////////////////////

TClientCache::TClientCache(
    TSlruCacheConfigPtr config,
    IConnectionPtr connection)
    : TSyncSlruCacheBase<TAuthenticationIdentity, TCachedClient>(std::move(config))
    , Connection_(std::move(connection))
{ }

IClientPtr TClientCache::Get(
    const TAuthenticationIdentity& identity,
    const TClientOptions& options)
{
    auto cachedClient = Find(identity);
    if (!cachedClient) {
        cachedClient = New<TCachedClient>(identity, Connection_->CreateClient(options));
        TryInsert(cachedClient, &cachedClient);
    }
    return cachedClient->GetClient();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
