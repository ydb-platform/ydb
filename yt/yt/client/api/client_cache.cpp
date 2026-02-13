#include "client_cache.h"
#include "connection.h"

namespace NYT::NApi {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TClientAuthenticationIdentity::TClientAuthenticationIdentity(
    const std::string& user,
    const std::string& userTag,
    const std::string& serviceTicket)
    : NRpc::TAuthenticationIdentity(user, userTag)
    , ServiceTicket(serviceTicket)
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedClient::TCachedClient(
    const TClientAuthenticationIdentity& identity,
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
    : TSyncSlruCacheBase<TClientAuthenticationIdentity, TCachedClient>(std::move(config))
    , Connection_(std::move(connection))
{ }

IClientPtr TClientCache::Get(
    const TClientAuthenticationIdentity& identity,
    const TClientOptions& options)
{
    auto cachedClient = Find(identity);
    if (!cachedClient) {
        cachedClient = New<TCachedClient>(identity, Connection_->CreateClient(options));
        TryInsert(cachedClient, &cachedClient);
    }
    return cachedClient->GetClient();
}

IClientPtr TClientCache::Get(
    const NRpc::TAuthenticationIdentity& identity,
    const TClientOptions& options)
{
    return Get(
        TClientAuthenticationIdentity(
            identity.User,
            identity.UserTag,
            ""),
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

size_t THash<NYT::NApi::TClientAuthenticationIdentity>::operator()(const NYT::NApi::TClientAuthenticationIdentity& value) const
{
    size_t result = 0;
    result = THash<NYT::NRpc::TAuthenticationIdentity>()(value);
    NYT::HashCombine(result, value.ServiceTicket);
    return result;
}
