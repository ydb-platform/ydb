#pragma once

#include "private.h"

#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/core/rpc/authentication_identity.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TClientAuthenticationIdentity
    : public NRpc::TAuthenticationIdentity
{
    TClientAuthenticationIdentity() = default;
    explicit TClientAuthenticationIdentity(
        const std::string& user,
        const std::string& userTag = {},
        const std::string& serviceTicket = {});

    std::string ServiceTicket;
};

class TCachedClient
    : public TSyncCacheValueBase<TClientAuthenticationIdentity, TCachedClient>
{
public:
    TCachedClient(
        const TClientAuthenticationIdentity& identity,
        IClientPtr client);

    const IClientPtr& GetClient();

private:
    const IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

//! An SLRU-cache based class for keeping a cache of clients for different users.
/*
 *  For NApi::NNative::IClient equivalent see ytlib/api/native/client_cache.h.
 *
 *  Cache is completely thread-safe.
 */
class TClientCache
    : public TSyncSlruCacheBase<TClientAuthenticationIdentity, TCachedClient>
{
public:
    TClientCache(
        TSlruCacheConfigPtr config,
        IConnectionPtr connection);

    IClientPtr Get(
        const TClientAuthenticationIdentity& identity,
        const TClientOptions& options);

    IClientPtr Get(
        const NRpc::TAuthenticationIdentity& identity,
        const TClientOptions& options);

private:
    // TODO(max42): shouldn't this be TWeakPtr?
    const IConnectionPtr Connection_;
};

DEFINE_REFCOUNTED_TYPE(TClientCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi


template <>
struct THash<NYT::NApi::TClientAuthenticationIdentity>
{
    size_t operator()(const NYT::NApi::TClientAuthenticationIdentity& value) const;
};

