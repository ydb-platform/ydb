#pragma once

#include "public.h"
#include "replication_card.h"

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardCacheKey
{
    TReplicationCardId CardId;
    TReplicationCardFetchOptions FetchOptions;
    TReplicationEra RefreshEra = InvalidReplicationEra;

    operator size_t() const;
    bool operator == (const TReplicationCardCacheKey& other) const;
};

void FormatValue(TStringBuilderBase* builder, const TReplicationCardCacheKey& key, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

struct IReplicationCardCache
    : public virtual TRefCounted
{
    virtual TFuture<TReplicationCardPtr> GetReplicationCard(const TReplicationCardCacheKey& key) = 0;
    virtual void ForceRefresh(const TReplicationCardCacheKey& key, const TReplicationCardPtr& replicationCard) = 0;
    virtual void Clear() = 0;
    virtual void Reconfigure(const TReplicationCardCacheDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicationCardCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

