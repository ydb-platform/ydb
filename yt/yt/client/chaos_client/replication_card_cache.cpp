#include "replication_card_cache.h"

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/guid.h>

#include <util/digest/multi.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

TReplicationCardCacheKey::operator size_t() const
{
    return MultiHash(
        CardId,
        FetchOptions);
}

bool TReplicationCardCacheKey::operator == (const TReplicationCardCacheKey& other) const
{
    return CardId == other.CardId && FetchOptions == other.FetchOptions;
}

void FormatValue(TStringBuilderBase* builder, const TReplicationCardCacheKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("{CardId: %v, FetchOptions: %v}",
        key.CardId,
        key.FetchOptions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
