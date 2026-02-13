#include "noop_timestamp_provider.h"

#include "private.h"
#include "timestamp_provider.h"

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TNoopTimestampProvider
    : public ITimestampProvider
{
public:
    TFuture<TTimestamp> GenerateTimestamps(int /*count*/, NObjectClient::TCellTag /*clockClusterTag*/) override
    {
        return MakeFuture(NullTimestamp);
    }

    TTimestamp GetLatestTimestamp(NObjectClient::TCellTag /*clockClusterTag*/) override
    {
        return NullTimestamp;
    }

    void Reconfigure(const TRemoteTimestampProviderConfigPtr& /*config*/) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateNoopTimestampProvider()
{
    return New<TNoopTimestampProvider>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
