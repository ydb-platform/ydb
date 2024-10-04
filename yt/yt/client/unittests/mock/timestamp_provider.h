#pragma once

#include <yt/yt/client/transaction_client/timestamp_provider.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMockTimestampProvider);

class TMockTimestampProvider
    : public ITimestampProvider
{
public:
    MOCK_METHOD(TFuture<TTimestamp>, GenerateTimestamps, (int, NObjectClient::TCellTag), (override));
    MOCK_METHOD(TTimestamp, GetLatestTimestamp, (NObjectClient::TCellTag), (override));
};

DEFINE_REFCOUNTED_TYPE(TMockTimestampProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
