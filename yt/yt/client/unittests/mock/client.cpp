#include "client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

const NTabletClient::ITableMountCachePtr& TMockClient::GetTableMountCache()
{
    return TableMountCache_;
}

void TMockClient::SetTableMountCache(NTabletClient::ITableMountCachePtr value)
{
    TableMountCache_ = std::move(value);
}

const NTransactionClient::ITimestampProviderPtr& TMockClient::GetTimestampProvider()
{
    return TimestampProvider_;
}

void TMockClient::SetTimestampProvider(NTransactionClient::ITimestampProviderPtr value)
{
    TimestampProvider_ = std::move(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
