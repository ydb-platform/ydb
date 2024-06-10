#include "connection_reuse_helpers.h"

#include "connection_pool.h"

#include <yt/yt/core/net/connection.h>

namespace NYT::NHttp::NDetail {

////////////////////////////////////////////////////////////////////////////////

TReusableConnectionState::TReusableConnectionState(
    NNet::IConnectionPtr connection,
    TConnectionPoolPtr owningPool)
    : Connection(std::move(connection))
    , OwningPool(std::move(owningPool))
{ }

TReusableConnectionState::~TReusableConnectionState()
{
    if (Reusable && OwningPool && Connection->IsIdle()) {
        OwningPool->Release(std::move(Connection));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp::NDetail
