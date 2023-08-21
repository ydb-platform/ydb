#include "connection.h"
#include "connection_impl.h"

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    TConnectionOptions options)
{
    return New<TConnection>(std::move(config), std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

