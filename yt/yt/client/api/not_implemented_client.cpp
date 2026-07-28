#include "not_implemented_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

// Ensure that TNotImplementedClient contains implementations for all
// methods of IClient. This reduces the number of PR iterations you need to
// find that some out-of-yt/yt implementation of IClient does not compile.
void InstantiateNotImplementedClient()
{
    Y_UNUSED(New<TNotImplementedClient>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
