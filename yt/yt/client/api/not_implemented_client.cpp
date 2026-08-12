#include "not_implemented_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

namespace {

// Ensure that TNotImplementedClient contains implementations for all
// methods of IClient. This reduces the number of PR iterations you need to
// find that some out-of-yt/yt implementation of IClient does not compile.
[[maybe_unused]] void InstantiateNotImplementedClient()
{
    Y_UNUSED(New<TNotImplementedClient>());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
