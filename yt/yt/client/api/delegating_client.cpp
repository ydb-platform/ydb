#include "delegating_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TDelegatingClient::TDelegatingClient(IClientPtr underlying)
    : Underlying_(std::move(underlying))
{ }

////////////////////////////////////////////////////////////////////////////////

// Ensure that delegating client contains implementations for all
// methods of IClient. Tthis reduces the number of PR iterations you need to
// find that some out-of-yt/yt implementation of IClient does not compile.
void InstantiateDelegatingClient()
{
    Y_UNUSED(New<TDelegatingClient>(/*client*/ nullptr));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
