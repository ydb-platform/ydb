#include "client.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ILock::Wait(TDuration timeout)
{
    return GetAcquiredFuture().GetValue(timeout);
}

void ITransaction::Detach()
{
    Y_ABORT("ITransaction::Detach() is not implemented");
}

////////////////////////////////////////////////////////////////////////////////

const TNode::TMapType& IClient::GetDynamicConfiguration(const TString& /*configProfile*/)
{
    Y_ABORT("IClient::GetDynamicConfiguration is not implemented");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
