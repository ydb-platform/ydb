#include "service.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

TMockServiceContext::TMockServiceContext()
{
    ON_CALL(*this, GetRequestHeader()).WillByDefault(::testing::ReturnRef(RequestHeader_));
    ON_CALL(*this, ResponseAttachments()).WillByDefault(::testing::ReturnRef(ResponseAttachments_));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
