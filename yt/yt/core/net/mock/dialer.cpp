#include "dialer.h"

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

TDialerMock::TDialerMock(IDialerPtr underlying)
    : Underlying_(std::move(underlying))
{
    ON_CALL(*this, Dial).WillByDefault([this] (const TNetworkAddress& address, TDialerContextPtr /*context*/) {
        return Underlying_->Dial(address);
    });
}

DECLARE_REFCOUNTED_CLASS(TDialerMock)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
