#pragma once

#include <yt/yt/core/net/dialer.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

class TDialerMock
    : public IDialer
{
public:
    explicit TDialerMock(IDialerPtr underlying);

    MOCK_METHOD(TFuture<IConnectionPtr>, Dial, (const TNetworkAddress& remote, TDialerContextPtr context), (override));

private:
    const IDialerPtr Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TDialerMock)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
