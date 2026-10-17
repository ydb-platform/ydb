#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/unittests/lib/test_service.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>

namespace NYT::NRpc {
namespace {

////////////////////////////////////////////////////////////////////////////////

class THeavyTestProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(THeavyTestProxy, TestService);

    DEFINE_RPC_PROXY_METHOD(NTestRpc, SomeCall);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, PassCall,
        .SetRequestHeavy(true));
    DEFINE_RPC_PROXY_METHOD(NTestRpc, AllocationCall,
        .SetResponseHeavy(true));
};

TEST(TMethodDescriptorTest, HeavyFlagsReachRequest)
{
    THeavyTestProxy proxy(CreateLocalChannel(CreateLocalServer()));

    auto light = proxy.SomeCall();
    EXPECT_FALSE(light->GetRequestHeavy());
    EXPECT_FALSE(light->GetResponseHeavy());

    auto requestHeavy = proxy.PassCall();
    EXPECT_TRUE(requestHeavy->GetRequestHeavy());
    EXPECT_FALSE(requestHeavy->GetResponseHeavy());

    auto responseHeavy = proxy.AllocationCall();
    EXPECT_FALSE(responseHeavy->GetRequestHeavy());
    EXPECT_TRUE(responseHeavy->GetResponseHeavy());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
