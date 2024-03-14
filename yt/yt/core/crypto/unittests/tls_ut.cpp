#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_key.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/dialer.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/private.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/rpc/grpc/dispatcher.h>

#include <yt/yt/core/crypto/tls.h>

namespace NYT {
namespace {

using namespace NCrypto;
using namespace NRpc;
using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTlsTest
    : public ::testing::Test
{
public:
    TTlsTest()
    {
        // Piggybacking on openssl initialization in grpc.
        GrpcLock = NRpc::NGrpc::TDispatcher::Get()->GetLibraryLock();
        Context = New<TSslContext>();

        Context->AddCertificate(TestCertificate);
        Context->AddPrivateKey(TestCertificate);
        Context->Commit();

        Poller = CreateThreadPoolPoller(2, "TlsTest");
    }

    ~TTlsTest()
    {
        Poller->Shutdown();
    }

    NRpc::NGrpc::TGrpcLibraryLockPtr GrpcLock;
    TSslContextPtr Context;
    IPollerPtr Poller;
};

TEST_F(TTlsTest, CreateContext)
{
    // Not that trivial as it seems!
}

TEST_F(TTlsTest, CreateListener)
{
    auto localhost = TNetworkAddress::CreateIPv6Loopback(0);
    auto listener = Context->CreateListener(localhost, Poller, Poller);
}

TEST_F(TTlsTest, CreateDialer)
{
    auto config = New<TDialerConfig>();
    config->SetDefaults();
    auto dialer = Context->CreateDialer(config, Poller, NetLogger);
}

TEST_F(TTlsTest, SimplePingPong)
{
    auto localhost = TNetworkAddress::CreateIPv6Loopback(0);
    auto listener = Context->CreateListener(localhost, Poller, Poller);

    auto config = New<TDialerConfig>();
    config->SetDefaults();
    auto dialer = Context->CreateDialer(config, Poller, NetLogger);

    auto context = New<TDialerContext>();
    context->Host = "localhost";

    auto asyncFirstSide = dialer->Dial(listener->GetAddress(), context);
    auto asyncSecondSide = listener->Accept();

    auto firstSide = asyncFirstSide.Get().ValueOrThrow();
    auto secondSide = asyncSecondSide.Get().ValueOrThrow();

    auto buffer = TSharedRef::FromString(TString("ping"));
    auto outputBuffer = TSharedMutableRef::Allocate(4);

    auto result = firstSide->Write(buffer).Get();
    ASSERT_EQ(secondSide->Read(outputBuffer).Get().ValueOrThrow(), 4u);
    result.ThrowOnError();
    ASSERT_EQ(ToString(outputBuffer), ToString(buffer));

    secondSide->Write(buffer).Get().ThrowOnError();
    ASSERT_EQ(firstSide->Read(outputBuffer).Get().ValueOrThrow(), 4u);
    ASSERT_EQ(ToString(outputBuffer), ToString(buffer));

    WaitFor(firstSide->Close())
        .ThrowOnError();
    ASSERT_EQ(secondSide->Read(outputBuffer).Get().ValueOrThrow(), 0u);
}

TEST(TTlsTestWithoutFixture, LoadCertificateChain)
{
    auto grpcLock = NRpc::NGrpc::TDispatcher::Get()->GetLibraryLock();
    auto context = New<TSslContext>();
    context->AddCertificateChain(TestCertificateChain);
    context->Commit();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
