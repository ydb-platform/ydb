#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_key.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/dialer.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/private.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/rpc/grpc/dispatcher.h>

#include <yt/yt/core/crypto/config.h>
#include <yt/yt/core/crypto/tls.h>

#include <util/system/env.h>

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

        Context->AddCertificate(GetTestKeyContent("cert.pem"));
        Context->AddPrivateKey(GetTestKeyContent("key.pem"));
        Context->AddCertificateAuthority(GetTestKeyContent("ca.pem"));
        Context->Commit();

        Poller = CreateThreadPoolPoller(2, "TlsTest");
    }

    ~TTlsTest()
    {
        Poller->Shutdown();
    }

    void PingPong()
    {
        auto localhost = TNetworkAddress::CreateIPv6Loopback(0);
        Listener = Context->CreateListener(localhost, Poller, Poller);

        auto config = New<TDialerConfig>();
        config->SetDefaults();
        Dialer = Context->CreateDialer(config, Poller, NetLogger());

        auto context = New<TDialerContext>();
        context->Host = "localhost";

        auto asyncFirstSide = Dialer->Dial(Listener->GetAddress(), context);
        auto asyncSecondSide = Listener->Accept();

        auto firstSide = WaitForFast(asyncFirstSide).ValueOrThrow();
        auto secondSide = WaitForFast(asyncSecondSide).ValueOrThrow();

        auto buffer = TSharedRef::FromString(std::string("ping"));
        auto outputBuffer = TSharedMutableRef::Allocate(4);

        auto result = WaitForFast(firstSide->Write(buffer));
        ASSERT_EQ(WaitForFast(secondSide->Read(outputBuffer)).ValueOrThrow(), 4u);
        result.ThrowOnError();
        ASSERT_EQ(ToString(outputBuffer), ToString(buffer));

        WaitForFast(secondSide->Write(buffer)).ThrowOnError();
        ASSERT_EQ(WaitForFast(firstSide->Read(outputBuffer)).ValueOrThrow(), 4u);
        ASSERT_EQ(ToString(outputBuffer), ToString(buffer));

        WaitFor(firstSide->Close())
            .ThrowOnError();
        ASSERT_EQ(WaitForFast(secondSide->Read(outputBuffer)).ValueOrThrow(), 0u);
    }

    NRpc::NGrpc::TGrpcLibraryLockPtr GrpcLock;
    TSslContextPtr Context;
    IPollerPtr Poller;
    IListenerPtr Listener;
    IDialerPtr Dialer;
};

TEST_F(TTlsTest, CreateContext)
{
    // Not that trivial as it seems!
}

TEST_F(TTlsTest, CreateSslObject)
{
    auto ssl = Context->NewSsl();
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
    auto dialer = Context->CreateDialer(config, Poller, NetLogger());
}

TEST_F(TTlsTest, SimplePingPong)
{
    PingPong();
}

TEST_F(TTlsTest, LoadCertificatesFromValues)
{
    auto config = New<TSslContextConfig>();
    config->CertificateAuthority = CreateTestKeyBlob("ca.pem");
    config->CertificateChain = CreateTestKeyBlob("cert.pem");
    config->PrivateKey = CreateTestKeyBlob("key.pem");
    Context->Reset();
    Context->ApplyConfig(config);
    Context->Commit();
    PingPong();
}

TEST_F(TTlsTest, LoadCertificatesFromFiles)
{
    auto config = New<TSslContextConfig>();
    config->CertificateAuthority = CreateTestKeyFile("ca.pem");
    config->CertificateChain = CreateTestKeyFile("cert.pem");
    config->PrivateKey = CreateTestKeyFile("key.pem");
    Context->Reset();
    Context->ApplyConfig(config);
    Context->Commit();
    PingPong();
}

TEST_F(TTlsTest, LoadBuiltInCertificateAuthorityForEmptyConfig)
{
    UnsetEnv("SSL_CERT_FILE");
    Context->Reset();
    Context->ApplyConfig(nullptr);
    Context->AddCertificate(GetTestKeyContent("cert.pem"));
    Context->AddPrivateKey(GetTestKeyContent("key.pem"));
    Context->Commit();
    EXPECT_THROW_WITH_SUBSTRING(PingPong(), "SSL_do_handshake failed");
}

TEST_F(TTlsTest, LoadBuiltInCertificateAuthorityAsDefault)
{
    UnsetEnv("SSL_CERT_FILE");
    auto config = New<TSslContextConfig>();
    config->CertificateChain = CreateTestKeyFile("cert.pem");
    config->PrivateKey = CreateTestKeyFile("key.pem");
    Context->Reset();
    Context->ApplyConfig(config);
    Context->Commit();
    EXPECT_THROW_WITH_SUBSTRING(PingPong(), "SSL_do_handshake failed");
}

TEST_F(TTlsTest, LoadEnvironmentCertificateAuthorityForEmptyConfig)
{
    auto ca = CreateTestKeyFile("ca.pem");
    SetEnv("SSL_CERT_FILE", TString(*ca->FileName));
    Context->Reset();
    Context->ApplyConfig(nullptr);
    Context->AddCertificate(GetTestKeyContent("cert.pem"));
    Context->AddPrivateKey(GetTestKeyContent("key.pem"));
    Context->Commit();
    PingPong();
    UnsetEnv("SSL_CERT_FILE");
}

TEST_F(TTlsTest, LoadEnvironmentCertificateAuthorityAsDefault)
{
    auto ca = CreateTestKeyFile("ca.pem");
    SetEnv("SSL_CERT_FILE", TString(*ca->FileName));
    auto config = New<TSslContextConfig>();
    config->CertificateChain = CreateTestKeyFile("cert.pem");
    config->PrivateKey = CreateTestKeyFile("key.pem");
    Context->Reset();
    Context->ApplyConfig(config);
    Context->Commit();
    PingPong();
    UnsetEnv("SSL_CERT_FILE");
}

TEST(TTlsTestWithoutFixtureTest, LoadCertificateChain)
{
    auto grpcLock = NRpc::NGrpc::TDispatcher::Get()->GetLibraryLock();
    auto context = New<TSslContext>();
    auto certificateChain = GetTestKeyContent("ca.pem") + GetTestKeyContent("cert.pem");
    context->AddCertificateChain(certificateChain);
    context->Commit();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
