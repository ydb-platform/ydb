#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>
#include <yt/yt/core/bus/tcp/ssl_context.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NBus {
namespace {

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateMessage(int numParts, int partSize = 1)
{
    auto data = TSharedMutableRef::Allocate(numParts * partSize);

    std::vector<TSharedRef> parts;
    for (int i = 0; i < numParts; ++i) {
        parts.push_back(data.Slice(i * partSize, (i + 1) * partSize));
    }

    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyBusHandler
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        Y_UNUSED(message);
        Y_UNUSED(replyBus);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSslTest
    : public testing::Test
{
public:
    NTesting::TPortHolder Port;
    TString Address;

    TSslTest()
    {
        Port = NTesting::GetFreePort();
        Address = Format("localhost:%v", Port);

        LoadKeyPairIntoCxt();
    }

    void LoadKeyPairIntoCxt()
    {
        auto certChain = R"foo(-----BEGIN CERTIFICATE-----
MIIFUTCCAzmgAwIBAgIBATANBgkqhkiG9w0BAQsFADBGMQswCQYDVQQGEwJSVTEP
MA0GA1UECAwGTW9zY293MQ8wDQYDVQQKDAZZYW5kZXgxFTATBgNVBAMMDENBQGxv
Y2FsaG9zdDAeFw0yMzA3MTMxMTUxMTdaFw0zMzA3MTAxMTUxMTdaMEMxCzAJBgNV
BAYTAlJVMQ8wDQYDVQQIDAZNb3Njb3cxDzANBgNVBAoMBllhbmRleDESMBAGA1UE
AwwJbG9jYWxob3N0MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA1ZrD
+kcfmsi52zMZ7cJxWl8pMI1kX1uQ6KkOJRVQVuweJuhp35rJGK0vHkWvrpihO+Ue
h1jncReH8PaNdejt3gPsYJksUeRxCaZ8O+awtyOq6TK8L9pDzbQyXme5k2Ea/+yl
XSHrGGzZUuHaZRzBigyQXzb04ZdQMzqGGKUz52ed5elZnOoAMUL7/wZ7O8n5dlXh
DwxEQ4SQ3a39SLpPWv3hWXiIDrlR5xhkqQjZnb0WEVlSwN8lpyjA6XwCs/IX7PVr
khoBKq7664rsR3B7qhGA5cmSy4+KrSoYKYHGCvl6OakO7fn4FcJwHHFNQzMq8oa5
i5LbYsaCy6IgJt07nBeNXxaEY+4WIImHfWR0yvQgqtrFQZuIQ4N7YeZpLvTtoO3I
UJyjMsih5GX4LPY0k9nE8rd437lV3PdiBYAktkwmgCTY9uJFhCBPFsMhE30nL79t
X0gm5FugW1aZ3CFvgQywkcYQIjBjJQ42zcpqnLjj4lIwfDO5YEAiAOPqQ3VV7FKm
bSWpeXyRAL/uqkBVCDKTsKJkEjd8gc30honRIKNHQkr/RoV1M1W449uRkvfVfbfg
kdcRcYMIynTwZMSiKDoxXkQyrcyY6qVjYQNUKwRvtN2m6aGftYhfgo4j01UHTrK7
BHRHKjW5LFXgNJUXnycmO69B+w1eTRgnVE7ws6sCAwEAAaNNMEswCQYDVR0TBAIw
ADAdBgNVHQ4EFgQUNvB1ghYixkG53VZoSvUPnCCN7hwwHwYDVR0jBBgwFoAUW5Z1
bHNDpMX/IfexCeWTcQSvVWMwDQYJKoZIhvcNAQELBQADggIBAHW/+xxign61axqT
hO338rJGuidhXzraCQtJT/J6OwUaI+P6C71wUoBKEk3SrIVNG1Q8mrlsj93EzQuv
M9OvBenIomF7A2ydaFL/6NJhZ8W6PmZwYF3VvInYJ2p7zqCMjqECg2C92NIC7+Z6
fdmL+xoj3XKYypqA1x6xvSnCtXyXGRCxta3Es163wbqffq+4jjBUFOyUr9vk2N7X
vy3/x8LWHIXffzNSJLtnXiznSNBSubmedac8JQ+XE9RgK+R0kUrj1lqnkSg+tPWD
jP52kG84J9URV18BZfLFpcUiYloWXREfwNXRhMAQ6DyucupLW1Skl+Nf9K+41C3h
f4mDn4Axn6toBzav9NLdFelGVAp4R3Yjoiv2LvpwYxGnMs/cPJMGm/NqoAbuyOMY
ZKZPWvwsbxeaG8u9GRGvTSpawnGWJqIgxknKpQT5QztjNwtI9iT0/f4n3CPEGdAi
6bHw0q+jiCKmXZMZPHyJ/tSJ74H7tdeWYYjhthJWnrAz4BziZ+bFBLkcYq95VV4L
pOeMUr0PUi8fpSK06wIVTES9AWgcfuXL6i7AQ+hadEa3Ve1BGRsu0KOXW2XZZFeo
3Pczm/o+jwMiLELcgrM5Ngy8dcCKr6v84F+fi9Y+C8+RZ7g37aLJM0kqbaoN8owL
mP88f9xDGRAmesvuYlHo+57VTyzU
-----END CERTIFICATE-----)foo";
        TSslContext::Get()->UseCertificateChain(certChain);

        auto privateKey = R"foo(-----BEGIN PRIVATE KEY-----
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
rPl77gAcribJm3TzBVHm2m6jBGtb
-----END PRIVATE KEY-----)foo";
        TSslContext::Get()->UsePrivateKey(privateKey);
        TSslContext::Get()->CheckPrivateKeyWithCertificate();
    }

    IBusServerPtr StartBusServer(IMessageHandlerPtr handler)
    {
        auto config = TBusServerConfig::CreateTcp(Port);
        config->EncryptionMode = EEncryptionMode::Required;
        config->UseKeyPairFromSslContext = true;
        auto server = CreateBusServer(config);
        server->Start(handler);
        return server;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSslTest, RequiredAndRequiredEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, RequiredAndOptionalEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Optional;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, OptionalAndRequiredEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Optional;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, OptionalAndOptionalEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Optional;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Optional;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_FALSE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, DisabledAndDisabledEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_FALSE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, RequiredAndDisabledEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_FALSE(bus->GetReadyFuture().Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, DisabledAndRequiredEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_FALSE(bus->GetReadyFuture().Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, DisabledAndOptionalEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Optional;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_FALSE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, OptionalAndDisabledEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Optional;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_FALSE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, CAVerificationModeFailure)
{
    // Reset ctx in order to unload possibly loaded CA.
    TSslContext::Get()->Reset();
    LoadKeyPairIntoCxt();

    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::CA;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_FALSE(bus->GetReadyFuture().Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, CAVerificationModeSuccess)
{
    // Reset ctx in order to unload possibly loaded CA.
    TSslContext::Get()->Reset();
    LoadKeyPairIntoCxt();

    // Load CA into ctx.
    auto ca = R"foo(-----BEGIN CERTIFICATE-----
MIIFWjCCA0KgAwIBAgIBATANBgkqhkiG9w0BAQsFADBGMQswCQYDVQQGEwJSVTEP
MA0GA1UECAwGTW9zY293MQ8wDQYDVQQKDAZZYW5kZXgxFTATBgNVBAMMDENBQGxv
Y2FsaG9zdDAeFw0yMzA3MTMxMTUxMTFaFw0zMzA3MTAxMTUxMTFaMEYxCzAJBgNV
BAYTAlJVMQ8wDQYDVQQIDAZNb3Njb3cxDzANBgNVBAoMBllhbmRleDEVMBMGA1UE
AwwMQ0FAbG9jYWxob3N0MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA
mxWRSfbYAJcBgGhYagnQMJOiW60B82Ok94n8muIaa2gRo0ZHKmtY7CMiFIIN0GhI
Blw+G6HPN8SPYK0+bTlAIHlb0u6/ty2AVyveiH31p7Ld/Ib5S/MSKuMiGt5S/Ci9
mSdRAmD46twIKfm9bY/8SZmNsTCJIrKnJaTbjTnbz9O3FVAYjiuc8yNGb1LnZNA6
B4ZrF3fkYr9kn4nKSOd6mypPFIOZAxKGQ95X9nCblajEMAPzHfr+1EArnM+PauAO
cMcxfKT+OlMPGR4ZtZpl90qX6ZVZqD9zd8gp2I/2SM1vVS7AT96t89T7Uag8OjH2
c8jMCD2z1fk1oDD4K53pGkBucTwDolCvxIbN77gcwkutdor/dbHLqIvGV/M4Z/iA
GzqCD6pCpi5gT8SXn2IQrlvSdF01k9YZS093Y8bIQm3dCo1lKDaz6oHytk9Ro+fu
b+dLOhSjopNfa/1Thw6fgta/mRsdgubWI/IHn+IyMpW65vGbnrMD4oQl1AanRJj5
iBS73ZXIs/Y9LuMtSNk/I9u1o2fc+Sg0zb1AchC70h8M3sAYaGbqg1churOaDuTO
Yom5W+bAQO8uLUvmlDcpqxMBYqeE8VJGJWRLtQnhM18iYAa03w7m6uMzrlkoX1Q7
AHnX4899WSUcz/BJc5JBAtHQXCzEzyudT+8xw5MwqRkCAwEAAaNTMFEwHQYDVR0O
BBYEFFuWdWxzQ6TF/yH3sQnlk3EEr1VjMB8GA1UdIwQYMBaAFFuWdWxzQ6TF/yH3
sQnlk3EEr1VjMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggIBAHqS
W4FIjH/YKkcj/wuh7sQQTIBAx+LsjosyHb9K/1QbgLNVpxawl/YlOiArfVmgTGud
9B6xdVacxqI6saMqHNPPgYm7OPnozSnprRKh9yGb2TS9j+2G7hCQLnM+xYroP8XE
8fL9tyq14lHb/BFYnPsFFxqeA6lXzzm73hlm/hH58CWpL/3eR/TDjC+oiEBBV4VF
Dm4X3L73MFcniDKkCb7Mw3wdi6zqx231F4+9Cqgq+RqAucnmLXTG7yR7wOKP2u8E
Ye1Jrt3nnMlD1tqiXyRJywPSK9mMiBTGbzGLLiTcvecBIHG8oRuvn1DnkZa9L/48
+1aTC8QhbRslaeDyXj5IY/7scW7ZkEw18qYCT+9sI4/LngS4U8+taKqLf5S8A5sR
O8OCP0nMk/l5f84cDwe1dp4GXVQdkbnfT1sd93BLww7Szbw2iMt9sLMmyY8qIe7a
Ss3OEfP6eyNCBu6KtL8oufdj1AqAQdmYYTlGwgaFZTAomDiJU662EGSt3uXK7V32
EzgJbpxSSjWh5JpCDA/jnqzlkFkSaw92/HZwO5l1lCdqK+F1UWYQyU0NveOaSEuX
344PIi8m1YWwUfukUB97D+C6B4y5vgdkC7OYLHb4W4+N0ZvYtbDDaBeTpn70CiI7
JFWcF3ghP7uPmbONWLiTFwxsSJHT0svVQZgq1aZz
-----END CERTIFICATE-----)foo";
    TSslContext::Get()->UseCA(ca);

    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::CA;
    clientConfig->CAFile = "unused if CA has already been loaded";
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    for (int i = 0; i < 2; ++i) {
        auto message = CreateMessage(1);
        auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
        Cerr << sendFuture.Get().GetMessage() << Endl;
        EXPECT_TRUE(sendFuture.Get().IsOK());
    }

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, FullVerificationMode)
{
    // Reset ctx in order to unload possibly loaded CA.
    TSslContext::Get()->Reset();
    LoadKeyPairIntoCxt();

    // Load CA into ctx.
    auto ca = R"foo(-----BEGIN CERTIFICATE-----
MIIFWjCCA0KgAwIBAgIBATANBgkqhkiG9w0BAQsFADBGMQswCQYDVQQGEwJSVTEP
MA0GA1UECAwGTW9zY293MQ8wDQYDVQQKDAZZYW5kZXgxFTATBgNVBAMMDENBQGxv
Y2FsaG9zdDAeFw0yMzA3MTMxMTUxMTFaFw0zMzA3MTAxMTUxMTFaMEYxCzAJBgNV
BAYTAlJVMQ8wDQYDVQQIDAZNb3Njb3cxDzANBgNVBAoMBllhbmRleDEVMBMGA1UE
AwwMQ0FAbG9jYWxob3N0MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA
mxWRSfbYAJcBgGhYagnQMJOiW60B82Ok94n8muIaa2gRo0ZHKmtY7CMiFIIN0GhI
Blw+G6HPN8SPYK0+bTlAIHlb0u6/ty2AVyveiH31p7Ld/Ib5S/MSKuMiGt5S/Ci9
mSdRAmD46twIKfm9bY/8SZmNsTCJIrKnJaTbjTnbz9O3FVAYjiuc8yNGb1LnZNA6
B4ZrF3fkYr9kn4nKSOd6mypPFIOZAxKGQ95X9nCblajEMAPzHfr+1EArnM+PauAO
cMcxfKT+OlMPGR4ZtZpl90qX6ZVZqD9zd8gp2I/2SM1vVS7AT96t89T7Uag8OjH2
c8jMCD2z1fk1oDD4K53pGkBucTwDolCvxIbN77gcwkutdor/dbHLqIvGV/M4Z/iA
GzqCD6pCpi5gT8SXn2IQrlvSdF01k9YZS093Y8bIQm3dCo1lKDaz6oHytk9Ro+fu
b+dLOhSjopNfa/1Thw6fgta/mRsdgubWI/IHn+IyMpW65vGbnrMD4oQl1AanRJj5
iBS73ZXIs/Y9LuMtSNk/I9u1o2fc+Sg0zb1AchC70h8M3sAYaGbqg1churOaDuTO
Yom5W+bAQO8uLUvmlDcpqxMBYqeE8VJGJWRLtQnhM18iYAa03w7m6uMzrlkoX1Q7
AHnX4899WSUcz/BJc5JBAtHQXCzEzyudT+8xw5MwqRkCAwEAAaNTMFEwHQYDVR0O
BBYEFFuWdWxzQ6TF/yH3sQnlk3EEr1VjMB8GA1UdIwQYMBaAFFuWdWxzQ6TF/yH3
sQnlk3EEr1VjMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggIBAHqS
W4FIjH/YKkcj/wuh7sQQTIBAx+LsjosyHb9K/1QbgLNVpxawl/YlOiArfVmgTGud
9B6xdVacxqI6saMqHNPPgYm7OPnozSnprRKh9yGb2TS9j+2G7hCQLnM+xYroP8XE
8fL9tyq14lHb/BFYnPsFFxqeA6lXzzm73hlm/hH58CWpL/3eR/TDjC+oiEBBV4VF
Dm4X3L73MFcniDKkCb7Mw3wdi6zqx231F4+9Cqgq+RqAucnmLXTG7yR7wOKP2u8E
Ye1Jrt3nnMlD1tqiXyRJywPSK9mMiBTGbzGLLiTcvecBIHG8oRuvn1DnkZa9L/48
+1aTC8QhbRslaeDyXj5IY/7scW7ZkEw18qYCT+9sI4/LngS4U8+taKqLf5S8A5sR
O8OCP0nMk/l5f84cDwe1dp4GXVQdkbnfT1sd93BLww7Szbw2iMt9sLMmyY8qIe7a
Ss3OEfP6eyNCBu6KtL8oufdj1AqAQdmYYTlGwgaFZTAomDiJU662EGSt3uXK7V32
EzgJbpxSSjWh5JpCDA/jnqzlkFkSaw92/HZwO5l1lCdqK+F1UWYQyU0NveOaSEuX
344PIi8m1YWwUfukUB97D+C6B4y5vgdkC7OYLHb4W4+N0ZvYtbDDaBeTpn70CiI7
JFWcF3ghP7uPmbONWLiTFwxsSJHT0svVQZgq1aZz
-----END CERTIFICATE-----)foo";
    TSslContext::Get()->UseCA(ca);

    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::Full;
    clientConfig->CAFile = "unused if CA has already been loaded";
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    // This test should pass since key pair is issued for CN=localhost.
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, ServerCipherList)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CipherList = "AES128-GCM-SHA256:PSK-AES128-GCM-SHA256";
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::None;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    for (int i = 0; i < 2; ++i) {
        auto message = CreateMessage(1);
        auto sendFuture = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full));
        Cerr << sendFuture.Get().GetMessage() << Endl;
        EXPECT_TRUE(sendFuture.Get().IsOK());
    }

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, DifferentCipherLists)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->UseKeyPairFromSslContext = true;
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CipherList = "PSK-AES128-GCM-SHA256";
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(Address);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::None;
    clientConfig->CipherList = "AES128-GCM-SHA256";
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_FALSE(bus->GetReadyFuture().Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBus
