#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_key.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/common/network.h>

namespace NYT::NBus {
namespace {

using namespace NCrypto;

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
    std::string AddressWithHostName;
    std::string AddressWithIpV4;
    std::string AddressWithIpV6;

    TPemBlobConfigPtr CACert;
    TPemBlobConfigPtr PrivateKey;
    TPemBlobConfigPtr CertificateChain;
    TPemBlobConfigPtr CACertWithIPInSAN;
    TPemBlobConfigPtr PrivateKeyWithIpInSAN;
    TPemBlobConfigPtr CertificateChainWithIpInSAN;
    TPemBlobConfigPtr CACertEC;
    TPemBlobConfigPtr PrivateKeyEC;
    TPemBlobConfigPtr CertificateChainEC;

    TSslTest()
    {
        Port = NTesting::GetFreePort();
        AddressWithHostName = Format("localhost:%v", Port);
        AddressWithIpV4 = Format("127.0.0.1:%v", Port);
        AddressWithIpV6 = Format("[::1]:%v", Port);

        CACert = CreateTestKeyFile("ca.pem");
        PrivateKey = CreateTestKeyFile("key.pem");
        CertificateChain = CreateTestKeyFile("cert.pem");
        CACertWithIPInSAN = CreateTestKeyFile("ca_with_ip_in_san.pem");
        PrivateKeyWithIpInSAN = CreateTestKeyFile("key_with_ip_in_san.pem");
        CertificateChainWithIpInSAN = CreateTestKeyFile("cert_with_ip_in_san.pem");

        CACertEC = CreateTestKeyFile("ca_ec.pem");
        PrivateKeyEC = CreateTestKeyFile("key_ec.pem");
        CertificateChainEC = CreateTestKeyFile("cert_ec.pem");
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSslTest, RequiredAndRequiredEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, RequiredAndOptionalEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Optional;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, OptionalAndRequiredEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Optional;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, OptionalAndOptionalEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Optional;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Optional;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_FALSE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, DisabledAndDisabledEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_FALSE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, RequiredAndDisabledEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto error = bus->GetReadyFuture().Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), EErrorCode::SslError);

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, DisabledAndRequiredEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto error = bus->GetReadyFuture().Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), EErrorCode::SslError);

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, DisabledAndOptionalEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Optional;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_FALSE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, OptionalAndDisabledEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Optional;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_FALSE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, CAVerificationModeFailure)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::Ca;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto error = bus->GetReadyFuture().Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), EErrorCode::SslError);

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, CAVerificationModeSuccess)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->CertificateAuthority = CACert;
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::Ca;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    for (int i = 0; i < 2; ++i) {
        auto message = CreateMessage(1);
        auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        Cerr << sendFuture.Get().GetMessage() << Endl;
        EXPECT_TRUE(sendFuture.Get().IsOK());
    }

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, FullVerificationModeByHostName)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::Full;
    clientConfig->CertificateAuthority = CACert;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    // This test should pass since key pair is issued for CN=localhost.
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(sendFuture.Get().IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, FullVerificationModeByIpAddress)
{
    // Connect via ipv4 and ipv6 addresses.
    for (const auto& address : {AddressWithIpV4, AddressWithIpV6}) {
        auto serverConfig = TBusServerConfig::CreateTcp(Port);
        serverConfig->EncryptionMode = EEncryptionMode::Required;
        serverConfig->VerificationMode = EVerificationMode::None;
        serverConfig->CertificateChain = CertificateChainWithIpInSAN;
        serverConfig->PrivateKey = PrivateKeyWithIpInSAN;
        auto server = CreateBusServer(serverConfig);
        server->Start(New<TEmptyBusHandler>());

        auto clientConfig = TBusClientConfig::CreateTcp(address);
        clientConfig->EncryptionMode = EEncryptionMode::Required;
        clientConfig->VerificationMode = EVerificationMode::Full;
        clientConfig->CertificateAuthority = CACertWithIPInSAN;
        auto client = CreateBusClient(clientConfig);

        auto bus = client->CreateBus(New<TEmptyBusHandler>());
        // This test should pass since (127.0.0.1 | [::1]) is in SAN.
        EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
        EXPECT_TRUE(bus->IsEncrypted());

        auto message = CreateMessage(1);
        auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        EXPECT_TRUE(sendFuture.Get().IsOK());

        server->Stop()
            .Get()
            .ThrowOnError();
    }
}

TEST_F(TSslTest, FullVerificationByAlternativeHostName)
{
    for (const auto& address : {AddressWithIpV4, AddressWithIpV6}) {
        auto serverConfig = TBusServerConfig::CreateTcp(Port);
        serverConfig->EncryptionMode = EEncryptionMode::Required;
        serverConfig->VerificationMode = EVerificationMode::None;
        serverConfig->CertificateChain = CertificateChain;
        serverConfig->PrivateKey = PrivateKey;
        auto server = CreateBusServer(serverConfig);
        server->Start(New<TEmptyBusHandler>());

        // Connect via IP.
        auto clientConfig = TBusClientConfig::CreateTcp(address);
        clientConfig->EncryptionMode = EEncryptionMode::Required;
        clientConfig->VerificationMode = EVerificationMode::Full;
        clientConfig->CertificateAuthority = CACert;

        {
            auto client = CreateBusClient(clientConfig);
            auto bus = client->CreateBus(New<TEmptyBusHandler>());
            // This test should fail since (127.0.0.1 | [::1]) != localhost.
            EXPECT_THROW_MESSAGE_HAS_SUBSTR(
                bus->GetReadyFuture().Get().ThrowOnError(),
                NYT::TErrorException,
                "Failed to establish TLS/SSL session");
        }

        // Connect via IP with Alt Hostname.
        clientConfig->PeerAlternativeHostName = "localhost";
        auto client = CreateBusClient(clientConfig);

        auto bus = client->CreateBus(New<TEmptyBusHandler>());
        // This test should pass since key pair is issued for CN=localhost.
        EXPECT_NO_THROW(bus->GetReadyFuture().Get().ThrowOnError());
        EXPECT_TRUE(bus->IsEncrypted());

        auto message = CreateMessage(1);
        auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        EXPECT_NO_THROW(sendFuture.Get().ThrowOnError());

        server->Stop()
            .Get()
            .ThrowOnError();
    }
}

TEST_F(TSslTest, MutualVerificationSuccess)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::Full;
    serverConfig->CertificateAuthority = CACert;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::Full;
    clientConfig->CertificateAuthority = CACert;
    clientConfig->CertificateChain = CertificateChain;
    clientConfig->PrivateKey = PrivateKey;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    bus->GetReadyFuture().Get().ThrowOnError();
    EXPECT_TRUE(bus->IsEncrypted());

    auto message = CreateMessage(1);
    auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    sendFuture.Get().ThrowOnError();

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, MutualVerificationFailedWithoutClientCertificate)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::Full;
    serverConfig->CertificateAuthority = CACert;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::Full;
    clientConfig->CertificateAuthority = CACert;
    // Not sending client certificate.
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto error = bus->GetReadyFuture().Get();

    if (!error.IsOK()) {
        // Client should get error after BUS handshake and avoid TLS handshake.
        EXPECT_EQ(error.GetCode(), EErrorCode::SslError);
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            error.ThrowOnError(),
            NYT::TErrorException,
            "Server requested TLS/SSL client certificate for connection");
    } else {
        // Check that connection is terminated by server after TLS handshake.
        EXPECT_TRUE(bus->IsEncrypted());

        auto message = CreateMessage(1);
        auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        auto error = sendFuture.Get();
        EXPECT_EQ(error.GetCode(), EErrorCode::SslError);
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            error.ThrowOnError(),
            NYT::TErrorException,
            "alert certificate required");

        THROW_ERROR_EXCEPTION("Mutual TLS failed only after TLS handshake");
    }

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, MutualVerificationFailedWithWrongClientCertificate)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::Full;
    serverConfig->CertificateAuthority = CACert;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::Full;
    clientConfig->CertificateAuthority = CACert;
    // Send client certificate signed by different authority.
    clientConfig->CertificateChain = CertificateChainWithIpInSAN;
    clientConfig->PrivateKey = PrivateKeyWithIpInSAN;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto error = bus->GetReadyFuture().Get();
    if (!error.IsOK()) {
        // Connection could be terminated on TLS handshake.
        EXPECT_EQ(error.GetCode(), EErrorCode::SslError);
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            error.ThrowOnError(),
            NYT::TErrorException,
            "alert unknown ca");
    } else {
        // For TLS1.3 connection is terminated after TLS handshake.
        EXPECT_TRUE(bus->IsEncrypted());

        auto message = CreateMessage(1);
        auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        auto error = sendFuture.Get();
        EXPECT_EQ(error.GetCode(), EErrorCode::SslError);
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            error.ThrowOnError(),
            NYT::TErrorException,
            "alert unknown ca");
    }

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, ServerCipherList)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    serverConfig->CipherList = "AES128-GCM-SHA256:PSK-AES128-GCM-SHA256";
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::None;
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
    EXPECT_TRUE(bus->IsEncrypted());

    for (int i = 0; i < 2; ++i) {
        auto message = CreateMessage(1);
        auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        Cerr << sendFuture.Get().GetMessage() << Endl;
        EXPECT_TRUE(sendFuture.Get().IsOK());
    }

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, DifferentCipherLists)
{
    auto cipherStringAES128 = TSslContextCommand::Create("CipherString", "AES128-GCM-SHA256");
    auto cipherSuitesAES128 = TSslContextCommand::Create("CipherSuites", "TLS_AES_128_GCM_SHA256");
    auto cipherStringAES256 = TSslContextCommand::Create("CipherString", "AES256-GCM-SHA384");
    auto cipherSuitesAES256 = TSslContextCommand::Create("CipherSuites", "TLS_AES_256_GCM_SHA384");
    auto maxProtocolTLS1_2 = TSslContextCommand::Create("MaxProtocol", "TLSv1.2");

    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = CertificateChain;
    serverConfig->PrivateKey = PrivateKey;
    serverConfig->SslConfigurationCommands.push_back(cipherStringAES256);
    serverConfig->SslConfigurationCommands.push_back(cipherSuitesAES256);
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::None;
    clientConfig->SslConfigurationCommands.push_back(cipherStringAES128);
    clientConfig->SslConfigurationCommands.push_back(cipherSuitesAES128);
    auto client = CreateBusClient(clientConfig);

    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto error = bus->GetReadyFuture().Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), EErrorCode::SslError);

    {
        clientConfig->SslConfigurationCommands.push_back(maxProtocolTLS1_2);
        auto client = CreateBusClient(clientConfig);
        auto bus = client->CreateBus(New<TEmptyBusHandler>());
        EXPECT_FALSE(bus->GetReadyFuture().Get().IsOK());
    }

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TSslTest, FullVerificationWithEllipticCurve)
{
    // Connect via localhost, ipv4 and ipv6 addresses.
    for (const auto& address : {AddressWithHostName, AddressWithIpV4, AddressWithIpV6}) {
        auto serverConfig = TBusServerConfig::CreateTcp(Port);
        serverConfig->EncryptionMode = EEncryptionMode::Required;
        serverConfig->VerificationMode = EVerificationMode::None;
        serverConfig->CertificateChain = CertificateChainEC;
        serverConfig->PrivateKey = PrivateKeyEC;
        auto server = CreateBusServer(serverConfig);
        server->Start(New<TEmptyBusHandler>());

        auto clientConfig = TBusClientConfig::CreateTcp(address);
        clientConfig->EncryptionMode = EEncryptionMode::Required;
        clientConfig->VerificationMode = EVerificationMode::Full;
        clientConfig->CertificateAuthority = CACertEC;
        auto client = CreateBusClient(clientConfig);

        auto bus = client->CreateBus(New<TEmptyBusHandler>());
        // This test should pass since (localhost | 127.0.0.1 | [::1]) is in SAN.
        EXPECT_TRUE(bus->GetReadyFuture().Get().IsOK());
        EXPECT_TRUE(bus->IsEncrypted());

        auto message = CreateMessage(1);
        auto sendFuture = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        EXPECT_TRUE(sendFuture.Get().IsOK());

        server->Stop()
            .Get()
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBus
