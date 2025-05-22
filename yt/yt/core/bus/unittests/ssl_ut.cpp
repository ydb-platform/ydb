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
    TString AddressWithHostName;
    TString AddressWithIpV4;
    TString AddressWithIpV6;

    const char* CA = R"foo(-----BEGIN CERTIFICATE-----
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

    const char* CertChain = R"foo(-----BEGIN CERTIFICATE-----
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

    const char* PrivateKey = R"foo(-----BEGIN PRIVATE KEY-----
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
rPl77gAcribJm3TzBVHm2m6jBGtb
-----END PRIVATE KEY-----)foo";

    const char* CAWithIpInSAN = R"foo(-----BEGIN CERTIFICATE-----
MIIFHzCCAwegAwIBAgIUQEt4xnHWGulMGzqad434c4Mw+cAwDQYJKoZIhvcNAQEL
BQAwJjERMA8GA1UECgwIWVRzYXVydXMxETAPBgNVBAMMCFlUc2F1cnVzMB4XDTI0
MDIyOTEwMTIzM1oXDTM0MDIyNjEwMTIzM1owJjERMA8GA1UECgwIWVRzYXVydXMx
ETAPBgNVBAMMCFlUc2F1cnVzMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKC
AgEAwUS9deIwfMNeicFqw7/fDslSb7sSiPbYfYUxwuELBR8nVomaL/a7IpQx5dOw
nB2CSUiTDsLczNaBvQyNkjJ8VIX4fEPtgfHCuoUaCg4NwcaHfY0TSssbCKh8U92z
fM/iSCjjkMdp9pTzZL93yam72dqfFbHmBvO6XiGjYeR9BL6AqoZMBnIjmxsU9JCW
SM7B0l+Sf8dsH8DmnzqtLUXKIZJyVK6LOVb7MtSw0iAYatrIt3t7IytqZMOe+dEZ
V0C0YzTecOgCWKS+rA8X1h7zHW5vXLDYU6RR+qw3gFsRK0oHmECuo+4Ui2nMzkx1
jf2nK8L0Z2MHP0YFI9cGCBfSZMv6eocbYZI1DmhNKZNFhSLL8lmfzv4uR7WPyNL6
Ml+2xgp03r1QFx93cRNW5bjjFytzYQkEVwbvLsABHDFm056PpNXKh6eoXGkUfaWf
iXxUWhAFJRxggkTrPawCr+YQtVbBFe/YcdrTkdvFengFLm4wE20RiJUZWvG4oXmV
JdKvDMsR1RFoQQfSqKL3mERMvVBBO1CXqymdPW6K+S2r367ryXGCG6kRDGpfmu7R
vYZcA9BJqNvGcowf5fBe2VZf3cqZwdZKjCYOR3dCVDbXClraeStzy0VXKgxukHyT
g3XfPqptaUKtpy413b/AVG2YupEn+SoOGMP0NKMyj9XAr20CAwEAAaNFMEMwDgYD
VR0PAQH/BAQDAgLkMBIGA1UdEwEB/wQIMAYBAf8CAQEwHQYDVR0OBBYEFAYGwcRb
cWnfIkC1Mozkzm0CDcFbMA0GCSqGSIb3DQEBCwUAA4ICAQCztlZxgJNdhebicTkT
B4iXXntoOzlnusua1lCBKRUowHAocw2ylXb32Ahh/fENmHyt/jYTlcFu/qK6Q1T8
/pN14hnyF2pB+ZJjyAr3vNnZtHGJ2xz3wPqFRzD00YUqvvLi9xD2nhsfn4xex/OK
KUx/dbV6FkJ60Fpg97zeOmO+5Kh9srIEGEczPb+y9meiMGB91tm+ZUcd9cGShTRH
krFkuCySJuNgrAZCxqvdsUPvPDd12lPyqmfmuVjauON9ENYUYwNoxQc8MxEx0x6j
QolYDmc2A2VP29rTLQhyjf8TvwXfq8z+zNoeQZCNCON5jg7zH67XarXxK1AZOePc
ZqHtjfTLTqmmKkkG7IRt1h9dtGsROMaRfXf0t4M9mvqx/1Cx6abA6LfcOkp7OG8S
0tx0IzIRnQh2iN4zR1MihS3hn3s9ayviiaopIPVyCKEKRZpsL3QzhzydnQM/Wb+r
UTe546vZV3q2irHH/x4SZFWoFhDwepAyUMI4qo1REd+cM/MakLP5x4nFzzDmPjyf
FiuyqHTlkMtjveytSblzpzWE1/Sum3RcMh4s9ECq8XaUl/8FerUYvIJfRDq1j79J
w/cDyD142joRYwQG0HQkmE4ph4mYFwKhmYOv11Wik9zvEt156VPFaExu6rkjmLia
nTkPBMUXiU3GIb4H7k78sEjv1g==
-----END CERTIFICATE-----)foo";

    const char* CertChainWithIpInSAN = R"foo(-----BEGIN CERTIFICATE-----
MIIFVTCCAz2gAwIBAgIBATANBgkqhkiG9w0BAQsFADAmMREwDwYDVQQKDAhZVHNh
dXJ1czERMA8GA1UEAwwIWVRzYXVydXMwHhcNMjQwMjI5MTAxMjMzWhcNMzQwMjI2
MTAxMjMzWjATMREwDwYDVQQKDAhZVHNhdXJ1czCCAiIwDQYJKoZIhvcNAQEBBQAD
ggIPADCCAgoCggIBAJ4BhT+WpWpkEEgaqcbZoV5/JOeAkFj2iZL08nIjkDRVhDBh
dQoop47vB1lpLD/1i5+2YEackPrfeJawbNu32s/belvxHpTc1ljndCJlilMiR5B+
MccP00fUZRsFhkzznANz++31PI4wamHJ1kdJOgD/4civ0mpWgd5s1hityqsypXI3
RFbi689+mnBp2sGCD6l3QVbMj+8ORvXOVC1h1W3tExiivabjFILgXwb6WG6ZNgg8
T20zdH4uEc4d2v6XKY4nz4AqYDHax9oqs3XTOTo0Bld6m7oipjGjToMqqpJD32pb
nxSNT/XECpsNqZ/UMtcQf3HoA2LEOZOg/Knf1mosEww2svb0CqMGfsxxHehCaFJT
CWkdd8GTQ/3t85xtrd5Ccdqb78o+5039H4GkfkcxAIZQe7siNLbUJ+dR6wRMzJw2
GMcAHEIozOYVDgwyOT/Q3gaKMg4A3Ki2x7tkCie1KmmAcnECWDRhzG/Hg/RdnYqA
L6g/m7Z3bw93rUbwXNudVj1ls5MPoyYtTACtZAXnv/PBBd+KR+RZ//T6dD7B9zzC
MiIsIHemPzE3XsWB6I1AqXv2B8THELgJ8foN6Rho4YQ0t0wjhcM0higfSaiHwqhf
Cgoc5xxOGa8PlEETWQvcT3ORAukpjM22QbZmA3uLN/MR1DJbwNHqiYPiFZ1zAgMB
AAGjgaAwgZ0wDgYDVR0PAQH/BAQDAgWgMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggr
BgEFBQcDAjAsBgNVHREEJTAjgglsb2NhbGhvc3SHBH8AAAGHEAAAAAAAAAAAAAAA
AAAAAAEwHQYDVR0OBBYEFDyAfXqSAoKxK/sCyFs3d2yUpSntMB8GA1UdIwQYMBaA
FAYGwcRbcWnfIkC1Mozkzm0CDcFbMA0GCSqGSIb3DQEBCwUAA4ICAQCHRuHI5XaS
PIK3JKzA6l01z7YDHROGn5Xf9hqo5QKHv/aUD01UUhxCbx5z/YZMFDc6iRvu4xQw
HMl8BCvTo3gHSS8JRcgRn1ov5m50NZMl0Ws9xvvy1j5rZWd3HkBTQSpzr4eSHAZp
dUWXN2JEW7QIaIxtoiLwMZVjo3Lfr2Qv4uIn9lbIR0F7s39+qFlzvDx+6JbstJsv
5wPjTS9og3WfpBDaOecHM/nP8v1H9ilx8/EW0nM3jlS0q0Gj9whkha0Pcl156Bga
biLDoQk7uTccO8Wiyddwfq6tlYy1OAIMqDy0vmoz4L/3FHJUqrzO/fdI9VQLlug+
M2G6qTJHKzmDkvmtxPfTjRFMu+g7L3QEdYCBogfIHS+VoB9a9K/XmoWBg85cWIPw
Kfjjf7OouqksfOQopxY2+PCQ66nnkN7y13RjoU2heAme8Fexiowkjhzc1lq0Zn0Q
XPlnvCHAQMNRNmvBLwNEkW+KN4no0TCImOOTuInBrlKGTaBkinUNS39AF9lZWwAE
hd1kK5zzF6XvZnKXdVIn4MjcW81hcbrnulq5GHz7XY+lwmORYumYo3Gykjj/+G93
K9HRlSRV1+BNXmPYtI8hvbAYw05+AWKCk0J5r1GQtPx+Tx3sug/2qks26oURgEHc
ySl4OPJLp2lhKCUkKVP24Tzg/iS1xT/uHQ==
-----END CERTIFICATE-----)foo";

    const char* PrivateKeyWithIpInSAN = R"foo(-----BEGIN PRIVATE KEY-----
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
AbE/BnHl1tAmZXLMrHq/4r0wYUjBsA==
-----END PRIVATE KEY-----)foo";

    TSslTest()
    {
        Port = NTesting::GetFreePort();
        AddressWithHostName = Format("localhost:%v", Port);
        AddressWithIpV4 = Format("127.0.0.1:%v", Port);
        AddressWithIpV6 = Format("[::1]:%v", Port);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSslTest, RequiredAndRequiredEncryptionMode)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
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
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
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
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
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
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
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
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
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
    serverConfig->EncryptionMode = EEncryptionMode::Disabled;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
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
    // Reset ctx in order to unload possibly loaded CA.
    TSslContext::Get()->Reset();

    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::Ca;
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

    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->CA = New<NCrypto::TPemBlobConfig>();
    clientConfig->CA->Value = CA;
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
    // Reset ctx in order to unload possibly loaded CA.
    TSslContext::Get()->Reset();

    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
    clientConfig->EncryptionMode = EEncryptionMode::Required;
    clientConfig->VerificationMode = EVerificationMode::Full;
    clientConfig->CA = New<NCrypto::TPemBlobConfig>();
    clientConfig->CA->Value = CA;
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
    // Reset ctx in order to unload possibly loaded CA.
    TSslContext::Get()->Reset();

    // Connect via ipv4 and ipv6 addresses.
    for (const auto& address : {AddressWithIpV4, AddressWithIpV6}) {
        auto serverConfig = TBusServerConfig::CreateTcp(Port);
        serverConfig->EncryptionMode = EEncryptionMode::Required;
        serverConfig->VerificationMode = EVerificationMode::None;
        serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
        serverConfig->CertificateChain->Value = CertChainWithIpInSAN;
        serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
        serverConfig->PrivateKey->Value = PrivateKeyWithIpInSAN;
        auto server = CreateBusServer(serverConfig);
        server->Start(New<TEmptyBusHandler>());

        auto clientConfig = TBusClientConfig::CreateTcp(address);
        clientConfig->EncryptionMode = EEncryptionMode::Required;
        clientConfig->VerificationMode = EVerificationMode::Full;
        clientConfig->CA = New<NCrypto::TPemBlobConfig>();
        clientConfig->CA->Value = CAWithIpInSAN;
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
    // Reset ctx in order to unload possibly loaded CA.
    TSslContext::Get()->Reset();

    for (const auto& address : {AddressWithIpV4, AddressWithIpV6}) {
        auto serverConfig = TBusServerConfig::CreateTcp(Port);
        serverConfig->EncryptionMode = EEncryptionMode::Required;
        serverConfig->VerificationMode = EVerificationMode::None;
        serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
        serverConfig->CertificateChain->Value = CertChain;
        serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
        serverConfig->PrivateKey->Value = PrivateKey;
        auto server = CreateBusServer(serverConfig);
        server->Start(New<TEmptyBusHandler>());

        // Connect via IP.
        auto clientConfig = TBusClientConfig::CreateTcp(address);
        clientConfig->EncryptionMode = EEncryptionMode::Required;
        clientConfig->VerificationMode = EVerificationMode::Full;
        clientConfig->CA = New<NCrypto::TPemBlobConfig>();
        clientConfig->CA->Value = CA;

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

TEST_F(TSslTest, ServerCipherList)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
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
    auto serverConfig = TBusServerConfig::CreateTcp(Port);
    serverConfig->EncryptionMode = EEncryptionMode::Required;
    serverConfig->VerificationMode = EVerificationMode::None;
    serverConfig->CertificateChain = New<NCrypto::TPemBlobConfig>();
    serverConfig->CertificateChain->Value = CertChain;
    serverConfig->PrivateKey = New<NCrypto::TPemBlobConfig>();
    serverConfig->PrivateKey->Value = PrivateKey;
    serverConfig->CipherList = "PSK-AES128-GCM-SHA256";
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto clientConfig = TBusClientConfig::CreateTcp(AddressWithHostName);
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
