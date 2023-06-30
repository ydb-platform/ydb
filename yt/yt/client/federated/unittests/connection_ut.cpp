#include <yt/yt/client/federated/connection.h>
#include <yt/yt/client/federated/config.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/connection.h>

#include <yt/yt/core/net/local_address.h>

namespace NYT::NClient::NFederated {
namespace {

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRefOfCopy;
using ::testing::StrictMock;

using TStrictMockClient = StrictMock<NApi::TMockClient>;
using TStrictMockConnection = StrictMock<NApi::TMockConnection>;

////////////////////////////////////////////////////////////////////////////////

TEST(TFederatedConnectionTest, CreateClient)
{
    auto config = New<TFederationConfig>();
    config->BundleName = "my_bundle";

    auto mockConnectionSas = New<TStrictMockConnection>();
    auto mockConnectionVla = New<TStrictMockConnection>();
    auto mockClientSas = New<TStrictMockClient>();
    auto mockClientVla = New<TStrictMockClient>();

    EXPECT_CALL(*mockClientVla, CheckClusterLiveness(_))
        .WillRepeatedly(Return(VoidFuture));

    // To identify best (closest) cluster.
    NYson::TYsonString nodesYsonSas(TStringBuf(R"(["a-rpc-proxy-a.sas.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientSas, ListNode("//sys/rpc_proxies", _))
        .WillRepeatedly(Return(MakeFuture(nodesYsonSas)));

    NYson::TYsonString nodesYsonVla(TStringBuf(R"(["a-rpc-proxy-a.vla.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientVla, ListNode("//sys/rpc_proxies", _))
        .WillRepeatedly(Return(MakeFuture(nodesYsonVla)));

    EXPECT_CALL(*mockClientSas, CheckClusterLiveness(_))
        .WillRepeatedly(Return(VoidFuture));
    EXPECT_CALL(*mockClientVla, CheckClusterLiveness(_))
        .WillRepeatedly(Return(VoidFuture));

    NApi::TClientOptions clientOptions;
    EXPECT_CALL(*mockConnectionSas, CreateClient(::testing::Ref(clientOptions)))
        .WillOnce(Return(mockClientSas));
    EXPECT_CALL(*mockConnectionVla, CreateClient(::testing::Ref(clientOptions)))
        .WillOnce(Return(mockClientVla));

    EXPECT_CALL(*mockConnectionSas, GetLoggingTag())
        .WillOnce(ReturnRefOfCopy(TString("sas")));
    EXPECT_CALL(*mockConnectionVla, GetLoggingTag())
        .WillOnce(ReturnRefOfCopy(TString("vla")));

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("a-rpc-proxy.sas.yp-c.yandex.net");

    auto connection = CreateConnection({mockConnectionSas, mockConnectionVla}, config);
    EXPECT_THAT(connection->GetLoggingTag(), testing::HasSubstr("Clusters: (sas; vla)"));
    auto client = connection->CreateClient(clientOptions);
    auto nodes = client->ListNode("//sys/rpc_proxies").Get().ValueOrThrow();
    EXPECT_EQ(nodesYsonSas, nodes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NClient::NFederated
