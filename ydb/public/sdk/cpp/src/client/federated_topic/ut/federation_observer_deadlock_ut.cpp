#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/retry_policy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <ydb/public/sdk/cpp/src/client/federated_topic/ut/fds_mock/fds_mock.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

namespace NYdb::NFederatedTopic::NTests {

namespace {

class TNotReadyTokenCredentialsProvider final : public ICredentialsProvider {
public:
    std::string GetAuthInfo() const override {
        throw TAuthenticationError("IAM-token not ready yet");
    }

    bool IsValid() const override {
        return true;
    }
};

class TNotReadyTokenCredentialsProviderFactory final : public ICredentialsProviderFactory {
public:
    TCredentialsProviderPtr CreateProvider() const override {
        return std::make_shared<TNotReadyTokenCredentialsProvider>();
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        Y_UNUSED(facility);
        return CreateProvider();
    }
};

std::unique_ptr<grpc::Server> StartFdsGrpcServer(TFederationDiscoveryServiceMock& service, int* port) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), port);
    builder.RegisterService(&service);
    return builder.BuildAndStart();
}

} // namespace

Y_UNIT_TEST_SUITE(FederationObserverDeadlock) {

// Regression for YQ-5434: RunFederationDiscoveryImpl must not hold Lock across RunDeferred.
// When GetAuthInfo() fails synchronously, RunDeferred invokes the response callback inline,
// which calls OnFederationDiscovery and tries to re-acquire the same non-recursive TSpinLock.
Y_UNIT_TEST(SyncAuthErrorDuringDiscoveryDoesNotDeadlock) {
    TFederationDiscoveryServiceMock fdsMock;

    int port = 0;
    auto grpcServer = StartFdsGrpcServer(fdsMock, &port);
    UNIT_ASSERT(grpcServer);
    UNIT_ASSERT(port > 0);

    auto credentialsFactory = std::make_shared<TNotReadyTokenCredentialsProviderFactory>();

    TDriverConfig driverConfig;
    driverConfig
        .SetEndpoint(TStringBuilder() << "127.0.0.1:" << port)
        .SetDatabase("/Root")
        .SetCredentialsProviderFactory(credentialsFactory);

    TDriver driver(driverConfig);

    TFederatedTopicClientSettings clientSettings;
    clientSettings
        .CredentialsProviderFactory(credentialsFactory)
        .RetryPolicy(NTopic::IRetryPolicy::GetNoRetryPolicy());

    TFederatedTopicClient client(driver, clientSettings);

    auto clusterInfoFuture = client.GetAllClusterInfo();
    UNIT_ASSERT(clusterInfoFuture.Wait(TDuration::Seconds(5)));
    UNIT_ASSERT_VALUES_EQUAL(clusterInfoFuture.GetValue().size(), 0u);
}

} // Y_UNIT_TEST_SUITE(FederationObserverDeadlock)

} // namespace NYdb::NFederatedTopic::NTests
