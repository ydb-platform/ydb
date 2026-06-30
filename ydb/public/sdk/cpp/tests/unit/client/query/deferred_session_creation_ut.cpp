#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <thread>

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

constexpr TDuration kShortDeadline = TDuration::MilliSeconds(50);
constexpr TDuration kSlowAttach = TDuration::MilliSeconds(300);

class TDelayedMockQueryService : public Ydb::Query::V1::QueryService::Service {
public:
    TDuration AttachDelay = TDuration::Zero();

    grpc::Status CreateSession(
        grpc::ServerContext*,
        const Ydb::Query::CreateSessionRequest*,
        Ydb::Query::CreateSessionResponse* response) override
    {
        response->set_status(Ydb::StatusIds::SUCCESS);
        response->set_session_id("fake-query-session-id");
        response->set_node_id(1);
        return grpc::Status::OK;
    }

    grpc::Status AttachSession(
        grpc::ServerContext* context,
        const Ydb::Query::AttachSessionRequest*,
        grpc::ServerWriter<Ydb::Query::SessionState>* writer) override
    {
        if (AttachDelay != TDuration::Zero()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(AttachDelay.MilliSeconds()));
        }
        Ydb::Query::SessionState state;
        state.set_status(Ydb::StatusIds::SUCCESS);
        writer->Write(state);
        while (!context->IsCancelled()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return grpc::Status::OK;
    }
};

template <class TService>
std::unique_ptr<grpc::Server> StartGrpcServer(const std::string& address, TService& service) {
    return grpc::ServerBuilder()
        .AddListeningPort(TString{address}, grpc::InsecureServerCredentials())
        .RegisterService(&service)
        .BuildAndStart();
}

TCreateSessionSettings ShortDeadlineSettings() {
    return TCreateSessionSettings()
        .ClientTimeout(kShortDeadline)
        .Deadline(TDeadline::AfterDuration(kShortDeadline));
}

std::unique_ptr<TQueryClient> MakeClient(TDriver& driver, bool deferred) {
    return std::make_unique<TQueryClient>(
        driver,
        TClientSettings().SessionPoolSettings(
            TSessionPoolSettings().UseDeferredSessionCreation(deferred)));
}

} // namespace

Y_UNIT_TEST_SUITE(DeferredGetSession) {

Y_UNIT_TEST(TimeoutThenPoolWarmup) {
    NTesting::InitPortManagerFromEnv();
    const auto port = NTesting::GetFreePort();
    const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

    TDelayedMockQueryService service;
    service.AttachDelay = kSlowAttach;
    auto server = StartGrpcServer(endpoint, service);

    TDriver driver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/My/DB"));
    auto client = MakeClient(driver, /*deferred=*/true);

    const auto result = client->GetSession(ShortDeadlineSettings()).ExtractValueSync();
    UNIT_ASSERT(!result.IsSuccess());
    UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::CLIENT_DEADLINE_EXCEEDED);

    for (int i = 0; i < 40; ++i) {
        if (client->GetCurrentPoolSize() == 1 && client->GetActiveSessionCount() == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    UNIT_ASSERT_EQUAL(client->GetCurrentPoolSize(), 1);
    UNIT_ASSERT_EQUAL(client->GetActiveSessionCount(), 0);

    client.reset();
    driver.Stop(true);
}

Y_UNIT_TEST(DisabledWaitsForAttach) {
    NTesting::InitPortManagerFromEnv();
    const auto port = NTesting::GetFreePort();
    const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

    TDelayedMockQueryService service;
    service.AttachDelay = kSlowAttach;
    auto server = StartGrpcServer(endpoint, service);

    TDriver driver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/My/DB"));
    auto client = MakeClient(driver, /*deferred=*/false);

    const auto started = TInstant::Now();
    const auto result = client->GetSession(ShortDeadlineSettings()).ExtractValueSync();
    UNIT_ASSERT(result.IsSuccess());
    UNIT_ASSERT(!result.GetSession().GetId().empty());
    UNIT_ASSERT_GE(TInstant::Now() - started, kSlowAttach);
    UNIT_ASSERT_EQUAL(client->GetActiveSessionCount(), 1);

    client.reset();
    driver.Stop(true);
}

}
