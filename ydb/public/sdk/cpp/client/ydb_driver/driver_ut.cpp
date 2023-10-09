#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <atomic>

#include <google/protobuf/text_format.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

    class TMockDiscoveryService : public Ydb::Discovery::V1::DiscoveryService::Service {
    public:
        grpc::Status ListEndpoints(
                grpc::ServerContext* context,
                const Ydb::Discovery::ListEndpointsRequest* request,
                Ydb::Discovery::ListEndpointsResponse* response) override
        {
            Y_UNUSED(context);

            Cerr << "ListEndpoints: " << request->ShortDebugString() << Endl;

            const auto* result = MockResults.FindPtr(request->database());
            Y_ABORT_UNLESS(result, "Mock service doesn't have a result for database '%s'", request->database().c_str());

            auto* op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->mutable_result()->PackFrom(*result);
            return grpc::Status::OK;
        }

        // From database name to result
        THashMap<TString, Ydb::Discovery::ListEndpointsResult> MockResults;
    };

    class TMockTableService : public Ydb::Table::V1::TableService::Service {
    public:
        grpc::Status CreateSession(
                grpc::ServerContext* context,
                const Ydb::Table::CreateSessionRequest* request,
                Ydb::Table::CreateSessionResponse* response) override
        {
            Y_UNUSED(context);

            Cerr << "CreateSession: " << request->ShortDebugString() << Endl;

            Ydb::Table::CreateSessionResult result;
            result.set_session_id("my-session-id");

            auto* op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->mutable_result()->PackFrom(result);
            return grpc::Status::OK;
        }
    };

    template<class TService>
    std::unique_ptr<grpc::Server> StartGrpcServer(const TString& address, TService& service) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        return builder.BuildAndStart();
    }

} // namespace

Y_UNIT_TEST_SUITE(CppGrpcClientSimpleTest) {
    Y_UNIT_TEST(ConnectWrongPort) {
        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint("localhost:100"));
        auto client = NTable::TTableClient(driver);
        auto sessionFuture = client.CreateSession();

        UNIT_ASSERT(sessionFuture.Wait(TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(ConnectWrongPortRetry) {
        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint("localhost:100"));
        auto client = NTable::TTableClient(driver);

        std::atomic_int counter = 0;
        std::function<void(const NTable::TAsyncCreateSessionResult& future)> handler =
            [&handler, &counter, client] (const NTable::TAsyncCreateSessionResult& future) mutable {
                UNIT_ASSERT_EQUAL(future.GetValue().GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
                UNIT_ASSERT_EXCEPTION(future.GetValue().GetSession(), NYdb::TContractViolation);
                ++counter;
                if (counter.load() > 4) {
                    return;
                }

                auto f = client.CreateSession();

                f.Apply(handler).GetValueSync();
            };

        client.CreateSession().Apply(handler).GetValueSync();
        UNIT_ASSERT_EQUAL(counter, 5);
    }

    Y_UNIT_TEST(TokenCharacters) {
        auto checkToken = [](const TString& token) {
            auto driver = TDriver(
                TDriverConfig()
                    .SetEndpoint("localhost:100")
                    .SetAuthToken(token));
            auto client = NTable::TTableClient(driver);

            auto result = client.CreateSession().GetValueSync();

            return result.GetStatus();
        };

        TVector<TString> InvalidTokens = {
            TString('\t'),
            TString('\n'),
            TString('\r')
        };
        for (auto& t : InvalidTokens) {
            UNIT_ASSERT_EQUAL(checkToken(t), EStatus::CLIENT_UNAUTHENTICATED);
        }

        TVector<TString> ValidTokens = {
            TString("qwerty 1234 <>,.?/:;\"'\\|}{~`!@#$%^&*()_+=-"),
            TString()
        };
        for (auto& t : ValidTokens) {
            UNIT_ASSERT_EQUAL(checkToken(t), EStatus::TRANSPORT_UNAVAILABLE);
        }
    }

    Y_UNIT_TEST(UsingIpAddresses) {
        TPortManager pm;

        // Start our mock table service
        TMockTableService tableService;
        ui16 tablePort = pm.GetPort();
        auto tableServer = StartGrpcServer(
                TStringBuilder() << "127.0.0.1:" << tablePort,
                tableService);

        // Start our mock discovery service
        TMockDiscoveryService discoveryService;
        {
            auto& dbResult = discoveryService.MockResults["/Root/My/DB"];
            auto* endpoint = dbResult.add_endpoints();
            endpoint->set_address("this.dns.name.is.not.reachable");
            endpoint->set_port(tablePort);
            endpoint->add_ip_v4("127.0.0.1");
        }
        ui16 discoveryPort = pm.GetPort();
        auto discoveryServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << discoveryPort,
                discoveryService);

        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << discoveryPort)
                .SetDatabase("/Root/My/DB"));
        auto client = NTable::TTableClient(driver);
        auto sessionFuture = client.CreateSession();

        UNIT_ASSERT(sessionFuture.Wait(TDuration::Seconds(10)));
        auto sessionResult = sessionFuture.ExtractValueSync();
        UNIT_ASSERT(sessionResult.IsSuccess());
        auto session = sessionResult.GetSession();
        UNIT_ASSERT_VALUES_EQUAL(session.GetId(), "my-session-id");
    }
}
