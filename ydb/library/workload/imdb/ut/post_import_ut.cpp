#include <ydb/library/workload/imdb/imdb.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <util/datetime/base.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

using namespace NYdbWorkload;

namespace {

class TAnalyzeService final: public Ydb::Query::V1::QueryService::Service {
public:
    explicit TAnalyzeService(size_t failAt)
        : FailAt(failAt)
    {}

    grpc::Status CreateSession(grpc::ServerContext*, const Ydb::Query::CreateSessionRequest*,
            Ydb::Query::CreateSessionResponse* response) override {
        response->set_status(Ydb::StatusIds::SUCCESS);
        response->set_session_id("imdb-test-session");
        response->set_node_id(1);
        return grpc::Status::OK;
    }

    grpc::Status AttachSession(grpc::ServerContext* context, const Ydb::Query::AttachSessionRequest*,
            grpc::ServerWriter<Ydb::Query::SessionState>* writer) override {
        Ydb::Query::SessionState state;
        state.set_status(Ydb::StatusIds::SUCCESS);
        if (!writer->Write(state)) {
            return grpc::Status::CANCELLED;
        }
        while (!context->IsCancelled()) {
            Sleep(TDuration::MilliSeconds(1));
        }
        return grpc::Status::OK;
    }

    grpc::Status ExecuteQuery(grpc::ServerContext*, const Ydb::Query::ExecuteQueryRequest* request,
            grpc::ServerWriter<Ydb::Query::ExecuteQueryResponsePart>* writer) override {
        auto guard = Guard(Lock);
        Requests.push_back(*request);
        Ydb::Query::ExecuteQueryResponsePart response;
        response.set_status(Requests.size() == FailAt ? Ydb::StatusIds::BAD_REQUEST : Ydb::StatusIds::SUCCESS);
        if (Requests.size() == FailAt) {
            response.add_issues()->set_message("Statistics collection failed");
        }
        return writer->Write(response) ? grpc::Status::OK : grpc::Status::CANCELLED;
    }

    TVector<Ydb::Query::ExecuteQueryRequest> GetRequests() {
        auto guard = Guard(Lock);
        return Requests;
    }

private:
    const size_t FailAt;
    TMutex Lock;
    TVector<Ydb::Query::ExecuteQueryRequest> Requests;
};

void CheckPostImport(size_t failAt) {
    NTesting::InitPortManagerFromEnv();
    const auto port = NTesting::GetFreePort();
    const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;
    TAnalyzeService service(failAt);
    auto server = grpc::ServerBuilder()
        .AddListeningPort(endpoint, grpc::InsecureServerCredentials())
        .RegisterService(&service)
        .BuildAndStart();
    UNIT_ASSERT(server);
    NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(endpoint)
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Off).SetDatabase("/Root"));
    Y_DEFER {
        driver.Stop(true);
        server->Shutdown();
    };
    NYdb::NQuery::TQueryClient client(driver);
    TImdbWorkloadParams params;
    params.DbPath = "/Root";
    params.SetPath("custom/imdb");
    params.QueryClient = &client;
    auto initializer = params.CreateDataInitializers().front();
    if (failAt) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(initializer->PostImport(), NYdb::NStatusHelpers::TYdbErrorException, "Statistics collection failed");
    } else {
        UNIT_ASSERT_VALUES_EQUAL(initializer->PostImport(), EXIT_SUCCESS);
    }
    const auto requests = service.GetRequests();
    const auto queries = TImdbWorkloadGenerator(params).GetAnalyzeQueries();
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), failAt ? failAt : 21);
    for (size_t i = 0; i < requests.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(requests[i].query_content().text(), queries[i]);
        UNIT_ASSERT(requests[i].tx_control().tx_selector_case() == Ydb::Query::TransactionControl::TX_SELECTOR_NOT_SET);
        UNIT_ASSERT(!requests[i].tx_control().commit_tx());
    }
}

} // namespace

Y_UNIT_TEST_SUITE(ImdbPostImport) {
    Y_UNIT_TEST(RequiresQueryClient) {
        TImdbWorkloadParams params;
        const auto initializer = params.CreateDataInitializers().front();
        UNIT_ASSERT_EXCEPTION_CONTAINS(initializer->PostImport(), yexception, "query client is required");
    }

    Y_UNIT_TEST(AnalyzesEveryTableWithoutTransaction) {
        CheckPostImport(0);
    }

    Y_UNIT_TEST(StopsAndReportsAnalyzeFailure) {
        CheckPostImport(3);
    }
}
