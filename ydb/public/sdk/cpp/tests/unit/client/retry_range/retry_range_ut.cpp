#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/ranges.h>
#include <ydb/public/sdk/cpp/src/client/result_ranges/ranges_stream_drain.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <google/protobuf/text_format.h>

#include <util/string/builder.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <deque>
#include <stdexcept>

using namespace NYdb;
using namespace NYdb::NStatusHelpers;

using NYdb::NRetry::TRetryOperationSettings;

namespace {

TRetryOperationSettings FastRetrySettings() {
    return TRetryOperationSettings()
        .MaxRetries(3)
        .FastBackoffSettings(
            TRetryOperationSettings::DefaultFastBackoffSettings()
                .SlotDuration(TDuration::MilliSeconds(1))
                .Ceiling(1));
}

TRetryOperationSettings CatchYdbExceptionsRetrySettings() {
    return FastRetrySettings().CatchYdbExceptions(true);
}

TStatus OkStatus() {
    return TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues{});
}

TStatus UnavailableStatus() {
    return TStatus(EStatus::UNAVAILABLE, NYdb::NIssue::TIssues{});
}

TResultSet MakeSingleInt32ResultSet(int32_t value) {
    const std::string text =
        "columns {\n"
        "  name: \"v\"\n"
        "  type { type_id: INT32 }\n"
        "}\n"
        "rows {\n"
        "  items { int32_value: " +
        std::to_string(value) +
        " }\n"
        "}\n";
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(TStringType{text}, &proto);
    return TResultSet(std::move(proto));
}

template <typename Part>
struct TMockStreamIterator {
    std::deque<NThreading::TFuture<Part>> Queue;

    NThreading::TFuture<Part> ReadNext() {
        UNIT_ASSERT(!Queue.empty());
        NThreading::TFuture<Part> f = std::move(Queue.front());
        Queue.pop_front();
        return f;
    }

    void Push(Part&& part) {
        Queue.push_back(NThreading::MakeFuture(std::move(part)));
    }
};

void SimulateScanDrainFailure() {
    TMockStreamIterator<NTable::TScanQueryPart> it;
    it.Push(NTable::TScanQueryPart(UnavailableStatus()));
    NYdb::NResultRangesDetail::DrainStreamIterator(it);
}

class TMockTableService : public Ydb::Table::V1::TableService::Service {
public:
    grpc::Status CreateSession(
        grpc::ServerContext*,
        const Ydb::Table::CreateSessionRequest*,
        Ydb::Table::CreateSessionResponse* response) override
    {
        Ydb::Table::CreateSessionResult result;
        result.set_session_id("fake-table-session-id");

        auto* op = response->mutable_operation();
        op->set_ready(true);
        op->set_status(Ydb::StatusIds::SUCCESS);
        op->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }
};

class TMockQueryService : public Ydb::Query::V1::QueryService::Service {
public:
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
        grpc::ServerContext*,
        const Ydb::Query::AttachSessionRequest*,
        grpc::ServerWriter<Ydb::Query::SessionState>* writer) override
    {
        Ydb::Query::SessionState state;
        state.set_status(Ydb::StatusIds::SUCCESS);
        writer->Write(state);
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

struct TTableClientFixture {
    TMockTableService TableService;
    std::unique_ptr<grpc::Server> GrpcServer;
    std::unique_ptr<TDriver> Driver;
    std::unique_ptr<NTable::TTableClient> Client;

    TTableClientFixture() {
        NTesting::InitPortManagerFromEnv();
        const auto portHolder = NTesting::GetFreePort();
        const ui16 port = portHolder;
        const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

        GrpcServer = StartGrpcServer(endpoint, TableService);
        Driver = std::make_unique<TDriver>(
            TDriverConfig()
                .SetEndpoint(endpoint)
                .SetDiscoveryMode(EDiscoveryMode::Off)
                .SetDatabase("/Root/My/DB"));
        Client = std::make_unique<NTable::TTableClient>(*Driver);
    }
};

struct TQueryClientFixture {
    TMockQueryService QueryService;
    std::unique_ptr<grpc::Server> GrpcServer;
    std::unique_ptr<TDriver> Driver;
    std::unique_ptr<NQuery::TQueryClient> Client;

    TQueryClientFixture() {
        NTesting::InitPortManagerFromEnv();
        const auto portHolder = NTesting::GetFreePort();
        const ui16 port = portHolder;
        const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

        GrpcServer = StartGrpcServer(endpoint, QueryService);
        Driver = std::make_unique<TDriver>(
            TDriverConfig()
                .SetEndpoint(endpoint)
                .SetDiscoveryMode(EDiscoveryMode::Off)
                .SetDatabase("/Root/My/DB"));
        Client = std::make_unique<NQuery::TQueryClient>(*Driver);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TTableCatchYdbExceptionsRetryTest) {
    Y_UNIT_TEST(RetryOperationSyncWithoutSessionRetriesAfterDrainException) {
        TTableClientFixture fixture;
        size_t attempts = 0;

        auto status = fixture.Client->RetryOperationSync(
            [&](NTable::TTableClient&) -> TStatus {
                ++attempts;
                if (attempts == 1) {
                    SimulateScanDrainFailure();
                }
                return OkStatus();
            },
            CatchYdbExceptionsRetrySettings());

        UNIT_ASSERT(status.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(attempts, 2u);
    }

    Y_UNIT_TEST(RetryOperationSyncWithoutSessionPropagatesDrainException) {
        TTableClientFixture fixture;

        UNIT_ASSERT_EXCEPTION(
            fixture.Client->RetryOperationSync(
                [&](NTable::TTableClient&) -> TStatus {
                    SimulateScanDrainFailure();
                    return OkStatus();
                },
                FastRetrySettings()),
            TYdbErrorException);
    }

    Y_UNIT_TEST(RetryOperationSyncWithSessionRetriesAfterDrainException) {
        TTableClientFixture fixture;
        size_t attempts = 0;

        auto status = fixture.Client->RetryOperationSync(
            [&](NTable::TSession) -> TStatus {
                ++attempts;
                if (attempts == 1) {
                    SimulateScanDrainFailure();
                }
                return OkStatus();
            },
            CatchYdbExceptionsRetrySettings());

        UNIT_ASSERT(status.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(attempts, 2u);
    }

    Y_UNIT_TEST(RetryOperationSyncWithoutSessionRecoversWithResultSetRange) {
        TTableClientFixture fixture;
        size_t attempts = 0;
        int32_t sum = 0;

        auto status = fixture.Client->RetryOperationSync(
            [&](NTable::TTableClient&) -> TStatus {
                ++attempts;
                if (attempts == 1) {
                    SimulateScanDrainFailure();
                }

                for (auto& row : TResultSetRange(MakeSingleInt32ResultSet(5))) {
                    sum += static_cast<int32_t>(row.ColumnParser("v").GetInt32());
                }
                return OkStatus();
            },
            CatchYdbExceptionsRetrySettings());

        UNIT_ASSERT(status.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(attempts, 2u);
        UNIT_ASSERT_VALUES_EQUAL(sum, 5);
    }
}

Y_UNIT_TEST_SUITE(TQueryCatchYdbExceptionsRetryTest) {
    Y_UNIT_TEST(RetryQuerySyncRetriesAfterDrainException) {
        TQueryClientFixture fixture;
        size_t attempts = 0;

        auto status = fixture.Client->RetryQuerySync(
            [&](NQuery::TSession) -> TStatus {
                ++attempts;
                if (attempts == 1) {
                    SimulateScanDrainFailure();
                }
                return OkStatus();
            },
            CatchYdbExceptionsRetrySettings());

        UNIT_ASSERT(status.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(attempts, 2u);
    }

    Y_UNIT_TEST(RetryQuerySyncPropagatesDrainException) {
        TQueryClientFixture fixture;

        UNIT_ASSERT_EXCEPTION(
            fixture.Client->RetryQuerySync(
                [&](NQuery::TSession) -> TStatus {
                    SimulateScanDrainFailure();
                    return OkStatus();
                },
                FastRetrySettings()),
            TYdbErrorException);
    }

    Y_UNIT_TEST(RetryQuerySyncPropagatesNonYdbExceptionWithCatchYdbExceptions) {
        TQueryClientFixture fixture;

        UNIT_ASSERT_EXCEPTION(
            fixture.Client->RetryQuerySync(
                [&](NQuery::TSession) -> TStatus {
                    throw std::runtime_error("user bug");
                    return OkStatus();
                },
                CatchYdbExceptionsRetrySettings()),
            std::runtime_error);
    }
}
