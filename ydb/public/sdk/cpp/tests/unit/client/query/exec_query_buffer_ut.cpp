#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <util/string/builder.h>

#include <deque>
#include <thread>

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

Ydb::ResultSet MakeSingleInt32ResultSetProto(int32_t value) {
    Ydb::ResultSet proto;
    auto* column = proto.add_columns();
    column->set_name("v");
    column->mutable_type()->set_type_id(Ydb::Type::INT32);
    auto* row = proto.add_rows();
    row->add_items()->set_int32_value(value);
    return proto;
}

Ydb::Query::ExecuteQueryResponsePart MakeSuccessResponsePart(Ydb::ResultSet proto, uint64_t resultSetIndex = 0) {
    Ydb::Query::ExecuteQueryResponsePart part;
    part.set_status(Ydb::StatusIds::SUCCESS);
    part.set_result_set_index(resultSetIndex);
    *part.mutable_result_set() = std::move(proto);
    return part;
}

int32_t SumColumnV(const TResultSet& resultSet) {
    int32_t sum = 0;
    TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        sum += static_cast<int32_t>(parser.ColumnParser("v").GetInt32());
    }
    return sum;
}

int32_t SumColumnV(const TExecuteQueryResult& result) {
    return SumColumnV(result.GetResultSet(0));
}

class TMockExecuteQueryService : public Ydb::Query::V1::QueryService::Service {
public:
    std::deque<Ydb::Query::ExecuteQueryResponsePart> ResponseParts;

    grpc::Status ExecuteQuery(
        grpc::ServerContext*,
        const Ydb::Query::ExecuteQueryRequest*,
        grpc::ServerWriter<Ydb::Query::ExecuteQueryResponsePart>* writer) override
    {
        for (const auto& part : ResponseParts) {
            writer->Write(part);
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

std::unique_ptr<TQueryClient> MakeClient(TDriver& driver) {
    return std::make_unique<TQueryClient>(driver);
}

} // namespace

Y_UNIT_TEST_SUITE(ExecuteQueryBuffer) {

Y_UNIT_TEST(SinglePartResultUsesFastPath) {
    NTesting::InitPortManagerFromEnv();
    const auto port = NTesting::GetFreePort();
    const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

    TMockExecuteQueryService service;
    service.ResponseParts.push_back(MakeSuccessResponsePart(MakeSingleInt32ResultSetProto(42)));
    auto server = StartGrpcServer(endpoint, service);

    TDriver driver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/My/DB"));
    auto client = MakeClient(driver);

    const auto result = client->ExecuteQuery("SELECT 1", TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT(result.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(SumColumnV(result), 42);

    client.reset();
    driver.Stop(true);
}

Y_UNIT_TEST(MultiPartSameIndexFallsBackToAggregation) {
    NTesting::InitPortManagerFromEnv();
    const auto port = NTesting::GetFreePort();
    const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

    TMockExecuteQueryService service;
    service.ResponseParts.push_back(MakeSuccessResponsePart(MakeSingleInt32ResultSetProto(1)));
    service.ResponseParts.push_back(MakeSuccessResponsePart(MakeSingleInt32ResultSetProto(2)));
    auto server = StartGrpcServer(endpoint, service);

    TDriver driver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/My/DB"));
    auto client = MakeClient(driver);

    const auto result = client->ExecuteQuery("SELECT 1", TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT(result.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(SumColumnV(result), 3);

    client.reset();
    driver.Stop(true);
}

Y_UNIT_TEST(NonZeroFirstIndexUsesAggregationPath) {
    NTesting::InitPortManagerFromEnv();
    const auto port = NTesting::GetFreePort();
    const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

    TMockExecuteQueryService service;
    service.ResponseParts.push_back(MakeSuccessResponsePart(MakeSingleInt32ResultSetProto(7), 1));
    auto server = StartGrpcServer(endpoint, service);

    TDriver driver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/My/DB"));
    auto client = MakeClient(driver);

    const auto result = client->ExecuteQuery("SELECT 1", TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT(result.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(SumColumnV(result.GetResultSet(1)), 7);

    client.reset();
    driver.Stop(true);
}

Y_UNIT_TEST(StatsOnlyPartCompletesSuccessfully) {
    NTesting::InitPortManagerFromEnv();
    const auto port = NTesting::GetFreePort();
    const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

    TMockExecuteQueryService service;
    Ydb::Query::ExecuteQueryResponsePart statsPart;
    statsPart.set_status(Ydb::StatusIds::SUCCESS);
    statsPart.mutable_exec_stats()->set_process_cpu_time_us(5);
    service.ResponseParts.push_back(std::move(statsPart));
    auto server = StartGrpcServer(endpoint, service);

    TDriver driver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/My/DB"));
    auto client = MakeClient(driver);

    const auto result = client->ExecuteQuery("SELECT 1", TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT(result.IsSuccess());
    UNIT_ASSERT(result.GetResultSets().empty());
    UNIT_ASSERT(result.GetStats().has_value());
    UNIT_ASSERT_VALUES_EQUAL(result.GetStats()->GetProcessCpuTimeUs(), 5u);

    client.reset();
    driver.Stop(true);
}

}
