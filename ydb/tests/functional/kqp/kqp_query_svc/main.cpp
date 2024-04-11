#include <util/system/env.h>
#include <util/generic/size_literals.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/grpc/client/grpc_common.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_query/query.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>

using namespace NYdbGrpc;
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NDiscovery;
using namespace NTestHelpers;

static TString CreateHostWithPort(const TEndpointInfo& info) {
    return info.Address + ":" + ToString(info.Port);
}

using TProcessor = NYdbGrpc::IStreamRequestReadProcessor<Ydb::Query::ExecuteQueryResponsePart>::TPtr;
using TProcessorPromise = NThreading::TPromise<TProcessor>;

struct TStats {
    ui32 TotalRows = 0;
    ui32 TotalBatches = 0;
};

void DoRead(TProcessor processor, Ydb::Query::ExecuteQueryResponsePart* part, Ydb::StatusIds::StatusCode expected,
    bool& allDoneOk, TStats& stats, TProcessorPromise promise)
{
    processor->Read(part, [&allDoneOk, part, promise, processor, expected, &stats](TGrpcStatus grpcStatus) mutable {
        if (grpcStatus.GRpcStatusCode == grpc::StatusCode::OK) {
            allDoneOk &= (part->status() == expected);
            if (!allDoneOk) {
                promise.SetValue(processor);
            } else {
                stats.TotalBatches++;
                stats.TotalRows += part->result_set().rows_size();

                DoRead(processor, part, expected, allDoneOk, stats, promise);
            }
        } else {
            promise.SetValue(processor);
        }
    });
}

// rows, batches
static TStats ExecuteQuery(NYdbGrpc::TGRpcClientLow& clientLow, const TGRpcClientConfig& clientConfig,
    const TString& id, const TString& query, int batchLimit, int code, bool& allDoneOk)
{
    const Ydb::StatusIds::StatusCode expected = static_cast<Ydb::StatusIds::StatusCode>(code);
    auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

    Ydb::Query::ExecuteQueryRequest request;
    request.set_session_id(id);
    request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
    request.set_response_part_limit_bytes(batchLimit);
    request.mutable_query_content()->set_text(query);

    auto promise = NThreading::NewPromise<TProcessor>();
    TStats stats;

    Ydb::Query::ExecuteQueryResponsePart part;
    auto cb = [&allDoneOk, promise, expected, &stats, &part](TGrpcStatus grpcStatus, TProcessor processor) mutable {
        UNIT_ASSERT(grpcStatus.GRpcStatusCode == grpc::StatusCode::OK);
        DoRead(processor, &part, expected, allDoneOk, stats, promise);
    };

    connection->DoStreamRequest<Ydb::Query::ExecuteQueryRequest, Ydb::Query::ExecuteQueryResponsePart>(
        request,
        cb,
        &Ydb::Query::V1::QueryService::Stub::AsyncExecuteQuery);

    promise.GetFuture().GetValueSync()->Cancel();

    return stats;
}

Y_UNIT_TEST_SUITE(KqpQueryService)
{
    Y_UNIT_TEST(ReplyPartLimitProxyNode)
    {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto db = TQueryClient(driver);

        auto client = TDiscoveryClient(driver);
        auto res = TDiscoveryClient(driver).ListEndpoints().GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        UNIT_ASSERT(res.GetEndpointsInfo().size() > 2);

        // Create and attach to session on the node #0
        auto sessionId = CreateQuerySession(TGRpcClientConfig(CreateHostWithPort(res.GetEndpointsInfo()[0])));

        bool allDoneOk = true;

        NYdbGrpc::TGRpcClientLow clientLow;

        auto p = CheckAttach(clientLow, TGRpcClientConfig(CreateHostWithPort(res.GetEndpointsInfo()[0])), sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);

        Y_DEFER {
            p->Cancel();
        };

        UNIT_ASSERT(allDoneOk);

        const TString query = "SELECT * FROM AS_TABLE(ListReplicate(AsStruct(\"12345678\" AS Key), 100000))";

        {
            // Check range for chunk size settings
            ExecuteQuery(clientLow, TGRpcClientConfig(CreateHostWithPort(res.GetEndpointsInfo()[1])),
                sessionId, query, 48_MB, Ydb::StatusIds::BAD_REQUEST, allDoneOk); 

            UNIT_ASSERT(allDoneOk);
        }

        {
            // Check range for chunk size settings
            auto stats = ExecuteQuery(clientLow, TGRpcClientConfig(CreateHostWithPort(res.GetEndpointsInfo()[1])),
                sessionId, query, 10000, Ydb::StatusIds::SUCCESS, allDoneOk); 

            UNIT_ASSERT(allDoneOk);
            UNIT_ASSERT_VALUES_EQUAL(stats.TotalRows, 100000);
            // 100000 rows * 9 (?) byte per row / 10000 chunk size limit -> expect 90 batches
            UNIT_ASSERT(stats.TotalBatches >= 90); // but got 91 in our case
            UNIT_ASSERT(stats.TotalBatches < 100);
        }

        p->Cancel();
    }
}
