#include "actors.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::Tests::NGrpc;

std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>("Topic");
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_SCHEMA,
            NKikimrServices::PQ_MLP_DESCRIBER,
        },
        NActors::NLog::PRI_DEBUG
    );
    setup->GetServer().EnableLogs({
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PERSQUEUE_READ_BALANCER,
            NKikimrServices::PQ_WRITE_PROXY
        },
        NActors::NLog::PRI_INFO
    );

    return setup;
}

template<typename TRequest, typename TResponse>
std::shared_ptr<TResultHolder<TResponse>> DoRequest(NActors::TTestActorRuntime& runtime, const TRequest& request, TString path = "/Root/test_db/topic1", TString database = "/Root/test_db") {
    auto result = std::make_shared<TResultHolder<TResponse>>();

    auto ctx = new TRequestCtx<TRequest, TResponse>(
        request,
        path,
        database,
        result
    );
    runtime.Register(CreateCreateTopicActor(ctx));

    for (int i = 0; i < 50; ++i) {
        if (result->ResultStatus) {
            break;
        }

        Sleep(TDuration::MilliSeconds(50));
    }

    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    return result;
}

    

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(CreateTopic_TopicAPI) {

Y_UNIT_TEST(CreateTopicWithNameEqDB) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    Ydb::Topic::CreateTopicRequest request;
    request.set_path("/Root");


    auto result = DoRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(runtime, request, "/Root", "/Root");

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(*status, Ydb::StatusIds::SCHEME_ERROR, result->Issues.ToString());
}

};

} // namespace NKikimr::NGRpcProxy::V1::NTopic
