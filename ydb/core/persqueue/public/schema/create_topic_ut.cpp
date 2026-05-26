#include "schema.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NPQ::NSchema {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::Tests::NGrpc;
using namespace NKikimr::NPQ::NSchema;

std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>("PQv1");
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_SCHEMA,
            NKikimrServices::PQ_MLP_DESCRIBER,
        },
        NActors::NLog::PRI_DEBUG
    );
    setup->GetServer().EnableLogs({
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PERSQUEUE_READ_BALANCER,
        },
        NActors::NLog::PRI_INFO
    );

    return setup;
}

THolder<TEvCreateTopicResponse> DoRequest(NActors::TTestActorRuntime& runtime, Ydb::Topic::CreateTopicRequest request) {
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(CreateCreateTopicActor(edge, {
        .Database = "/Root",
        .Request = request,
        .UserToken = nullptr,
        .IfNotExists = false,
        .PrepareOnly = false,
        .Cookie = 0,
    }));

    return runtime.GrabEdgeEvent<TEvCreateTopicResponse>(TDuration::Seconds(5));
}

    

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(CreateTopic) {

Y_UNIT_TEST(CreateTopicWithNameEqDB) {

    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    Ydb::Topic::CreateTopicRequest request;
    request.set_path("/Root");

    auto result = DoRequest(runtime, request);

    auto status = result->Status;
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL_C(status, Ydb::StatusIds::SCHEME_ERROR, result->ErrorMessage);
}

};

} // namespace NKikimr::NPQ::NSchema
