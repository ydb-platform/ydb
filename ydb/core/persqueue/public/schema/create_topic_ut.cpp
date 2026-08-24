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

THolder<TEvSchemaResponse> DoRequest(NActors::TTestActorRuntime& runtime, Ydb::Topic::CreateTopicRequest request) {
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(CreateCreateTopicActor(edge, {
        .Database = "/Root",
        .Request = request,
        .UserToken = nullptr,
        .IfNotExists = false,
        .PrepareOnly = false,
        .Cookie = 0,
    }));

    return runtime.GrabEdgeEvent<TEvSchemaResponse>(TDuration::Seconds(5));
}

Ydb::Topic::CreateTopicRequest MakeCreateTopicRequest(const TString& path) {
    Ydb::Topic::CreateTopicRequest request;
    request.set_path(path);
    request.mutable_partitioning_settings()->set_min_active_partitions(1);
    auto* consumer = request.add_consumers();
    consumer->set_name("user");
    consumer->mutable_streaming_consumer_type();
    return request;
}

void AssertStatus(const THolder<TEvSchemaResponse>& result, Ydb::StatusIds::StatusCode expected, TStringBuf contains = {}) {
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, expected, result->ErrorMessage);
    if (contains) {
        UNIT_ASSERT_STRING_CONTAINS(result->ErrorMessage, contains);
    }
}

NKikimrPQ::TPQTabletConfig DescribeTabletConfig(NActors::TTestActorRuntime& runtime, const TString& path) {
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(NDescriber::CreateDescriberActor(edge, "/Root", {path}));
    auto response = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1u);
    const auto& topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT(topic.Info);
    return topic.Info->Description.GetPQTabletConfig();
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

Y_UNIT_TEST(CreateTopicWithIdAttribute) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(true);

    // FirstClass: the _id attribute is silently ignored, the Id is the topic's
    // LocalPathId set by schemeshard, never taken from the request.
    {
        const TString path = "/Root/topic_id_attr_first_class";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "1234567";
        AssertStatus(DoRequest(runtime, request), Ydb::StatusIds::SUCCESS);

        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT_VALUES_UNEQUAL(config.GetId().GetId(), 1234567u);
    }

    // Flag off: the attribute is ignored as well.
    {
        runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(false);
        const TString path = "/Root/topic_id_attr_flag_off";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "1234567";
        AssertStatus(DoRequest(runtime, request), Ydb::StatusIds::SUCCESS);

        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT(!config.HasId());
    }

    // Federation + flag on: the _id attribute is accepted as the topic Id.
    // IdTxStep is stamped 0 (sentinel = "filled at create") so writers never
    // enable the name-keyed fallback for a brand-new federation topic.
    {
        runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(true);
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        const TString path = "/Root/rt3.dc1--test_account--topic_id_attr_federation";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "1234567";
        AssertStatus(DoRequest(runtime, request), Ydb::StatusIds::SUCCESS);

        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(config.GetId().GetId(), 1234567u);
        UNIT_ASSERT_VALUES_EQUAL(config.GetId().GetOwnerId(), 0u);
        UNIT_ASSERT(config.GetId().HasTxStep());
        UNIT_ASSERT_VALUES_EQUAL(config.GetId().GetTxStep(), 0u);
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    }

    {
        runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(true);
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        const TString path = "/Root/rt3.dc1--test_account--topic_bad_id";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "not-a-number";
        AssertStatus(DoRequest(runtime, request), Ydb::StatusIds::BAD_REQUEST, "not a valid positive integer");
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    }

    {
        runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(true);
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        const TString path = "/Root/rt3.dc1--test_account--topic_zero_id";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "0";
        AssertStatus(DoRequest(runtime, request), Ydb::StatusIds::BAD_REQUEST, "must be greater than 0");
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    }
}

};

} // namespace NKikimr::NPQ::NSchema
