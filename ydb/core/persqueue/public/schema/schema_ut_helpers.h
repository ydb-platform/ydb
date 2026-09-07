#pragma once

#include "schema.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr::NPQ::NSchema::NTests {

using namespace NYdb::NTopic::NTests;

inline std::shared_ptr<TTopicSdkTestSetup> CreateSetup(
    const char* name = "CoreSchema",
    NKikimr::Tests::TServerSettings settings = TTopicSdkTestSetup::MakeServerSettings())
{
    auto setup = std::make_shared<TTopicSdkTestSetup>(name, std::move(settings), false);
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_SCHEMA,
            NKikimrServices::PQ_MLP_DESCRIBER,
        },
        NActors::NLog::PRI_DEBUG);
    return setup;
}

inline std::shared_ptr<TTopicSdkTestSetup> CreateMeteringSetup(const char* name = "CoreSchemaMetering") {
    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.PQConfig.MutableBillingMeteringConfig()->SetEnabled(true);
    return CreateSetup(name, std::move(settings));
}

inline void AssertStatus(
    const THolder<TEvSchemaResponse>& result,
    Ydb::StatusIds::StatusCode expected,
    const TString& substring = {})
{
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, expected, result->ErrorMessage);
    if (!substring.empty()) {
        UNIT_ASSERT_STRING_CONTAINS(result->ErrorMessage, substring);
    }
}

inline THolder<TEvSchemaResponse> DoCreate(
    NActors::TTestActorRuntime& runtime,
    Ydb::Topic::CreateTopicRequest request,
    const TString& database = "/Root",
    bool prepareOnly = false,
    bool ifNotExists = false)
{
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(CreateCreateTopicActor(edge, {
        .Database = database,
        .Request = std::move(request),
        .UserToken = nullptr,
        .IfNotExists = ifNotExists,
        .PrepareOnly = prepareOnly,
        .Cookie = 0,
    }));
    return runtime.GrabEdgeEvent<TEvSchemaResponse>(TDuration::Seconds(10));
}

inline THolder<TEvSchemaResponse> DoAlter(
    NActors::TTestActorRuntime& runtime,
    Ydb::Topic::AlterTopicRequest request,
    const TString& database = "/Root",
    bool prepareOnly = false,
    bool ifExists = false)
{
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(CreateAlterTopicActor(edge, {
        .Database = database,
        .Request = std::move(request),
        .UserToken = nullptr,
        .IfExists = ifExists,
        .PrepareOnly = prepareOnly,
        .Cookie = 0,
    }));
    return runtime.GrabEdgeEvent<TEvSchemaResponse>(TDuration::Seconds(10));
}

inline THolder<TEvSchemaResponse> DoDrop(
    NActors::TTestActorRuntime& runtime,
    const TString& path,
    const TString& database = "/Root",
    bool ifExists = false)
{
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(CreateDropTopicActor(edge, {
        .Database = database,
        .Path = path,
        .UserToken = nullptr,
        .IfExists = ifExists,
        .Cookie = 0,
    }));
    return runtime.GrabEdgeEvent<TEvSchemaResponse>(TDuration::Seconds(10));
}

inline NThreading::TFuture<TSchemaResponse> DoCreateViaPromise(
    NActors::TTestActorRuntime& runtime,
    Ydb::Topic::CreateTopicRequest request,
    const TString& database = "/Root")
{
    auto promise = NThreading::NewPromise<TSchemaResponse>();
    auto future = promise.GetFuture();
    runtime.Register(CreateCreateTopicActor(std::move(promise), {
        .Database = database,
        .Request = std::move(request),
        .UserToken = nullptr,
        .IfNotExists = false,
        .PrepareOnly = false,
        .Cookie = 0,
    }));
    // Drive the actor system until the promise is filled.
    for (int i = 0; i < 1000 && !future.HasValue(); ++i) {
        runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::MilliSeconds(10));
    }
    return future;
}

inline NThreading::TFuture<TSchemaResponse> DoAlterViaPromise(
    NActors::TTestActorRuntime& runtime,
    Ydb::Topic::AlterTopicRequest request,
    const TString& database = "/Root")
{
    auto promise = NThreading::NewPromise<TSchemaResponse>();
    auto future = promise.GetFuture();
    runtime.Register(CreateAlterTopicActor(std::move(promise), {
        .Database = database,
        .Request = std::move(request),
        .UserToken = nullptr,
        .IfExists = false,
        .PrepareOnly = false,
        .Cookie = 0,
    }));
    for (int i = 0; i < 1000 && !future.HasValue(); ++i) {
        runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::MilliSeconds(10));
    }
    return future;
}

inline THolder<TEvSchemaResponse> DoAddConsumer(
    NActors::TTestActorRuntime& runtime,
    const TString& path,
    Ydb::Topic::Consumer consumer,
    const TString& database = "/Root")
{
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(CreateAddConsumerActor(edge, {
        .Database = database,
        .Path = path,
        .Consumer = std::move(consumer),
        .UserToken = nullptr,
        .Cookie = 0,
    }));
    return runtime.GrabEdgeEvent<TEvSchemaResponse>(TDuration::Seconds(10));
}

inline THolder<TEvSchemaResponse> DoRemoveConsumer(
    NActors::TTestActorRuntime& runtime,
    const TString& path,
    const TString& consumerName,
    const TString& database = "/Root")
{
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(CreateRemoveConsumerActor(edge, {
        .Database = database,
        .Path = path,
        .ConsumerName = consumerName,
        .UserToken = nullptr,
        .Cookie = 0,
    }));
    return runtime.GrabEdgeEvent<TEvSchemaResponse>(TDuration::Seconds(10));
}

inline Ydb::Topic::CreateTopicRequest MakeCreateTopicRequest(const TString& path, ui32 partitions = 1) {
    Ydb::Topic::CreateTopicRequest request;
    request.set_path(path);
    request.mutable_partitioning_settings()->set_min_active_partitions(partitions);
    auto* consumer = request.add_consumers();
    consumer->set_name("user");
    consumer->mutable_streaming_consumer_type();
    return request;
}

inline void CreateTopic(NActors::TTestActorRuntime& runtime, const TString& path) {
    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(path)), Ydb::StatusIds::SUCCESS);
}

inline void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query) {
    NYdb::TDriver driver(setup.MakeDriverConfig());
    NYdb::NQuery::TQueryClient client(driver);
    auto session = client.GetSession().GetValueSync().GetSession();
    auto res = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    driver.Stop(true);
}

inline NKikimrPQ::TPartitionConfig DescribePartitionConfig(
    NActors::TTestActorRuntime& runtime,
    const TString& path,
    const TString& database = "/Root")
{
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(NDescriber::CreateDescriberActor(edge, database, {path}));
    auto response = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1u);
    const auto& topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NDescriber::EStatus::SUCCESS);
    return topic.Info->Description.GetPQTabletConfig().GetPartitionConfig();
}

inline NKikimrPQ::TPQTabletConfig DescribeTabletConfig(
    NActors::TTestActorRuntime& runtime,
    const TString& path,
    const TString& database = "/Root")
{
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(NDescriber::CreateDescriberActor(edge, database, {path}));
    auto response = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1u);
    const auto& topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NDescriber::EStatus::SUCCESS);
    return topic.Info->Description.GetPQTabletConfig();
}

} // namespace NKikimr::NPQ::NSchema::NTests
