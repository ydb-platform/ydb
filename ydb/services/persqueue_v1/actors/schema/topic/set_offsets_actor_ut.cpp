#include "actors.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>
#include <ydb/services/persqueue_v1/actors/set_offsets_actor.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <google/protobuf/util/time_util.h>
#include <util/thread/pool.h>

#include <optional>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::Tests::NGrpc;
using NKikimr::TEvPipeCache;
using NKikimr::TEvPQ;

namespace {

std::shared_ptr<TTopicSdkTestSetup> CreateSetup(const char* name) {
    auto setup = std::make_shared<TTopicSdkTestSetup>(name, TTopicSdkTestSetup::MakeServerSettings(), false);
    setup->GetServer().EnableLogs({NKikimrServices::PQ_SCHEMA}, NActors::NLog::PRI_DEBUG);
    return setup;
}

struct TSimulatedServer {
    THolder<TThreadPool> Pool;
    THolder<::NPersQueue::TTestServer> Server;

    ~TSimulatedServer() {
        Server.Reset();
        if (Pool) {
            Pool->Stop();
        }
    }

    NActors::TTestActorRuntime& GetRuntime() {
        return *Server->CleverServer->GetRuntime();
    }
};

std::unique_ptr<TSimulatedServer> CreateSimulatedServer() {
    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetUseRealThreads(false);

    auto out = std::make_unique<TSimulatedServer>();
    out->Server = MakeHolder<::NPersQueue::TTestServer>(settings, /*start=*/false);
    out->Server->StartServer(/*doClientInit=*/false, TString("/Root"));

    auto& runtime = out->GetRuntime();
    runtime.UpdateCurrentTime(TInstant::Now());
    out->Server->EnableLogs({NKikimrServices::PQ_SCHEMA}, NActors::NLog::PRI_DEBUG);
    out->Server->AnnoyingClient->SetNoConfigMode();

    out->Pool = MakeHolder<TThreadPool>();
    out->Pool->Start(2);
    auto* server = out->Server.Get();
    auto future = NThreading::Async([server] {
        server->AnnoyingClient->FullInit();
        return true;
    }, *out->Pool);
    static_cast<NKikimr::TTestActorRuntime&>(runtime).WaitFuture(std::move(future));
    return out;
}

template <typename TRequest, typename TResponse>
std::shared_ptr<TResultHolder<TResponse>> DoActorRequest(
    NActors::TTestActorRuntime& runtime,
    const TRequest& request,
    NActors::IActor* (*factory)(NGRpcService::IRequestOpCtx*),
    const TString& path,
    const TString& database = "/Root",
    TDuration waitTimeout = TDuration::Seconds(30))
{
    auto result = std::make_shared<TResultHolder<TResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();
    auto* ctx = new TRequestCtx<TRequest, TResponse>(request, path, database, result, edgeActor);
    runtime.Register(factory(ctx));
    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, waitTimeout);
    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    return result;
}

template <typename TResponse>
void AssertStatus(
    const std::shared_ptr<TResultHolder<TResponse>>& result,
    Ydb::StatusIds::StatusCode expected,
    const TString& substring = {})
{
    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, expected, result->Issues.ToString());
    if (!substring.empty()) {
        UNIT_ASSERT_STRING_CONTAINS(result->Issues.ToString(), substring);
    }
}

Ydb::Topic::CreateTopicRequest MakeCreateTopicRequest(const TString& path, ui32 partitions = 1) {
    Ydb::Topic::CreateTopicRequest request;
    request.set_path(path);
    request.mutable_partitioning_settings()->set_min_active_partitions(partitions);
    auto* consumer = request.add_consumers();
    consumer->set_name("user");
    return request;
}

void CreateTopic(NActors::TTestActorRuntime& runtime, const TString& path, ui32 partitions = 1) {
    auto result = DoActorRequest<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
        runtime, MakeCreateTopicRequest(path, partitions), NTopic::CreateCreateTopicActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
}

Ydb::Topic::SetOffsetsRequest MakeSetOffsetsRequest(const TString& path, const TString& consumer = "user") {
    Ydb::Topic::SetOffsetsRequest request;
    request.set_path(path);
    request.set_consumer(consumer);
    request.mutable_earliest();
    return request;
}

} // namespace

Y_UNIT_TEST_SUITE(TGrpcSetOffsetsActorTests) {

Y_UNIT_TEST(HappyPath) {
    auto setup = CreateSetup("GrpcSetOffsetsHappy");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_reset_ok";
    CreateTopic(runtime, path);

    auto result = DoActorRequest<Ydb::Topic::SetOffsetsRequest, Ydb::Topic::SetOffsetsResponse>(
        runtime, MakeSetOffsetsRequest(path), CreateSetOffsetsActor, path);
    AssertStatus(result, Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(EmptyPosition) {
    auto setup = CreateSetup("GrpcSetOffsetsNoPosition");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_reset_no_pos";
    CreateTopic(runtime, path);

    Ydb::Topic::SetOffsetsRequest request;
    request.set_path(path);
    request.set_consumer("user");
    auto result = DoActorRequest<Ydb::Topic::SetOffsetsRequest, Ydb::Topic::SetOffsetsResponse>(
        runtime, request, CreateSetOffsetsActor, path);
    AssertStatus(result, Ydb::StatusIds::BAD_REQUEST, "Position is required");
}

Y_UNIT_TEST(TopicMissing) {
    auto setup = CreateSetup("GrpcSetOffsetsMissingTopic");
    auto& runtime = setup->GetRuntime();
    Ydb::Topic::SetOffsetsRequest request = MakeSetOffsetsRequest("/Root/no_such_topic");
    auto result = DoActorRequest<Ydb::Topic::SetOffsetsRequest, Ydb::Topic::SetOffsetsResponse>(
        runtime, request, CreateSetOffsetsActor, "/Root/no_such_topic");
    AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(ConsumerMissing) {
    auto setup = CreateSetup("GrpcSetOffsetsMissingConsumer");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_reset_no_cons";
    CreateTopic(runtime, path);
    auto result = DoActorRequest<Ydb::Topic::SetOffsetsRequest, Ydb::Topic::SetOffsetsResponse>(
        runtime, MakeSetOffsetsRequest(path, "missing"), CreateSetOffsetsActor, path);
    AssertStatus(result, Ydb::StatusIds::SCHEME_ERROR, "does not exist");
}

Y_UNIT_TEST(UnauthenticatedRejectedWhenRequired) {
    auto setup = CreateSetup("GrpcSetOffsetsUnauth");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_reset_auth";
    CreateTopic(runtime, path);
    runtime.GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

    auto result = DoActorRequest<Ydb::Topic::SetOffsetsRequest, Ydb::Topic::SetOffsetsResponse>(
        runtime, MakeSetOffsetsRequest(path), CreateSetOffsetsActor, path);
    AssertStatus(result, Ydb::StatusIds::UNAUTHORIZED, "Unauthenticated access is forbidden");
}

Y_UNIT_TEST(AllPositions) {
    auto setup = CreateSetup("GrpcSetOffsetsPositions");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_reset_pos";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::SetOffsetsRequest request;
        request.set_path(path);
        request.set_consumer("user");
        request.mutable_latest();
        auto result = DoActorRequest<Ydb::Topic::SetOffsetsRequest, Ydb::Topic::SetOffsetsResponse>(
            runtime, request, CreateSetOffsetsActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }
    {
        Ydb::Topic::SetOffsetsRequest request;
        request.set_path(path);
        request.set_consumer("user");
        *request.mutable_from_written_at()->mutable_written_at() =
            ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(TInstant::Now().MilliSeconds());
        auto result = DoActorRequest<Ydb::Topic::SetOffsetsRequest, Ydb::Topic::SetOffsetsResponse>(
            runtime, request, CreateSetOffsetsActor, path);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
    }
}

Y_UNIT_TEST(PartialFailListsPartitions) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    runtime.SetRegistrationObserverFunc([](NActors::TTestActorRuntimeBase& rt, const NActors::TActorId&, const NActors::TActorId& actorId) {
        rt.EnableScheduleForActor(actorId);
    });
    const TString path = "/Root/topic_reset_partial";
    CreateTopic(runtime, path, 1);

    size_t broken = 0;
    auto* rt = &runtime;
    auto observer = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&broken, rt](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev) {
                return;
            }
            if (ev->Get()->Ev->Type() != TEvPQ::TEvSetOffsetsRequest::EventType) {
                return;
            }
            ++broken;
            const ui64 tabletId = ev->Get()->TabletId;
            const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;
            rt->Send(new NActors::IEventHandle(
                ev->Sender,
                ev->Recipient,
                new TEvPipeCache::TEvDeliveryProblem(tabletId, true),
                0,
                subscribeCookie));
            ev.Reset();
        });

    auto result = DoActorRequest<Ydb::Topic::SetOffsetsRequest, Ydb::Topic::SetOffsetsResponse>(
        runtime, MakeSetOffsetsRequest(path), CreateSetOffsetsActor, path, "/Root", TDuration::Seconds(30));
    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_UNEQUAL(*result->ResultStatus, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result->Issues.ToString(), "Failed to set offsets for partitions");
    UNIT_ASSERT_GE(broken, 5u);
}

Y_UNIT_TEST(TwoPartitionsOneFails) {
    auto server = CreateSimulatedServer();
    auto& runtime = server->GetRuntime();
    runtime.SetRegistrationObserverFunc([](NActors::TTestActorRuntimeBase& rt, const NActors::TActorId&, const NActors::TActorId& actorId) {
        rt.EnableScheduleForActor(actorId);
    });
    const TString path = "/Root/topic_reset_two";
    CreateTopic(runtime, path, 2);

    std::optional<ui64> brokenTablet;
    size_t broken = 0;
    auto* rt = &runtime;
    auto observer = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [&broken, &brokenTablet, rt](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev) {
                return;
            }
            if (ev->Get()->Ev->Type() != TEvPQ::TEvSetOffsetsRequest::EventType) {
                return;
            }
            const ui64 tabletId = ev->Get()->TabletId;
            if (!brokenTablet) {
                brokenTablet = tabletId;
            }
            if (tabletId != *brokenTablet) {
                return;
            }
            ++broken;
            const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;
            rt->Send(new NActors::IEventHandle(
                ev->Sender,
                ev->Recipient,
                new TEvPipeCache::TEvDeliveryProblem(tabletId, true),
                0,
                subscribeCookie));
            ev.Reset();
        });

    auto result = DoActorRequest<Ydb::Topic::SetOffsetsRequest, Ydb::Topic::SetOffsetsResponse>(
        runtime, MakeSetOffsetsRequest(path), CreateSetOffsetsActor, path, "/Root", TDuration::Seconds(30));
    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_UNEQUAL(*result->ResultStatus, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result->Issues.ToString(), "Failed to set offsets for partitions");
    UNIT_ASSERT_GE(broken, 5u);
}

} // TGrpcSetOffsetsActorTests

} // namespace NKikimr::NGRpcProxy::V1
