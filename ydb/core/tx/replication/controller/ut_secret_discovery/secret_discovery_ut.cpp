#include <ydb/core/tx/replication/controller/private_events.h>
#include <ydb/core/tx/replication/controller/target_discoverer.h>
#include <ydb/core/tx/replication/controller/secret_copier.h>
#include <ydb/core/tx/replication/controller/stream_remover.h>
#include <ydb/core/tx/replication/controller/target_secret.h>
#include <ydb/core/tx/replication/controller/target_table.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

namespace {

struct TSecretRemovalSetup {
    TTestActorRuntime Runtime;
    TActorId Parent;
    TActorId Proxy;
    TActorId Remover;

    TSecretRemovalSetup() {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Parent = Runtime.AllocateEdgeActor();
        Proxy = Runtime.AllocateEdgeActor();
        Remover = Runtime.Register(CreateStreamRemover(Parent, Proxy, 1, 2,
            TReplication::ETargetKind::Secret, "/Root/source", "Stream", "/Root/secretImpl/Stream"));
        Runtime.EnableScheduleForActor(Remover);
    }

    void Allow() {
        const auto permission = Runtime.GrabEdgeEvent<TEvPrivate::TEvRequestDropStream>(Parent);
        UNIT_ASSERT_VALUES_EQUAL(permission->Sender, Remover);
        Runtime.Send(Remover, Parent, new TEvPrivate::TEvAllowDropStream());
    }

    void ReplyDrop(ui64 cookie, NYdb::EStatus status) {
        Runtime.Send(new IEventHandle(Remover, Proxy, new TEvYdbProxy::TEvDropTopicResponse(
            NYdb::TStatus(status, {})), 0, cookie));
    }

    void ReplyDescribe(ui64 cookie, NYdb::EStatus status) {
        Runtime.Send(new IEventHandle(Remover, Proxy, new TEvYdbProxy::TEvDescribeTopicResponse(
            NYdb::TStatus(status, {}), Ydb::Topic::DescribeTopicResult()), 0, cookie));
    }

    void Finish() {
        const auto result = Runtime.GrabEdgeEvent<TEvPrivate::TEvDropStreamResult>(Parent);
        UNIT_ASSERT(result->Get()->Status.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ReplicationId, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->TargetId, 2);
    }
};

}

Y_UNIT_TEST_SUITE(SecretDiscovery) {
    Y_UNIT_TEST(SecretTopicRemovalWaitsForControllerPermission) {
        TSecretRemovalSetup setup;
        UNIT_ASSERT(!setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(
            setup.Proxy, TDuration::MilliSeconds(100)));
        setup.Allow();
        const auto request = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(setup.Proxy);
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(request->Get()->GetArgs()), "/Root/secretImpl/Stream");
        setup.ReplyDrop(request->Cookie, NYdb::EStatus::SUCCESS);
        setup.Finish();
    }

    Y_UNIT_TEST(SecretTopicRemovalConfirmsMissingTopic) {
        TSecretRemovalSetup setup;
        setup.Allow();
        const auto drop = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(setup.Proxy);
        setup.ReplyDrop(drop->Cookie, NYdb::EStatus::SCHEME_ERROR);
        const auto describe = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDescribeTopicRequest>(setup.Proxy);
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(describe->Get()->GetArgs()), "/Root/secretImpl/Stream");
        UNIT_ASSERT(!setup.Runtime.GrabEdgeEvent<TEvPrivate::TEvDropStreamResult>(
            setup.Parent, TDuration::MilliSeconds(100)));
        setup.ReplyDescribe(describe->Cookie, NYdb::EStatus::SCHEME_ERROR);
        setup.Finish();
    }

    Y_UNIT_TEST(SecretTopicRemovalRetriesWhenTopicStillExists) {
        TSecretRemovalSetup setup;
        setup.Allow();
        const auto drop = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(setup.Proxy);
        setup.ReplyDrop(drop->Cookie, NYdb::EStatus::SCHEME_ERROR);
        const auto describe = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDescribeTopicRequest>(setup.Proxy);
        setup.ReplyDescribe(describe->Cookie, NYdb::EStatus::SUCCESS);
        const auto retry = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(setup.Proxy);
        setup.ReplyDrop(retry->Cookie, NYdb::EStatus::SUCCESS);
        setup.Finish();
    }

    Y_UNIT_TEST(SecretTopicRemovalRetriesErrors) {
        for (const auto status : {NYdb::EStatus::UNAVAILABLE, NYdb::EStatus::UNAUTHORIZED}) {
            TSecretRemovalSetup setup;
            setup.Allow();
            const auto drop = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(setup.Proxy);
            setup.ReplyDrop(drop->Cookie, status);
            const auto retry = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(setup.Proxy);
            setup.ReplyDrop(retry->Cookie, NYdb::EStatus::SUCCESS);
            setup.Finish();
        }
    }

    Y_UNIT_TEST(SecretTopicRemovalIgnoresOldProxyRepliesAndTimers) {
        TSecretRemovalSetup setup;
        setup.Allow();
        const auto oldDrop = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(setup.Proxy);
        setup.ReplyDrop(oldDrop->Cookie, NYdb::EStatus::SCHEME_ERROR);
        const auto oldDescribe = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDescribeTopicRequest>(setup.Proxy);
        setup.Proxy = setup.Runtime.AllocateEdgeActor();
        setup.Runtime.Send(setup.Remover, setup.Parent, new TEvPrivate::TEvUpdateYdbProxy(setup.Proxy));
        const auto drop = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(setup.Proxy);
        UNIT_ASSERT_VALUES_UNEQUAL(drop->Cookie, oldDrop->Cookie);
        setup.ReplyDescribe(oldDescribe->Cookie, NYdb::EStatus::SCHEME_ERROR);
        setup.ReplyDrop(oldDrop->Cookie, NYdb::EStatus::SUCCESS);
        setup.Runtime.Send(setup.Remover, setup.Parent, new TEvents::TEvWakeup(oldDrop->Cookie));
        UNIT_ASSERT(!setup.Runtime.GrabEdgeEvent<TEvPrivate::TEvDropStreamResult>(
            setup.Parent, TDuration::MilliSeconds(100)));
        UNIT_ASSERT(!setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDropTopicRequest>(
            setup.Proxy, TDuration::MilliSeconds(100)));
        setup.ReplyDrop(drop->Cookie, NYdb::EStatus::SUCCESS);
        setup.Finish();
    }

    Y_UNIT_TEST(DecodesEmptyValueAndRejectsIncompleteRecords) {
        NKikimrReplication::TSecretRecord source;
        source.SetSourcePath("/Root/source");
        source.SetValue("");
        NKikimrReplication::TSecretRecord decoded;
        UNIT_ASSERT(DecodeSecretRecord(TTopicMessage(0, source.SerializeAsString()), decoded));
        UNIT_ASSERT(decoded.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(decoded.GetValue(), "");
        source.ClearValue();
        UNIT_ASSERT(!DecodeSecretRecord(TTopicMessage(0, source.SerializeAsString()), decoded));
        source.SetValue("value");
        source.ClearSourcePath();
        UNIT_ASSERT(!DecodeSecretRecord(TTopicMessage(0, source.SerializeAsString()), decoded));
        UNIT_ASSERT(!DecodeSecretRecord(TTopicMessage(0, "not a protobuf record"), decoded));
    }

    Y_UNIT_TEST(SecretsDoNotParticipateInTablePathsOrLag) {
        NKikimrReplication::TReplicationConfig config;
        auto replication = MakeIntrusive<TReplication>(ui64{1}, TPathId(42, 17), config, "/Root");
        const auto tableId = replication->AddTarget(TReplication::ETargetKind::Table,
            std::make_shared<TTargetTable::TTableConfig>("/Source/table", "/Root/table"));
        replication->AddTarget(TReplication::ETargetKind::Secret,
            std::make_shared<TTargetSecret::TSecretConfig>("/Source/secret", "/Root/secret"));
        const auto paths = replication->GetTargetTablePaths();
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(paths.front(), "/Root/table");
        auto* table = replication->FindTarget(tableId);
        table->AddWorker(1);
        table->UpdateLag(1, TDuration::Seconds(3));
        UNIT_ASSERT(replication->GetLag());
        UNIT_ASSERT_VALUES_EQUAL(*replication->GetLag(), TDuration::Seconds(3));
    }

    Y_UNIT_TEST(NoSecretSetCompletesWithoutTransport) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        const auto parent = runtime.AllocateEdgeActor();
        NKikimrReplication::TReplicationConfig config;
        auto replication = MakeIntrusive<TReplication>(ui64{1}, TPathId(42, 17), config, "/Root");
        replication->AddTarget(TReplication::ETargetKind::Table,
            std::make_shared<TTargetTable::TTableConfig>("/Source/table", "/Root/table"));
        runtime.Register(CreateSecretCopier(parent, *replication));
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvSecretsCopied>(parent, TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Get()->Status.IsSuccess());
        UNIT_ASSERT(result->Get()->Created.empty());
    }

    Y_UNIT_TEST(CompletedSecretsFinishWithoutTransport) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        const auto parent = runtime.AllocateEdgeActor();
        NKikimrReplication::TReplicationConfig config;
        auto replication = MakeIntrusive<TReplication>(ui64{1}, TPathId(42, 17), config, "/Root");
        const auto id = replication->AddTarget(TReplication::ETargetKind::Secret,
            std::make_shared<TTargetSecret::TSecretConfig>("/Source/secret", "/Root/secret"));
        auto* target = replication->FindTarget(id);
        target->SetDstState(TReplication::EDstState::Done);
        target->SetStreamState(TReplication::EStreamState::Ready);
        target->SetStreamName("Stream");
        target->SetStreamConsumerName("replicationConsumer");
        runtime.Register(CreateSecretCopier(parent, *replication));
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvSecretsCopied>(parent, TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Get()->Status.IsSuccess());
        UNIT_ASSERT(result->Get()->Created.empty());
    }

    Y_UNIT_TEST(DiscoversSecretFromPathDescription) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        const auto parent = runtime.AllocateEdgeActor();
        const auto proxy = runtime.AllocateEdgeActor();
        NKikimrReplication::TReplicationConfig config;
        auto& target = *config.MutableSpecific()->AddTargets();
        target.SetSrcPath("/Root/source");
        target.SetDstPath("/Root/copy");
        runtime.Register(CreateTargetDiscoverer(parent, 1, proxy, config));
        auto path = runtime.GrabEdgeEvent<TEvYdbProxy::TEvDescribePathRequest>(proxy);
        NYdb::NScheme::TSchemeEntry entry;
        entry.Name = "source";
        entry.Type = NYdb::NScheme::ESchemeEntryType::Secret;
        runtime.Send(new IEventHandle(path->Sender, proxy,
            new TEvYdbProxy::TEvDescribePathResponse(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), entry),
            0, path->Cookie));
        TAutoPtr<IEventHandle> response;
        runtime.WaitForEdgeEvents([&](NActors::TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
            response = event;
            return true;
        }, {parent, proxy}, TDuration::Seconds(5));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL_C(response->GetTypeRewrite(), TEvPrivate::TEvDiscoveryTargetsResult::EventType,
            "Discovery must finish after DescribePath without another source request");
        UNIT_ASSERT_VALUES_EQUAL(response->GetRecipientRewrite(), parent);
        const auto* result = response->Get<TEvPrivate::TEvDiscoveryTargetsResult>();
        UNIT_ASSERT(result->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result->ToAdd.size(), 1);
        const auto& found = result->ToAdd.front();
        UNIT_ASSERT_VALUES_EQUAL(found.Kind, TReplication::ETargetKind::Secret);
        UNIT_ASSERT_VALUES_EQUAL(found.Config->GetSrcPath(), "/Root/source");
        UNIT_ASSERT_VALUES_EQUAL(found.Config->GetDstPath(), "/Root/copy");
    }

}

}
