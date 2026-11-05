#include <ydb/core/tx/replication/controller/private_events.h>
#include <ydb/core/tx/replication/controller/target_discoverer.h>
#include <ydb/core/tx/replication/controller/secret_copier.h>
#include <ydb/core/tx/replication/controller/target_secret.h>
#include <ydb/core/tx/replication/controller/target_table.h>
#include <ydb/core/tx/replication/controller/protos/secret_copy.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(SecretDiscovery) {
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
        runtime.Register(CreateSecretCopier(parent, replication));
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
