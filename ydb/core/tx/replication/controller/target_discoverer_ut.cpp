#include "private_events.h"
#include "target_discoverer.h"
#include "target_table.h"
#include "target_transfer.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/ut_helpers/test_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

NKikimrReplication::TReplicationConfig CreateConfig(const TVector<std::pair<TString, TString>>& paths) {
    NKikimrReplication::TReplicationConfig config;

    auto& specific = *config.MutableSpecific();
    for (const auto& [src, dst] : paths) {
        auto& t = *specific.AddTargets();
        t.SetSrcPath(src);
        t.SetDstPath(dst);
    }

    return config;
}

NKikimrReplication::TReplicationConfig CreateTransferConfig(const std::tuple<TString, TString, TString>& path) {
    NKikimrReplication::TReplicationConfig config;

    const auto& [src, dst, lambda] = path;
    auto& specific = *config.MutableTransferSpecific();
    auto& t = *specific.MutableTarget();
    t.SetSrcPath(src);
    t.SetDstPath(dst);
    t.SetTransformLambda(lambda);

    return config;
}

Y_UNIT_TEST_SUITE(TargetDiscoverer) {
    using namespace NTestHelpers;

    TTestTableDescription DummyTable() {
        return TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Uint32"},
            },
            .ReplicationConfig = Nothing(),
        };
    }

    TTestTopicDescription DummyTopic() {
        return TTestTopicDescription{
            .Name = "Topic",
        };
    }

    Y_UNIT_TEST(Basic) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetDstPath(), "/Root/Replicated/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Kind, TReplication::ETargetKind::Table);
    }

    Y_UNIT_TEST(IndexedTable) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTableWithIndex("/Root", *MakeTableDescription(DummyTable()),
             "Index", TVector<TString>{"value"}, NKikimrSchemeOp::EIndexTypeGlobal);

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Config->GetSrcPath(), "/Root/Table/Index");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Config->GetDstPath(), "/Root/Replicated/Table/Index/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Kind, TReplication::ETargetKind::IndexTable);
    }

    Y_UNIT_TEST(Transfer) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTopic("/Root", *MakeTopicDescription(DummyTopic()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateTransferConfig(std::tuple<TString, TString, TString>{
                "/Root/Topic", "/Root/Replicated/Table", "lambda body"
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Topic");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetDstPath(), "/Root/Replicated/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Kind, TReplication::ETargetKind::Transfer);
        auto p = std::dynamic_pointer_cast<const TTargetTransfer::TTransferConfig>(toAdd.at(0).Config);
        UNIT_ASSERT(p);
        UNIT_ASSERT_VALUES_EQUAL(p->GetTransformLambda(), "lambda body");
    }

    Y_UNIT_TEST(Negative) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root/Table", "/Root/ReplicatedTable"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(!ev->Get()->IsSuccess());

        const auto& failed = ev->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).SrcPath, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(Dirs) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.MkDir("/Root", "Dir");
        env.CreateTable("/Root/Dir", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Dir/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetDstPath(), "/Root/Replicated/Dir/Table");
    }

    Y_UNIT_TEST(SystemObjects) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));
        env.MkDir("/Root", "export-100500");
        env.CreateTable("/Root/export-100500", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Table");
    }

    Y_UNIT_TEST(InvalidCredentials) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));

        // create aux proxy
        NKikimrReplication::TStaticCredentials staticCreds;
        staticCreds.SetUser("user");
        staticCreds.SetPassword("password");
        const auto ydbProxy = env.GetRuntime().Register(CreateYdbProxy(
            env.GetEndpoint(), env.GetDatabase(), false /* ssl */, "" /* cert */, staticCreds));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, ydbProxy,
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(!ev->Get()->IsSuccess());

        const auto& failed = ev->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetStatus(), NYdb::EStatus::CLIENT_UNAUTHENTICATED);
    }
}

}
