#include "private_events.h"
#include "target_discoverer.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

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

    Y_UNIT_TEST(Basic) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            }
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).SrcPath, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).DstPath, "/Root/Replicated/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Kind, TReplication::ETargetKind::Table);
    }

    Y_UNIT_TEST(IndexedTable) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTableWithIndex("/Root", *MakeTableDescription(DummyTable()),
             "Index", TVector<TString>{"value"}, NKikimrSchemeOp::EIndexTypeGlobal);

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            }
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).SrcPath, "/Root/Table/Index");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).DstPath, "/Root/Replicated/Table/Index/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Kind, TReplication::ETargetKind::IndexTable);
    }

    Y_UNIT_TEST(Negative) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            TVector<std::pair<TString, TString>>{
                {"/Root/Table", "/Root/ReplicatedTable"},
            }
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
            TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            }
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).SrcPath, "/Root/Dir/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).DstPath, "/Root/Replicated/Dir/Table");
    }

    Y_UNIT_TEST(SystemObjects) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));
        env.MkDir("/Root", "export-100500");
        env.CreateTable("/Root/export-100500", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            }
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).SrcPath, "/Root/Table");
    }
}

}
