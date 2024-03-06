#include "dst_creator.h"
#include "private_events.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(DstCreator) {
    using namespace NTestHelpers;

    Y_UNIT_TEST(Basic) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        const auto tableDesc = TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        };

        env.CreateTable("/Root", *MakeTableDescription(tableDesc));
        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(), 1 /* rid */, 1 /* tid */,
            TReplication::ETargetKind::Table, "/Root/Table", "/Root/Replicated"
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);

        auto desc = env.GetDescription("/Root/Replicated");
        const auto& replicatedDesc = desc.GetPathDescription().GetTable();

        UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.KeyColumnNamesSize(), tableDesc.KeyColumns.size());
        for (ui32 i = 0; i < replicatedDesc.KeyColumnNamesSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.GetKeyColumnNames(i), tableDesc.KeyColumns[i]);
        }

        UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.ColumnsSize(), tableDesc.Columns.size());
        for (ui32 i = 0; i < replicatedDesc.ColumnsSize(); ++i) {
            auto pred = [name = replicatedDesc.GetColumns(i).GetName()](const auto& column) {
                return name == column.Name;
            };

            UNIT_ASSERT(FindIfPtr(tableDesc.Columns, pred));
        }
    }

    Y_UNIT_TEST(NonExistentSrc) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root"), env.GetYdbProxy(), 1 /* rid */, 1 /* tid */,
            TReplication::ETargetKind::Table, "/Root/Table", "/Root/Replicated"
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSchemeError);
    }
}

}
