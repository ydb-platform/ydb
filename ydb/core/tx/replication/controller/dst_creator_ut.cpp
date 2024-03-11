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

    template <typename T>
    void ExistingDst(NKikimrScheme::EStatus status, const TString& error, T&& mod, const TTestTableDescription& desc) {
        auto changeName = [](const TTestTableDescription& desc, const TString& name) {
            auto copy = desc;
            copy.Name = name;
            return copy;
        };

        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(changeName(desc, "Src")));
        env.CreateTable("/Root", *MakeTableDescription(mod(changeName(desc, "Dst"))));

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root"), env.GetYdbProxy(), 1 /* rid */, 1 /* tid */,
            TReplication::ETargetKind::Table, "/Root/Src", "/Root/Dst"
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, status);
        if (error) {
            UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Error, error);
        }
    }

    Y_UNIT_TEST(ExistingDst) {
        auto nop = [](const TTestTableDescription& desc) {
            return desc;
        };

        ExistingDst(NKikimrScheme::StatusSuccess, "", nop, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(KeyColumnsSizeMismatch) {
        auto addKeyColumn = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.KeyColumns.push_back("value");
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Key columns size mismatch", addKeyColumn, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(KeyColumnNameMismatch) {
        auto changeKeyColumn = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.KeyColumns = {"value"};
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Key column name mismatch", changeKeyColumn, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(ColumnsSizeMismatch) {
        auto addColumn = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.Columns.push_back({.Name = "extra", .Type = "Utf8"});
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Columns size mismatch", addColumn, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(CannotFindColumn) {
        auto changeColumnName = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.Columns[1] = {.Name = "value2", .Type = "Utf8"};
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Cannot find column", changeColumnName, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(ColumnTypeMismatch) {
        auto changeColumnType = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.Columns[1] = {.Name = "value", .Type = "Uint32"};
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Column type mismatch", changeColumnType, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }
}

}
