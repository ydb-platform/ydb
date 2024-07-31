#include "dst_creator.h"
#include "private_events.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(DstCreator) {
    using namespace NTestHelpers;

    void CheckTableReplica(const TTestTableDescription& tableDesc, const NKikimrSchemeOp::TTableDescription& replicatedDesc) {
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

        const auto& replCfg = replicatedDesc.GetReplicationConfig();
        UNIT_ASSERT_VALUES_EQUAL(replCfg.GetMode(), NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(replCfg.GetConsistency(), NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_WEAK);
    }

    void Basic(const TString& replicatedPath) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        const auto tableDesc = TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        };

        env.CreateTable("/Root", *MakeTableDescription(tableDesc));
        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(), env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", replicatedPath
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);

        auto desc = env.GetDescription(replicatedPath);
        const auto& replicatedDesc = desc.GetPathDescription().GetTable();

        CheckTableReplica(tableDesc, replicatedDesc);
    }

    Y_UNIT_TEST(Basic) {
        Basic("/Root/Replicated");
    }

    Y_UNIT_TEST(WithIntermediateDir) {
        Basic("/Root/Dir/Replicated");
    }

    void WithIndex(const TString& replicatedPath, NKikimrSchemeOp::EIndexType indexType) {
        TEnv env(TFeatureFlags().SetEnableChangefeedsOnIndexTables(true));
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        const auto tableDesc = TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Uint32"},
            },
            .ReplicationConfig = Nothing(),
        };

        const TString indexName = "index_by_value";

        env.CreateTableWithIndex("/Root", *MakeTableDescription(tableDesc),
             indexName, TVector<TString>{"value"}, indexType);
        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(), env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", replicatedPath
        ));
        {
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);
        }

        auto desc = env.GetDescription(replicatedPath);
        const auto& replicatedDesc = desc.GetPathDescription().GetTable();

        CheckTableReplica(tableDesc, replicatedDesc);

        switch (indexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.TableIndexesSize(), 1);
            break;
        default:
            UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.TableIndexesSize(), 0);
            return;
        }

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(), env.GetPathId("/Root"),
            1 /* rid */, 2 /* tid */, TReplication::ETargetKind::IndexTable,
            "/Root/Table/" + indexName + "/indexImplTable", replicatedPath + "/" + indexName + "/indexImplTable"
        ));
        {
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);
        }

        {
            auto desc = env.GetDescription(replicatedPath + "/" + indexName);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetPathDescription().GetTableIndex().GetName(), indexName);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetPathDescription().GetTableIndex().GetType(), indexType);
        }

        {
            auto desc = env.GetDescription(replicatedPath + "/" + indexName + "/indexImplTable");
            Cerr << desc.DebugString() << Endl;
            const auto& indexTableDesc = desc.GetPathDescription().GetTable();
            UNIT_ASSERT_VALUES_EQUAL(indexTableDesc.KeyColumnNamesSize(), 2);
        }
    }

    Y_UNIT_TEST(WithSyncIndex) {
        WithIndex("/Root/Replicated", NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(WithSyncIndexAndIntermediateDir) {
        WithIndex("/Root/Dir/Replicated", NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(WithAsyncIndex) {
        WithIndex("/Root/Replicated", NKikimrSchemeOp::EIndexTypeGlobalAsync);
    }

    Y_UNIT_TEST(SameOwner) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.ModifyOwner("/", "Root", "user@builtin");
        env.CreateTable("/Root", *MakeTableDescription({
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        }));

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(), env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", "/Root/Replicated"
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);

        auto desc = env.GetDescription("/Root/Replicated");
        const auto& replicatedSelf = desc.GetPathDescription().GetSelf();
        UNIT_ASSERT_VALUES_EQUAL(replicatedSelf.GetOwner(), "user@builtin");
    }

    Y_UNIT_TEST(SamePartitionCount) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription({
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
            .UniformPartitions = 2,
        }));

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(), env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", "/Root/Replicated"
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);

        auto originalDesc = env.GetDescription("/Root/Table");
        const auto& originalTable = originalDesc.GetPathDescription();
        UNIT_ASSERT_VALUES_EQUAL(originalTable.TablePartitionsSize(), 2);

        auto replicatedDesc = env.GetDescription("/Root/Replicated");
        const auto& replicatedTable = replicatedDesc.GetPathDescription();
        UNIT_ASSERT_VALUES_EQUAL(originalTable.TablePartitionsSize(), replicatedTable.TablePartitionsSize());
    }

    Y_UNIT_TEST(NonExistentSrc) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root"), env.GetYdbProxy(), env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", "/Root/Replicated"
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

        auto clearConfig = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.ReplicationConfig.Clear();
            return copy;
        };

        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(clearConfig(changeName(desc, "Src"))));
        env.CreateTable("/Root", *MakeTableDescription(mod(changeName(desc, "Dst"))));

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root"), env.GetYdbProxy(), env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Src", "/Root/Dst"
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

    Y_UNIT_TEST(EmptyReplicationConfig) {
        auto clearConfig = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.ReplicationConfig.Clear();
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Empty replication config", clearConfig, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(UnsupportedReplicationMode) {
        auto changeMode = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.ReplicationConfig->Mode = TTestTableDescription::TReplicationConfig::MODE_NONE;
            copy.ReplicationConfig->Consistency = TTestTableDescription::TReplicationConfig::CONSISTENCY_UNKNOWN;
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Unsupported replication mode", changeMode, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(UnsupportedReplicationConsistency) {
        auto changeConsistency = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.ReplicationConfig->Consistency = TTestTableDescription::TReplicationConfig::CONSISTENCY_STRONG;
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Unsupported replication consistency", changeConsistency, TTestTableDescription{
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
