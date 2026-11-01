#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

void EnableGeneratedColumns(TTestActorRuntime& runtime) {
    runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(true);
    runtime.GetAppData().FeatureFlags.SetEnableGeneratedVirtual(true);
}

const NKikimrSchemeOp::TColumnDescription* FindColumn(const NKikimrScheme::TEvDescribeSchemeResult& describe, const TString& name) {
    const auto& table = describe.GetPathDescription().GetTable();
    for (const auto& column : table.GetColumns()) {
        if (column.GetName() == name) {
            return &column;
        }
    }
    return nullptr;
}

NKikimrSchemeOp::TTableDescription GetDataShardTableDescription(TTestActorRuntime& runtime, const TString& path) {
    auto describe = DescribePath(runtime, path, /* returnPartitioning */ true);
    const auto& partitions = describe.GetPathDescription().GetTablePartitions();
    UNIT_ASSERT_C(partitions.size() > 0, "no partitions for " << path);
    const ui64 datashardTabletId = partitions.Get(0).GetDatashardId();

    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(datashardTabletId, sender, new TEvDataShard::TEvGetInfoRequest(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);

    const auto tableName = TStringBuf(path).RNextTok('/');
    for (const auto& table : response->Record.GetUserTables()) {
        if (table.GetName() == tableName) {
            return table.GetDescription();
        }
    }

    UNIT_FAIL("table " << path << " not found on datashard " << datashardTabletId);
    return {};
}

const NKikimrSchemeOp::TColumnDescription* FindDataShardColumn(
    const NKikimrSchemeOp::TTableDescription& description, const TString& name)
{
    for (const auto& column : description.GetColumns()) {
        if (column.GetName() == name) {
            return &column;
        }
    }
    return nullptr;
}

void CheckGeneratedColumn(const NKikimrScheme::TEvDescribeSchemeResult& describe, const TString& name, const TString& expectedExprText,
    bool expectedStored, const TVector<TString>& expectedDependencies)
{
    const auto* column = FindColumn(describe, name);
    UNIT_ASSERT_C(column, "column '" << name << "' not found in describe result: " << describe.ShortDebugString());

    UNIT_ASSERT_C(column->HasDefaultFromExpression(), "column '" << name << "' is not generated: " << column->ShortDebugString());

    const auto& generated = column->GetDefaultFromExpression();
    UNIT_ASSERT_VALUES_EQUAL(generated.GetExprText(), expectedExprText);
    UNIT_ASSERT_VALUES_EQUAL(generated.GetStored(), expectedStored);

    TVector<TString> dependencies(generated.GetDependencyColumnNames().begin(), generated.GetDependencyColumnNames().end());
    UNIT_ASSERT_VALUES_EQUAL_C(dependencies.size(), expectedDependencies.size(), "dependency mismatch: " << generated.ShortDebugString());
    for (size_t i = 0; i < expectedDependencies.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(dependencies[i], expectedDependencies[i]);
    }
}

const TString MixedForwardChain = R"(
    Name: "Table"
    Columns { Name: "key" Type: "Uint32" }
    Columns { Name: "a" Type: "Int32" }
    Columns { Name: "g3" Type: "Int32" DefaultFromExpression {
        ExprText: "g2 + 1" Stored: false DependencyColumnNames: ["g2"]
    } }
    Columns { Name: "g2" Type: "Int32" DefaultFromExpression {
        ExprText: "g1 * 2" Stored: true DependencyColumnNames: ["g1"]
    } }
    Columns { Name: "g1" Type: "Int32" DefaultFromExpression {
        ExprText: "a + 1" Stored: false DependencyColumnNames: ["a"]
    } }
    KeyColumnNames: ["key"]
)";

void CheckRejectedDependencies(const TString& columns, const TString& error) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime);
    EnableGeneratedColumns(runtime);
    const TString schema = TStringBuilder() << R"(Name: "Table" Columns { Name: "key" Type: "Uint32" })"
        << columns << R"( KeyColumnNames: ["key"])";
    TestCreateTable(runtime, 101, "/MyRoot", schema,
        {{NKikimrScheme::StatusSchemeError, error}});
    TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {NLs::PathNotExist});
}

void CheckDependencySafeMultiDrop(bool reverseOrder) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime);
    EnableGeneratedColumns(runtime);
    TestCreateTable(runtime, 101, "/MyRoot", MixedForwardChain);
    env.TestWaitNotification(runtime, 101);
    TStringBuilder alter;
    alter << "Name: \"Table\"";
    const TVector<TString> drops = reverseOrder ? TVector<TString>{"a", "g1", "g2", "g3"}
        : TVector<TString>{"g3", "g2", "g1", "a"};
    for (const auto& name : drops) {
        alter << " DropColumns { Name: \"" << name << "\" }";
    }
    TestAlterTable(runtime, 102, "/MyRoot", alter);
    env.TestWaitNotification(runtime, 102);
    for (const auto& name : drops) {
        UNIT_ASSERT(!FindColumn(DescribePath(runtime, "/MyRoot/Table"), name));
    }
    TestAlterTable(runtime, 103, "/MyRoot", R"(
        Name: "Table" Columns { Name: "a" Type: "Int32" }
        Columns { Name: "g1" Type: "Int32" DefaultFromExpression {
            ExprText: "a + 2" Stored: true DependencyColumnNames: ["a"]
        } }
    )");
    env.TestWaitNotification(runtime, 103);
    CheckGeneratedColumn(DescribePath(runtime, "/MyRoot/Table"), "g1", "a + 2", true, {"a"});
}


}   // namespace

Y_UNIT_TEST_SUITE(TSchemeShardGeneratedColumnsTest) {
    Y_UNIT_TEST(RejectExpressionDependencyMismatch) {
        for (const TString dependencies : {"", R"(DependencyColumnNames: ["key"])", R"(DependencyColumnNames: ["a", "key"])"}) {
            CheckRejectedDependencies(TStringBuilder() << R"(
                Columns { Name: "a" Type: "Int32" }
                Columns { Name: "g" Type: "Int32" DefaultFromExpression {
                    ExprText: "a + 1" Stored: true )" << dependencies << " } }", "inconsistent dependencies");
        }
    }

    Y_UNIT_TEST(RejectConcealedUnknownExpressionReference) {
        CheckRejectedDependencies(R"(
            Columns { Name: "a" Type: "Int32" }
            Columns { Name: "g" Type: "Int32" DefaultFromExpression {
                ExprText: "missing + 1" Stored: false DependencyColumnNames: ["a"]
            } }
        )", "inconsistent dependencies");
    }

    Y_UNIT_TEST(RejectDisconnectedMalformedExpression) {
        CheckRejectedDependencies(R"(
            Columns { Name: "good" Type: "Int32" DefaultFromExpression {
                ExprText: "key + 1" Stored: true DependencyColumnNames: ["key"]
            } }
            Columns { Name: "bad" Type: "Int32" DefaultFromExpression {
                ExprText: "bad + 1" Stored: false DependencyColumnNames: []
            } }
        )", "inconsistent dependencies");
    }

    Y_UNIT_TEST(CanonicalizeDuplicateUnsortedDependencies) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);
        TestCreateTable(runtime, 101, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "a" Type: "Uint32" }
            Columns { Name: "g" Type: "Uint32" DefaultFromExpression {
                ExprText: "key + a + a" Stored: true DependencyColumnNames: ["key", "a", "key", "a"]
            } }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, 101);
        CheckGeneratedColumn(DescribePath(runtime, "/MyRoot/Table"), "g", "key + a + a", true, {"a", "key"});
    }

    Y_UNIT_TEST(DependencySafeMultiDropForwardOrder) {
        CheckDependencySafeMultiDrop(false);
    }

    Y_UNIT_TEST(DependencySafeMultiDropReverseOrder) {
        CheckDependencySafeMultiDrop(true);
    }

    Y_UNIT_TEST(RejectMalformedGeneratedColumnOnAlter) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);
        TestCreateTable(runtime, 101, "/MyRoot", MixedForwardChain);
        env.TestWaitNotification(runtime, 101);
        TestAlterTable(runtime, 102, "/MyRoot", R"(
            Name: "Table" Columns { Name: "bad" Type: "Int32" DefaultFromExpression {
                ExprText: "g1 + 1" Stored: true DependencyColumnNames: ["g2"]
            } }
        )", {{NKikimrScheme::StatusInvalidParameter, "inconsistent dependencies"}});
        auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT(!FindColumn(describe, "bad"));
        CheckGeneratedColumn(describe, "g3", "g2 + 1", false, {"g2"});
    }

    Y_UNIT_TEST(RejectNewReferenceToSimultaneouslyDroppedColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);
        TestCreateTable(runtime, 101, "/MyRoot", R"(
            Name: "Table" Columns { Name: "key" Type: "Uint32" } Columns { Name: "a" Type: "Int32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, 101);
        TestAlterTable(runtime, 102, "/MyRoot", R"(
            Name: "Table" DropColumns { Name: "a" }
            Columns { Name: "g" Type: "Int32" DefaultFromExpression {
                ExprText: "a + 1" Stored: true DependencyColumnNames: ["a"]
            } }
        )", {{NKikimrScheme::StatusInvalidParameter, "unknown column"}});
        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT(FindColumn(describe, "a"));
        UNIT_ASSERT(!FindColumn(describe, "g"));
    }

    Y_UNIT_TEST(RejectInvalidExpressionText) {
        for (const TString expression : {"", "a +"}) {
            CheckRejectedDependencies(TStringBuilder() << R"(
                Columns { Name: "a" Type: "Int32" }
                Columns { Name: "g" Type: "Int32" DefaultFromExpression { ExprText: ")"
                << expression << R"(" Stored: true } }
            )", "Failed to compile");
        }
    }

    Y_UNIT_TEST(RejectSelfCycle) {
        CheckRejectedDependencies(R"(
            Columns { Name: "g" Type: "Int32" DefaultFromExpression {
                ExprText: "g + 1" Stored: true DependencyColumnNames: ["g"]
            } }
        )", "can not reference itself");
    }

    Y_UNIT_TEST(RejectTwoNodeCycle) {
        CheckRejectedDependencies(R"(
            Columns { Name: "g1" Type: "Int32" DefaultFromExpression {
                ExprText: "g2 + 1" Stored: true DependencyColumnNames: ["g2"]
            } }
            Columns { Name: "g2" Type: "Int32" DefaultFromExpression {
                ExprText: "g1 + 1" Stored: true DependencyColumnNames: ["g1"]
            } }
        )", "Generated column dependency cycle:");
    }

    Y_UNIT_TEST(RejectDisconnectedCycle) {
        CheckRejectedDependencies(R"(
            Columns { Name: "good" Type: "Uint32" DefaultFromExpression {
                ExprText: "key + 1" Stored: true DependencyColumnNames: ["key"]
            } }
            Columns { Name: "g1" Type: "Int32" DefaultFromExpression {
                ExprText: "g2 + 1" Stored: true DependencyColumnNames: ["g2"]
            } }
            Columns { Name: "g2" Type: "Int32" DefaultFromExpression {
                ExprText: "g3 + 1" Stored: true DependencyColumnNames: ["g3"]
            } }
            Columns { Name: "g3" Type: "Int32" DefaultFromExpression {
                ExprText: "g1 + 1" Stored: true DependencyColumnNames: ["g1"]
            } }
        )", "Generated column dependency cycle:");
    }

    Y_UNIT_TEST(RejectMixedKindCycle) {
        CheckRejectedDependencies(R"(
            Columns { Name: "g1" Type: "Int32" DefaultFromExpression {
                ExprText: "g2 + 1" Stored: false DependencyColumnNames: ["g2"]
            } }
            Columns { Name: "g2" Type: "Int32" DefaultFromExpression {
                ExprText: "g1 + 1" Stored: true DependencyColumnNames: ["g1"]
            } }
        )", "Generated column dependency cycle:");
    }

    Y_UNIT_TEST(RejectUnknownDependency) {
        CheckRejectedDependencies(R"(
            Columns { Name: "g" Type: "Int32" DefaultFromExpression {
                ExprText: "missing + 1" Stored: false DependencyColumnNames: ["missing"]
            } }
        )", "unknown column");
    }

    Y_UNIT_TEST(AcyclicForwardChainSurvivesRestartAndCopy) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);
        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", MixedForwardChain);
        env.TestWaitNotification(runtime, txId);
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        TestCopyTable(runtime, ++txId, "/MyRoot", "Copy", "/MyRoot/Table");
        env.TestWaitNotification(runtime, txId);
        for (const TString path : {"/MyRoot/Table", "/MyRoot/Copy"}) {
            const auto describe = DescribePath(runtime, path);
            CheckGeneratedColumn(describe, "g1", "a + 1", false, {"a"});
            CheckGeneratedColumn(describe, "g2", "g1 * 2", true, {"g1"});
            CheckGeneratedColumn(describe, "g3", "g2 + 1", false, {"g2"});
            const auto physical = GetDataShardTableDescription(runtime, path);
            UNIT_ASSERT(FindDataShardColumn(physical, "g2"));
            UNIT_ASSERT(!FindDataShardColumn(physical, "g1"));
            UNIT_ASSERT(!FindDataShardColumn(physical, "g3"));
        }
    }

    Y_UNIT_TEST(DropIntermediateGeneratedColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);
        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", MixedForwardChain);
        env.TestWaitNotification(runtime, txId);
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(Name: "Table" DropColumns { Name: "g2" })",
            {{NKikimrScheme::StatusInvalidParameter, "it is used by generated column 'g3'"}});
        auto describe = DescribePath(runtime, "/MyRoot/Table");
        CheckGeneratedColumn(describe, "g2", "g1 * 2", true, {"g1"});
        CheckGeneratedColumn(describe, "g3", "g2 + 1", false, {"g2"});
        for (const TString name : {"g3", "g2", "g1"}) {
            TestAlterTable(runtime, ++txId, "/MyRoot",
                TStringBuilder() << "Name: \"Table\" DropColumns { Name: \"" << name << "\" }");
            env.TestWaitNotification(runtime, txId);
            UNIT_ASSERT(!FindColumn(DescribePath(runtime, "/MyRoot/Table"), name));
        }
    }

    Y_UNIT_TEST(CreateTableWithStoredGeneratedColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "a"     Type: "Int32"  }
              Columns { Name: "b"     Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        CheckGeneratedColumn(describe, "sum",
            /* expr */ "a + b",
            /* stored */ true,
            /* dependencies */ { "a", "b" });

        // Non-generated columns keep no generated descriptor
        const auto* keyColumn = FindColumn(describe, "key");
        UNIT_ASSERT(keyColumn && !keyColumn->HasDefaultFromExpression());
    }

    Y_UNIT_TEST(CreateTableWithVirtualGeneratedColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "first" Type: "Utf8"   }
              Columns { Name: "last"  Type: "Utf8"   }
              Columns {
                  Name: "full"
                  Type: "Utf8"
                  DefaultFromExpression {
                      ExprText: "first || \" \" || last"
                      Stored: false
                      DependencyColumnNames: ["first", "last"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        CheckGeneratedColumn(describe, "full",
            /* expr */ "first || \" \" || last",
            /* stored */ false,
            /* dependencies */ { "first", "last" });
    }

    Y_UNIT_TEST(VirtualColumnAbsentFromDataShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "a" Type: "Int32" }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + 1"
                      Stored: true
                      DependencyColumnNames: ["a"]
                  }
              }
              Columns {
                  Name: "diff"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a - 1"
                      Stored: false
                      DependencyColumnNames: ["a"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        CheckGeneratedColumn(describe, "diff", "a - 1", /* expectedStored */ false, { "a" });

        const auto datashardSchema = GetDataShardTableDescription(runtime, "/MyRoot/Table");
        UNIT_ASSERT(FindDataShardColumn(datashardSchema, "key"));
        UNIT_ASSERT(FindDataShardColumn(datashardSchema, "a"));
        UNIT_ASSERT(FindDataShardColumn(datashardSchema, "sum"));
        UNIT_ASSERT_C(!FindDataShardColumn(datashardSchema, "diff"),
            "virtual generated column leaked into the datashard schema");
    }

    Y_UNIT_TEST(DropVirtualColumnDoesNotReachDataShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "a" Type: "Int32" }
              Columns {
                  Name: "diff"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a - 1"
                      Stored: false
                      DependencyColumnNames: ["a"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              DropColumns { Name: "diff" }
        )");
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT(!FindColumn(DescribePath(runtime, "/MyRoot/Table"), "diff"));
        UNIT_ASSERT(!FindDataShardColumn(GetDataShardTableDescription(runtime, "/MyRoot/Table"), "diff"));

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "b" Type: "Int32" }
        )");
        env.TestWaitNotification(runtime, txId);
        UNIT_ASSERT(FindDataShardColumn(GetDataShardTableDescription(runtime, "/MyRoot/Table"), "b"));
    }

    Y_UNIT_TEST(GeneratedColumnSurvivesSchemeShardRestart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "a"     Type: "Int32"  }
              Columns { Name: "b"     Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              Columns {
                  Name: "diff"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a - b"
                      Stored: false
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto checkSchema = [&]() {
            auto describe = DescribePath(runtime, "/MyRoot/Table");
            CheckGeneratedColumn(describe, "sum", "a + b", /* expectedStored */ true, { "a", "b" });
            CheckGeneratedColumn(describe, "diff", "a - b", /* expectedStored */ false, { "a", "b" });
        };

        checkSchema();

        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        checkSchema();
    }

    Y_UNIT_TEST(DropDependencyColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "a"     Type: "Int32"  }
              Columns { Name: "b"     Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              DropColumns { Name: "a" }
        )",
            { { NKikimrScheme::StatusInvalidParameter, "it is used by generated column 'sum'" } });

        // The table is untouched: both the dependency and the generated column remain
        auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT(FindColumn(describe, "a"));
        CheckGeneratedColumn(describe, "sum", "a + b", /* expectedStored */ true, { "a", "b" });
    }

    Y_UNIT_TEST(DropGeneratedColumnAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "a"     Type: "Int32"  }
              Columns { Name: "b"     Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              DropColumns { Name: "sum" }
        )");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT(!FindColumn(describe, "sum"));
        UNIT_ASSERT(FindColumn(describe, "a"));
        UNIT_ASSERT(FindColumn(describe, "b"));

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              DropColumns { Name: "a" }
        )");
        env.TestWaitNotification(runtime, txId);

        describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT(!FindColumn(describe, "sum"));
        UNIT_ASSERT(!FindColumn(describe, "a"));
        UNIT_ASSERT(FindColumn(describe, "b"));
    }

    Y_UNIT_TEST(AlterDependencyColumnSetNotNullRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "a"     Type: "Int32"  }
              Columns { Name: "b"     Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "a" NotNull: true }
        )",
            { { NKikimrScheme::StatusInvalidParameter, "Cannot set NotNull to true on column 'a'" } });

        // Schema untouched
        auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* a = FindColumn(describe, "a");
        UNIT_ASSERT(a && !a->GetNotNull());
        CheckGeneratedColumn(describe, "sum", "a + b", /* expectedStored */ true, { "a", "b" });
    }

    Y_UNIT_TEST(DropNotNullOnDependencyColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableNotNullDataColumns(true));
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32" NotNull: true }
              Columns { Name: "b"   Type: "Int32" }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "a" NotNull: false }
        )",
            { { NKikimrScheme::StatusInvalidParameter, "Can't change nullability of column 'a': it is used by generated column 'sum'" } });

        // Dependency stays NOT NULL, generated column intact
        auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* a = FindColumn(describe, "a");
        UNIT_ASSERT(a && a->GetNotNull());
        CheckGeneratedColumn(describe, "sum", "a + b", /* expectedStored */ true, { "a", "b" });
    }

    Y_UNIT_TEST(SetAndDropDefaultOnDependencyColumnAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "a"     Type: "Int32"  }
              Columns { Name: "b"     Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Set a literal default on the dependency column 'a'
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns {
                  Name: "a"
                  DefaultFromLiteral {
                      type { optional_type { item { type_id: INT32 } } }
                      value { items { int32_value: 42 } }
                  }
              }
        )");
        env.TestWaitNotification(runtime, txId);

        {
            auto describe = DescribePath(runtime, "/MyRoot/Table");
            const auto* a = FindColumn(describe, "a");
            UNIT_ASSERT(a && a->HasDefaultFromLiteral());
            // Generated column is unaffected
            CheckGeneratedColumn(describe, "sum", "a + b", /* expectedStored */ true, { "a", "b" });
        }

        // Drop the default again
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "a" EmptyDefault: NULL_VALUE }
        )");
        env.TestWaitNotification(runtime, txId);

        {
            auto describe = DescribePath(runtime, "/MyRoot/Table");
            const auto* a = FindColumn(describe, "a");
            UNIT_ASSERT(a && !a->HasDefaultFromLiteral());
            CheckGeneratedColumn(describe, "sum", "a + b", /* expectedStored */ true, { "a", "b" });
        }
    }

    Y_UNIT_TEST(RenameTableWithGeneratedColumnPreserved) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "a"     Type: "Int32"  }
              Columns { Name: "b"     Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestMoveTable(runtime, ++txId, "/MyRoot/Table", "/MyRoot/TableRenamed");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), { NLs::PathNotExist });

        auto describe = DescribePath(runtime, "/MyRoot/TableRenamed");
        CheckGeneratedColumn(describe, "sum", "a + b", /* expectedStored */ true, { "a", "b" });
    }

    Y_UNIT_TEST(TtlOnGeneratedColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32"    }
              Columns { Name: "created" Type: "Timestamp" }
              Columns {
                  Name: "expires"
                  Type: "Timestamp"
                  DefaultFromExpression {
                      ExprText: "created"
                      Stored: true
                      DependencyColumnNames: ["created"]
                  }
              }
              KeyColumnNames: ["key"]
              TTLSettings {
                  Enabled { ColumnName: "expires" ExpireAfterSeconds: 3600 }
              }
        )",
            { { NKikimrScheme::StatusSchemeError, "Cannot enable TTL on generated column: 'expires'" } });
    }

    Y_UNIT_TEST(TtlOnDependencyColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32"    }
              Columns { Name: "created" Type: "Timestamp" }
              Columns {
                  Name: "expires"
                  Type: "Timestamp"
                  DefaultFromExpression {
                      ExprText: "created"
                      Stored: true
                      DependencyColumnNames: ["created"]
                  }
              }
              KeyColumnNames: ["key"]
              TTLSettings {
                  Enabled { ColumnName: "created" ExpireAfterSeconds: 3600 }
              }
        )",
            { { NKikimrScheme::StatusSchemeError, "it is used by generated column 'expires'" } });
    }

    Y_UNIT_TEST(AlterFamilyOnGeneratedColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "a"     Type: "Int32"  }
              Columns { Name: "b"     Type: "Int32"  }
              Columns {
                  Name: "sum_stored"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              Columns {
                  Name: "sum_virtual"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: false
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
              PartitionConfig {
                ColumnFamilies {
                  Id: 0
                  StorageConfig { SysLog {} Log {} }
                }
                ColumnFamilies {
                  Name: "alt"
                }
              }
        )");
        env.TestWaitNotification(runtime, txId);

        // STORED: family change to the existing "alt" family is accepted
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "sum_stored" FamilyName: "alt" }
        )");
        env.TestWaitNotification(runtime, txId);

        // VIRTUAL: family change is rejected
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "sum_virtual" FamilyName: "alt" }
        )",
            { { NKikimrScheme::StatusInvalidParameter, "Cannot set column family for virtual generated column 'sum_virtual'" } });
    }

    Y_UNIT_TEST(CreateFamilyOnGeneratedColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        const char* familyConfig = R"(
              PartitionConfig {
                ColumnFamilies {
                  Id: 0
                  StorageConfig { SysLog {} Log {} }
                }
                ColumnFamilies {
                  Name: "alt"
                }
              }
        )";

        // STORED generated column in a non-default family: accepted
        TestCreateTable(runtime, ++txId, "/MyRoot", TStringBuilder() << R"(
              Name: "StoredTable"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32"  }
              Columns { Name: "b"   Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  FamilyName: "alt"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )" << familyConfig);
        env.TestWaitNotification(runtime, txId);

        {
            auto describe = DescribePath(runtime, "/MyRoot/StoredTable");
            const auto* sum = FindColumn(describe, "sum");
            UNIT_ASSERT(sum && sum->HasDefaultFromExpression() && sum->GetDefaultFromExpression().GetStored());
            UNIT_ASSERT_VALUES_UNEQUAL(sum->GetFamily(), 0u);
        }

        // VIRTUAL generated column in a non-default family: rejected
        TestCreateTable(runtime, ++txId, "/MyRoot", TStringBuilder() << R"(
              Name: "VirtualTable"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32"  }
              Columns { Name: "b"   Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  FamilyName: "alt"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: false
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )" << familyConfig,
            { { NKikimrScheme::StatusSchemeError, "Cannot set column family for virtual generated column 'sum'" } });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/VirtualTable"), { NLs::PathNotExist });
    }

    Y_UNIT_TEST(AlterGeneratedColumnNotNullRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32"  }
              Columns { Name: "b"   Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // SET NOT NULL: caught by the general "NotNull only via the dedicated flow" guard
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "sum" NotNull: true }
        )",
            { { NKikimrScheme::StatusInvalidParameter, "Cannot set NotNull to true on column 'sum'" } });

        // DROP NOT NULL: caught by the generated-column guard
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "sum" NotNull: false }
        )",
            { { NKikimrScheme::StatusInvalidParameter, "Can't change nullability of generated column 'sum'" } });

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        CheckGeneratedColumn(describe, "sum", "a + b", /* expectedStored */ true, { "a", "b" });
    }

    Y_UNIT_TEST(AlterDependencyNotNullAllowedAfterDropGenerated) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableNotNullDataColumns(true));
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32" NotNull: true }
              Columns { Name: "b"   Type: "Int32" }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + b"
                      Stored: true
                      DependencyColumnNames: ["a", "b"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // While the generated column exists, DROP NOT NULL on the dependency is rejected
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "a" NotNull: false }
        )",
            { { NKikimrScheme::StatusInvalidParameter, "Can't change nullability of column 'a': it is used by generated column 'sum'" } });

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              DropColumns { Name: "sum" }
        )");
        env.TestWaitNotification(runtime, txId);

        // After the drop, DROP NOT NULL on the former dependency succeeds
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "a" NotNull: false }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            auto describe = DescribePath(runtime, "/MyRoot/Table");
            const auto* a = FindColumn(describe, "a");
            UNIT_ASSERT(a && !a->GetNotNull());
        }

        // SET NOT NULL (bypassing the general guard via an internal transaction) also succeeds now
        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(AlterTableRequest(++txId, "/MyRoot", R"(
                  Name: "Table"
                  Columns { Name: "a" NotNull: true }
            )")));
        TestModificationResults(runtime, txId, { { NKikimrScheme::StatusAccepted } });
        env.TestWaitNotification(runtime, txId);
        {
            auto describe = DescribePath(runtime, "/MyRoot/Table");
            const auto* a = FindColumn(describe, "a");
            UNIT_ASSERT(a && a->GetNotNull());
        }
    }

    Y_UNIT_TEST(AddTtlOnDependencyAllowedAfterDropGenerated) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32"    }
              Columns { Name: "created" Type: "Timestamp" }
              Columns {
                  Name: "expires"
                  Type: "Timestamp"
                  DefaultFromExpression {
                      ExprText: "created"
                      Stored: true
                      DependencyColumnNames: ["created"]
                  }
              }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Rejected while the generated column depends on "created"
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              TTLSettings { Enabled { ColumnName: "created" ExpireAfterSeconds: 3600 } }
        )",
            { { NKikimrScheme::StatusInvalidParameter, "it is used by generated column 'expires'" } });

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              DropColumns { Name: "expires" }
        )");
        env.TestWaitNotification(runtime, txId);

        // Allowed after the generated column is gone
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              TTLSettings { Enabled { ColumnName: "created" ExpireAfterSeconds: 3600 } }
        )");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT_VALUES_EQUAL(describe.GetPathDescription().GetTable().GetTTLSettings().GetEnabled().GetColumnName(), "created");
    }

    Y_UNIT_TEST(StoredGeneratedColumnFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedVirtual(true);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + 1"
                      Stored: true
                      DependencyColumnNames: ["a"]
                  }
              }
              KeyColumnNames: ["key"]
        )",
            { { NKikimrScheme::StatusSchemeError, "STORED GENERATED columns are disabled" } });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), { NLs::PathNotExist });
    }

    Y_UNIT_TEST(VirtualGeneratedColumnFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(true);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32"  }
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + 1"
                      Stored: false
                      DependencyColumnNames: ["a"]
                  }
              }
              KeyColumnNames: ["key"]
        )",
            { { NKikimrScheme::StatusSchemeError, "VIRTUAL GENERATED columns are disabled" } });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), { NLs::PathNotExist });
    }

    Y_UNIT_TEST(AlterAddStoredGeneratedColumnFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableGeneratedColumns(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32"  }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(false);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns {
                  Name: "sum"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a + 1"
                      Stored: true
                      DependencyColumnNames: ["a"]
                  }
              }
        )",
            { { NKikimrScheme::StatusInvalidParameter, "STORED GENERATED columns are disabled" } });

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT(!FindColumn(describe, "sum"));
    }
}
