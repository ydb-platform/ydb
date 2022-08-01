#include <ydb/core/metering/metering.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TCdcStreamTests) {
    Y_UNIT_TEST(Basic) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream/streamImpl"), {NLs::PathExist});

        TestAlterCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
            Disable {}
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateDisabled),
        });

        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream/streamImpl"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(RetentionPeriod) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        for (const auto& rp : {TDuration::Hours(12), TDuration::Days(7), TDuration::Days(60)}) {
            const auto status = rp.Seconds() <= TSchemeShard::MaxPQLifetimeSeconds
                ? NKikimrScheme::StatusAccepted
                : NKikimrScheme::StatusInvalidParameter;

            TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream%lu"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                }
                RetentionPeriodSeconds: %lu
            )", rp.Seconds(), rp.Seconds()), {status});

            if (status != NKikimrScheme::StatusAccepted) {
                continue;
            }

            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePrivatePath(runtime, Sprintf("/MyRoot/Table/Stream%lu/streamImpl", rp.Seconds())), {
                NLs::PathExist,
                NLs::RetentionPeriod(rp),
            });
        }
    }

    Y_UNIT_TEST(Negative) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestMkDir(runtime, ++txId, "/MyRoot/Table/Stream", "Dir", {NKikimrScheme::StatusNameConflict});

        TestCreateTable(runtime, ++txId, "/MyRoot/Table/Stream", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusNameConflict});

        TestCreatePQGroup(runtime, ++txId, "/MyRoot/Table/Stream", R"(
            Name: "streamImpl2"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig: { PartitionConfig { LifetimeSeconds: 3600 } }
        )", {NKikimrScheme::StatusNameConflict});
    }

    Y_UNIT_TEST(DisableProtoSourceIdInfo) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(false));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusPreconditionFailed});
    }

    Y_UNIT_TEST(CreateStream) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateCdcStream(runtime, ++txId, "/", R"(
            TableName: "MyRoot"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusNameConflict});

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "UnknownTable"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusPathDoesNotExist});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "Index"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Index"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusNameConflict});

        TestCreateCdcStream(runtime, ++txId, "/MyRoot/Table/Index", R"(
            TableName: "indexImplTable"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusNameConflict});

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusPathDoesNotExist});
    }

    Y_UNIT_TEST(AlterStream) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestAlterCdcStream(runtime, ++txId, "/", R"(
            TableName: "MyRoot"
            StreamName: "Stream"
            Disable {}
        )", {NKikimrScheme::StatusNameConflict});

        TestAlterCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "UnknownTable"
            StreamName: "Stream"
            Disable {}
        )", {NKikimrScheme::StatusPathDoesNotExist});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "Index"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Index"
            Disable {}
        )", {NKikimrScheme::StatusNameConflict});

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(DropStream) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestDropCdcStream(runtime, ++txId, "/", R"(
            TableName: "MyRoot"
            StreamName: "Stream"
        )", {NKikimrScheme::StatusNameConflict});
        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "UnknownTable"
            StreamName: "Stream"
        )", {NKikimrScheme::StatusPathDoesNotExist});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "Index"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Index"
        )", {NKikimrScheme::StatusNameConflict});
        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "UnknownStream"
        )", {NKikimrScheme::StatusPathDoesNotExist});

        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
        )", {NKikimrScheme::StatusPathDoesNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathNotExist});

        // drop table + index + stream
        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream/streamImpl"), {NLs::PathNotExist});

        // drop table + stream
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream/streamImpl"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(AlterStreamImplShouldFail) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterPQGroup(runtime, ++txId, "/MyRoot/Table/Stream", R"(
            Name: "streamImpl"
            PQTabletConfig: { PartitionConfig { LifetimeSeconds: 3600 } }
        )", {NKikimrScheme::StatusNameConflict});
    }

    Y_UNIT_TEST(DropStreamImplShouldFail) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropPQGroup(runtime, ++txId, "/MyRoot/Table/Stream", "streamImpl", {NKikimrScheme::StatusNameConflict});
    }

    Y_UNIT_TEST(CopyTableShouldNotCopyStream) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream/streamImpl"), {NLs::PathExist});

        TestCopyTable(runtime, ++txId, "/MyRoot", "TableCopy", "/MyRoot/Table");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TableCopy/Stream"), {NLs::PathNotExist});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
            CopyTableDescriptions {
              SrcPath: "/MyRoot/Table"
              DstPath: "/MyRoot/TableConsistentCopy"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TableCopy/Stream"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(MoveTableShouldFail) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestMoveTable(runtime, ++txId, "/MyRoot/Table", "/MyRoot/TableMoved", {NKikimrScheme::StatusPreconditionFailed});
    }

    Y_UNIT_TEST(CheckSchemeLimits) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        // index should not affect stream limits
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "Index"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TSchemeLimits limits;
        limits.MaxTableCdcStreams = 2;
        SetSchemeshardSchemaLimits(runtime, limits);

        for (ui32 i = 0; i <= limits.MaxTableCdcStreams; ++i) {
            const auto status = i < limits.MaxTableCdcStreams
                ? NKikimrScheme::StatusAccepted
                : NKikimrScheme::StatusResourceExhausted;

            TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream%u"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                }
            )", i), {status});
            env.TestWaitNotification(runtime, txId);
        }

        limits.MaxChildrenInDir = limits.MaxTableCdcStreams + 1 + 1 /* for index */;
        limits.MaxTableCdcStreams = limits.MaxChildrenInDir + 1;
        SetSchemeshardSchemaLimits(runtime, limits);

        for (ui32 i = limits.MaxTableCdcStreams; i <= limits.MaxChildrenInDir; ++i) {
            const auto status = i < limits.MaxChildrenInDir
                ? NKikimrScheme::StatusAccepted
                : NKikimrScheme::StatusResourceExhausted;

            TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream%u"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                }
            )", i), {status});
            env.TestWaitNotification(runtime, txId);
        }
    }

    Y_UNIT_TEST(RebootSchemeShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(Metering) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableProtoSourceIdInfo(true)
            .EnablePqBilling(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2
        )");
        env.TestWaitNotification(runtime, txId);

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_NOTICE);
        TVector<TString> meteringRecords;
        runtime.SetObserverFunc([&meteringRecords](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != NMetering::TEvMetering::EvWriteMeteringJson) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            meteringRecords.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        for (int i = 0; i < 10; ++i) {
            UNIT_ASSERT(meteringRecords.empty());
            env.SimulateSleep(runtime, TDuration::Seconds(10));
        }
    }

} // TCdcStreamTests
