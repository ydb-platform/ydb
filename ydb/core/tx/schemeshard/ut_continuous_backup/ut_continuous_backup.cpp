#include <ydb/core/metering/metering.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/escape.h>
#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TContinuousBackupTests) {
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

        TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            ContinuousBackupDescription {
                StreamName: "0_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeNewImage),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
            NLs::StreamVirtualTimestamps(false),
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathExist});

        TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            Stop {}
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeNewImage),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateDisabled),
        });

        TestDropContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(TakeIncrementalBackup) {
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

        TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            ContinuousBackupDescription {
                StreamName: "0_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeNewImage),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
            NLs::StreamVirtualTimestamps(false),
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {
            NLs::PathExist,
            NLs::HasNotOffloadConfig,
        });

        TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            TakeIncrementalBackup {
                DstPath: "IncrBackupImpl"
                DstStreamPath: "1_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto pathInfo = DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl");
        auto ownerId = pathInfo.GetPathOwnerId();
        auto localId = pathInfo.GetPathId();

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {
            NLs::PathExist,
            NLs::HasOffloadConfig(Sprintf(R"(
                    IncrementalBackup: {
                        DstPath: "/MyRoot/IncrBackupImpl"
                        DstPathId: {
                            OwnerId: %)" PRIu64 R"(
                            LocalId: %)" PRIu64 R"(
                        }
                        TxId: %)" PRIu64 R"(
                    }
                )", ownerId, localId, txId)),
        });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeNewImage),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
            NLs::StreamVirtualTimestamps(false),
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl/streamImpl"), {
            NLs::PathExist,
            NLs::HasNotOffloadConfig,
        });

        // Check that stream is deleted after offloading
        env.SimulateSleep(runtime, TDuration::Seconds(5));
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl"), {
            NLs::PathExist,
            NLs::IsTable,
            NLs::CheckColumns("IncrBackupImpl", {"key", "value", "__ydb_incrBackupImpl_deleted"}, {}, {"key"}),
        });
    }
} // TCdcStreamWithInitialScanTests
