#include <ydb/core/metering/metering.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

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
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/continuousBackupImpl"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
            NLs::StreamVirtualTimestamps(false),
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/continuousBackupImpl/streamImpl"), {NLs::PathExist});

        TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            Stop {}
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/continuousBackupImpl"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateDisabled),
        });

        TestDropContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/continuousBackupImpl"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
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
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/continuousBackupImpl"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
            NLs::StreamVirtualTimestamps(false),
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/continuousBackupImpl/streamImpl"), {
            NLs::PathExist,
            NLs::HasNotOffloadConfig,
        });

        TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            TakeIncrementalBackup {}
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/continuousBackupImpl/streamImpl"), {
            NLs::PathExist,
            NLs::HasOffloadConfig,
        });
    }
} // TCdcStreamWithInitialScanTests
