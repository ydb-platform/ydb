#include <ydb/core/metering/metering.h>
#include <ydb/core/persqueue/events/global.h>
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
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
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
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
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
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
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
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
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
            NLs::CheckColumns("IncrBackupImpl", {"key", "value", "__ydb_incrBackupImpl_changeMetadata"}, {}, {"key"}),
        });
    }

    Y_UNIT_TEST(DropAfterTakeIncrementalBackup) {
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

        // Keep the offload status away so the orphan cleaner does not drop the
        // rotated-out stream on its own: DropContinuousBackup must handle both
        // streams by itself.
        runtime.SetObserverFunc([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPersQueue::TEvOffloadStatus::EventType) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            TakeIncrementalBackup {
                DstPath: "IncrBackupImpl"
                DstStreamPath: "1_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl"), {NLs::PathExist});

        // Both the rotated-out and the current stream exist; the drop must
        // compose a single coordinated operation covering all of them —
        // a separate drop per stream would reject itself (the second
        // DropCdcStreamAtTable part sees the table under operation).
        TestDropContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
    }
} // TCdcStreamWithInitialScanTests
