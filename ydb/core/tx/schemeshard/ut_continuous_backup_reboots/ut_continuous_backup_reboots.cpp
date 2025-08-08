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

Y_UNIT_TEST_SUITE(TContinuousBackupWithRebootsTests) {
    Y_UNIT_TEST(Basic) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableProtoSourceIdInfo(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 txId = 100;

            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, txId);
            }

            TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                ContinuousBackupDescription {
                    StreamName: "0_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeNewImage),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
                    NLs::StreamVirtualTimestamps(false),
                });
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathExist});
            }

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                Stop {}
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeNewImage),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateDisabled),
                });
            }

            TestDropContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(TakeIncrementalBackup) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableProtoSourceIdInfo(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 txId = 100;

            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, txId);
            }

            TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                ContinuousBackupDescription {
                    StreamName: "0_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
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
            }

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                TakeIncrementalBackup {
                    DstPath: "IncrBackupImpl"
                    DstStreamPath: "1_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
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
            }

            t.TestEnv->SimulateSleep(runtime, TDuration::Seconds(5));
            {
                TInactiveZone inactive(activeZone);
                // Check that stream is deleted after offloading
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckColumns("IncrBackupImpl", {"key", "value", "__ydb_incrBackupImpl_deleted"}, {}, {"key"}),
                });
            }
        });
    }

    Y_UNIT_TEST(TakeSeveralIncrementalBackups) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableProtoSourceIdInfo(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 txId = 100;

            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, txId);
            }

            TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                ContinuousBackupDescription {
                    StreamName: "0_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
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
            }

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                TakeIncrementalBackup {
                    DstPath: "IncrBackupImpl1"
                    DstStreamPath: "1_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                TakeIncrementalBackup {
                    DstPath: "IncrBackupImpl2"
                    DstStreamPath: "2_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                TakeIncrementalBackup {
                    DstPath: "IncrBackupImpl3"
                    DstStreamPath: "3_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            t.TestEnv->SimulateSleep(runtime, TDuration::Seconds(5));

            {
                TInactiveZone inactive(activeZone);
                // Check that streams are deleted after offloading
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/2_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/2_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/3_continuousBackupImpl"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeNewImage),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
                    NLs::StreamVirtualTimestamps(false),
                });
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/3_continuousBackupImpl/streamImpl"), {
                    NLs::PathExist,
                    NLs::HasNotOffloadConfig,
                });

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckColumns("IncrBackupImpl1", {"key", "value", "__ydb_incrBackupImpl_deleted"}, {}, {"key"}),
                });

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl2"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckColumns("IncrBackupImpl2", {"key", "value", "__ydb_incrBackupImpl_deleted"}, {}, {"key"}),
                });

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl3"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckColumns("IncrBackupImpl3", {"key", "value", "__ydb_incrBackupImpl_deleted"}, {}, {"key"}),
                });
            }
        });
    }
} // TContinuousBackupWithRebootsTests
