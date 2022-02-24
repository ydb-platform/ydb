#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TCdcStreamWithRebootsTests) {
    Y_UNIT_TEST(CreateStream) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathExist});
        });
    }

    Y_UNIT_TEST(AlterStream) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", R"(
                    TableName: "Table"
                    StreamDescription {
                      Name: "Stream"
                      Mode: ECdcStreamModeKeysOnly
                      Format: ECdcStreamFormatProto
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
                });
            }

            auto request = AlterCdcStreamRequest(++t.TxId, "/MyRoot", R"(
                TableName: "Table"
                StreamName: "Stream"
                Disable {}
            )");
            t.TestEnv->ReliablePropose(runtime, request, {
                NKikimrScheme::StatusAccepted,
                NKikimrScheme::StatusMultipleModifications,
            });
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                NLs::PathExist,
                NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly),
                NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateDisabled),
            });
        });
    }

    Y_UNIT_TEST(DropStream) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", R"(
                    TableName: "Table"
                    StreamDescription {
                      Name: "Stream"
                      Mode: ECdcStreamModeKeysOnly
                      Format: ECdcStreamFormatProto
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropCdcStream(runtime, ++t.TxId, "/MyRoot", R"(
                TableName: "Table"
                StreamName: "Stream"
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathNotExist});
        });
    }

    Y_UNIT_TEST(CreateDropRecreate) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            {
                auto request = CreateCdcStreamRequest(++t.TxId, "/MyRoot", R"(
                    TableName: "Table"
                    StreamDescription {
                      Name: "Stream"
                      Mode: ECdcStreamModeKeysOnly
                      Format: ECdcStreamFormatProto
                    }
                )");
                t.TestEnv->ReliablePropose(runtime, request, {
                    NKikimrScheme::StatusAccepted,
                    NKikimrScheme::StatusAlreadyExists,
                    NKikimrScheme::StatusMultipleModifications,
                });
            }
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathExist});

            {
                auto request = DropCdcStreamRequest(++t.TxId, "/MyRoot", R"(
                    TableName: "Table"
                    StreamName: "Stream"
                )");
                t.TestEnv->ReliablePropose(runtime, request, {
                    NKikimrScheme::StatusAccepted,
                    NKikimrScheme::StatusMultipleModifications,
                });
            }
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathNotExist});

            {
                auto request = CreateCdcStreamRequest(++t.TxId, "/MyRoot", R"(
                    TableName: "Table"
                    StreamDescription {
                      Name: "Stream"
                      Mode: ECdcStreamModeKeysOnly
                      Format: ECdcStreamFormatProto
                    }
                )");
                t.TestEnv->ReliablePropose(runtime, request, {
                    NKikimrScheme::StatusAccepted,
                    NKikimrScheme::StatusAlreadyExists,
                    NKikimrScheme::StatusMultipleModifications,
                });
            }
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {NLs::PathExist});
        });
    }

} // TCdcStreamWithRebootsTests
