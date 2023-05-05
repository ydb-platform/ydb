#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TCdcStreamWithRebootsTests) {
    void CreateStream(const TMaybe<NKikimrSchemeOp::ECdcStreamState>& state = Nothing(), bool vt = false) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnableChangefeedInitialScan(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().DisableCdcAutoSwitchingToReadyStateForTests = true;

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            NKikimrSchemeOp::TCdcStreamDescription streamDesc;
            streamDesc.SetName("Stream");
            streamDesc.SetMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly);
            streamDesc.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);
            streamDesc.SetVirtualTimestamps(vt);

            if (state) {
                streamDesc.SetState(*state);
            }

            TString strDesc;
            const bool ok = google::protobuf::TextFormat::PrintToString(streamDesc, &strDesc);
            UNIT_ASSERT_C(ok, "protobuf serialization failed");

            TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                StreamDescription { %s }
            )", strDesc.c_str()));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                NLs::PathExist,
                NLs::StreamVirtualTimestamps(vt),
            });
        });
    }

    Y_UNIT_TEST(CreateStream) {
        CreateStream();
    }

    Y_UNIT_TEST(CreateStreamExplicitReady) {
        CreateStream(NKikimrSchemeOp::ECdcStreamStateReady);
    }

    Y_UNIT_TEST(CreateStreamWithInitialScan) {
        CreateStream(NKikimrSchemeOp::ECdcStreamStateScan);
    }

    Y_UNIT_TEST(CreateStreamWithVirtualTimestamps) {
        CreateStream({}, true);
    }

    Y_UNIT_TEST(DisableStream) {
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

    Y_UNIT_TEST(GetReadyStream) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnableChangefeedInitialScan(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().DisableCdcAutoSwitchingToReadyStateForTests = true;

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
                      State: ECdcStreamStateScan
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateScan),
                });
            }

            const auto lockTxId = t.TxId;
            auto request = AlterCdcStreamRequest(++t.TxId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                StreamName: "Stream"
                GetReady {
                  LockTxId: %lu
                }
            )", lockTxId));
            request->Record.MutableTransaction(0)->MutableLockGuard()->SetOwnerTxId(lockTxId);

            t.TestEnv->ReliablePropose(runtime, request, {
                NKikimrScheme::StatusAccepted,
                NKikimrScheme::StatusMultipleModifications,
            });
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                NLs::PathExist,
                NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly),
                NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
            });
        });
    }

    void DropStream(const TMaybe<NKikimrSchemeOp::ECdcStreamState>& state = Nothing()) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnableChangefeedInitialScan(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().DisableCdcAutoSwitchingToReadyStateForTests = true;

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                NKikimrSchemeOp::TCdcStreamDescription streamDesc;
                streamDesc.SetName("Stream");
                streamDesc.SetMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly);
                streamDesc.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);

                if (state) {
                    streamDesc.SetState(*state);
                }

                TString strDesc;
                const bool ok = google::protobuf::TextFormat::PrintToString(streamDesc, &strDesc);
                UNIT_ASSERT_C(ok, "protobuf serialization failed");

                TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                    TableName: "Table"
                    StreamDescription { %s }
                )", strDesc.c_str()));
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

    Y_UNIT_TEST(DropStream) {
        DropStream();
    }

    Y_UNIT_TEST(DropStreamExplicitReady) {
        DropStream(NKikimrSchemeOp::ECdcStreamStateReady);
    }

    Y_UNIT_TEST(DropStreamCreatedWithInitialScan) {
        DropStream(NKikimrSchemeOp::ECdcStreamStateScan);
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

    Y_UNIT_TEST(RacySplitAndDropTable) {
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
                runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_TRACE);
            }

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint64: 2 } }
                    }
                }
            )", TTestTxConfig::FakeHiveTablets));
            TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                    NLs::PathNotExist,
                });
            }
        });
    }

} // TCdcStreamWithRebootsTests
