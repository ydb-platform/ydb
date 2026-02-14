#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TCdcStreamWithRebootsTests) {
    void CreateStream(TTestWithReboots& t, const TMaybe<NKikimrSchemeOp::ECdcStreamState>& state = Nothing(), bool vt = false, bool onIndex = false,
        bool userSIDs = false)
    {
        t.GetTestEnvOptions()
            .EnableChangefeedInitialScan(true)
            .EnableChangefeedsOnIndexTables(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().DisableCdcAutoSwitchingToReadyStateForTests = true;
                if (!onIndex) {
                    TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "Table"
                        Columns { Name: "key" Type: "Uint64" }
                        Columns { Name: "value" Type: "Uint64" }
                        KeyColumnNames: ["key"]
                    )");
                } else {
                    TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
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
                }
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            NKikimrSchemeOp::TCdcStreamDescription streamDesc;
            streamDesc.SetName("Stream");
            streamDesc.SetMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly);
            streamDesc.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);
            streamDesc.SetVirtualTimestamps(vt);
            streamDesc.SetUserSIDs(userSIDs);

            if (state) {
                streamDesc.SetState(*state);
            }

            TString strDesc;
            const bool ok = google::protobuf::TextFormat::PrintToString(streamDesc, &strDesc);
            UNIT_ASSERT_C(ok, "protobuf serialization failed");

            const TString path = !onIndex ? "/MyRoot" : "/MyRoot/Table/Index";
            const TString tableName = !onIndex ? "Table": "indexImplTable";

            TestCreateCdcStream(runtime, ++t.TxId, path, Sprintf(R"(
                TableName: "%s"
                StreamDescription { %s }
            )", tableName.c_str(), strDesc.c_str()));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePrivatePath(runtime, path + "/" + tableName + "/Stream"), {
                NLs::PathExist,
                NLs::StreamVirtualTimestamps(vt),
                NLs::StreamUserSIDs(userSIDs),
            });
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamSimple, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        CreateStream(t);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamOnIndexTable, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        CreateStream(t, {}, false, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamExplicitReady, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        CreateStream(t, NKikimrSchemeOp::ECdcStreamStateReady);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamOnIndexTableExplicitReady, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        CreateStream(t, NKikimrSchemeOp::ECdcStreamStateReady, false, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamWithInitialScan, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        CreateStream(t, NKikimrSchemeOp::ECdcStreamStateScan);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamOnIndexTableWithInitialScan, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        CreateStream(t, NKikimrSchemeOp::ECdcStreamStateScan, false, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamWithVirtualTimestamps, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        CreateStream(t, {}, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamOnIndexTableWithVirtualTimestamps, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        CreateStream(t, {}, true, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamOnIndexTableWithUserSIDs, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        CreateStream(t, {}, true, true, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamWithAwsRegion, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        t.GetTestEnvOptions().EnableChangefeedDynamoDBStreamsFormat(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"__document_api_version", "1"}}));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream"
                  Mode: ECdcStreamModeNewAndOldImages
                  Format: ECdcStreamFormatDynamoDBStreamsJson
                  AwsRegion: "ru-central1"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeNewAndOldImages),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson),
                    NLs::StreamAwsRegion("ru-central1"),
                });
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamWithResolvedTimestamps, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
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
                  ResolvedTimestampsIntervalMs: 1000
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                    NLs::StreamResolvedTimestamps(TDuration::MilliSeconds(1000)),
                });
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateStreamWithSchemaChanges, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
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
                  Format: ECdcStreamFormatJson
                  SchemaChanges: true
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                    NLs::StreamSchemaChanges(true),
                });
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DisableStream, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(GetReadyStream, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
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

    void DropStream(TTestWithReboots& t, const TMaybe<NKikimrSchemeOp::ECdcStreamState>& state = Nothing(), bool onIndex = false) {
        t.GetTestEnvOptions()
            .EnableChangefeedInitialScan(true)
            .EnableChangefeedsOnIndexTables(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const TString path = !onIndex ? "/MyRoot" : "/MyRoot/Table/Index";
            const TString tableName = !onIndex ? "Table": "indexImplTable";

            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().DisableCdcAutoSwitchingToReadyStateForTests = true;

                if (!onIndex) {
                    TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "Table"
                        Columns { Name: "key" Type: "Uint64" }
                        Columns { Name: "value" Type: "Uint64" }
                        KeyColumnNames: ["key"]
                    )");
                } else {
                    TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
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
                }
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

                TestCreateCdcStream(runtime, ++t.TxId, path, Sprintf(R"(
                    TableName: "%s"
                    StreamDescription { %s }
                )", tableName.c_str(), strDesc.c_str()));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropCdcStream(runtime, ++t.TxId, path, Sprintf(R"(
                TableName: "%s"
                StreamName: "Stream"
            )", tableName.c_str()));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePrivatePath(runtime, path + "/" + tableName + "/Stream"), {NLs::PathNotExist});
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropStreamSimple, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DropStream(t);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropStreamOnIndexTable, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DropStream(t, {}, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropStreamExplicitReady, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DropStream(t, NKikimrSchemeOp::ECdcStreamStateReady);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropStreamOnIndexTableExplicitReady, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DropStream(t, NKikimrSchemeOp::ECdcStreamStateReady, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropStreamCreatedWithInitialScan, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DropStream(t, NKikimrSchemeOp::ECdcStreamStateScan);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropStreamOnIndexTableCreatedWithInitialScan, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DropStream(t, NKikimrSchemeOp::ECdcStreamStateScan, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropMultipleStreams, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
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
                      Name: "Stream1"
                      Mode: ECdcStreamModeKeysOnly
                      Format: ECdcStreamFormatProto
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", R"(
                    TableName: "Table"
                    StreamDescription {
                      Name: "Stream2"
                      Mode: ECdcStreamModeKeysOnly
                      Format: ECdcStreamFormatProto
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Verify both streams exist
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream1"), {NLs::PathExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream2"), {NLs::PathExist});
            }

            // Drop both streams in one go
            auto request = DropCdcStreamRequest(++t.TxId, "/MyRoot", R"(
                TableName: "Table"
                StreamName: "Stream1"
                StreamName: "Stream2"
            )");
            t.TestEnv->ReliablePropose(runtime, request, {
                NKikimrScheme::StatusAccepted,
                NKikimrScheme::StatusMultipleModifications,
            });
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                // Verify both streams are deleted
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream1"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream2"), {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateDropRecreate, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(Attributes, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
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

            auto request = CreateCdcStreamRequest(++t.TxId, "/MyRoot", R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                  UserAttributes { Key: "key" Value: "value" }
                }
            )");
            t.TestEnv->ReliablePropose(runtime, request, {
                NKikimrScheme::StatusAccepted,
                NKikimrScheme::StatusAlreadyExists,
                NKikimrScheme::StatusMultipleModifications,
            });
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                    NLs::UserAttrsHas({
                        {"key", "value"},
                    })
                });
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RacySplitAndDropTable, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(InitialScan, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        t.GetTestEnvOptions().EnableChangefeedInitialScan(true);

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

                for (ui64 i = 1; i < 10; ++i) {
                    NKikimrMiniKQL::TResult result;
                    TString error;
                    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, Sprintf(R"(
                        (
                            (let key '( '('key (Uint64 '%lu) ) ) )
                            (let row '( '('value (Uint64 '%lu) ) ) )
                            (return (AsList (UpdateRow '__user__Table key row) ))
                        )
                    )", i, 10 * i), result, error);

                    UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
                    UNIT_ASSERT_VALUES_EQUAL(error, "");
                }

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
            }

            NKikimrSchemeOp::ECdcStreamState state;
            do {
                state = DescribePrivatePath(runtime, "/MyRoot/Table/Stream")
                    .GetPathDescription().GetCdcStreamDescription().GetState();
            } while (state != NKikimrSchemeOp::ECdcStreamStateReady);
        });
    }

    bool CheckRegistrations(TTestActorRuntime& runtime, NKikimrPQ::TMessageGroupInfo::EState expectedState,
            const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TTablePartition>& tablePartitions,
            const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TPersQueueGroupDescription::TPartition>& topicPartitions)
    {
        for (const auto& topicPartition : topicPartitions) {
            auto request = MakeHolder<TEvPersQueue::TEvRequest>();
            {
                auto& record = *request->Record.MutablePartitionRequest();
                record.SetPartition(topicPartition.GetPartitionId());
                auto& cmd = *record.MutableCmdGetMaxSeqNo();
                for (const auto& tablePartition : tablePartitions) {
                    cmd.AddSourceId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(tablePartition.GetDatashardId())));
                }
            }

            const auto& sender = runtime.AllocateEdgeActor();
            ForwardToTablet(runtime, topicPartition.GetTabletId(), sender, request.Release());

            auto response = runtime.GrabEdgeEvent<TEvPersQueue::TEvResponse>(sender);
            {
                const auto& record = response->Get()->Record.GetPartitionResponse();
                const auto& result = record.GetCmdGetMaxSeqNoResult().GetSourceIdInfo();

                UNIT_ASSERT_VALUES_EQUAL(result.size(), tablePartitions.size());
                for (const auto& item: result) {
                    if (item.GetState() != expectedState) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    struct TItem {
        TString Path;
        ui32 ExpectedPartitionCount;
    };

    void CheckRegistrations(TTestActorRuntime& runtime, const TItem& table, const TItem& topic,
            const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TTablePartition>* initialTablePartitions = nullptr)
    {
        auto tableDesc = DescribePath(runtime, table.Path, true, true);
        const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT_VALUES_EQUAL(tablePartitions.size(), table.ExpectedPartitionCount);

        auto topicDesc = DescribePrivatePath(runtime, topic.Path);
        const auto& topicPartitions = topicDesc.GetPathDescription().GetPersQueueGroup().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(topicPartitions.size(), topic.ExpectedPartitionCount);

        while (true) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            if (CheckRegistrations(runtime, NKikimrPQ::TMessageGroupInfo::STATE_REGISTERED, tablePartitions, topicPartitions)) {
                break;
            }
        }

        if (initialTablePartitions) {
            UNIT_ASSERT(CheckRegistrations(runtime, NKikimrPQ::TMessageGroupInfo::STATE_UNKNOWN, *initialTablePartitions, topicPartitions));
        }
    }

    void SplitTable(TTestWithReboots& t, const TString& cdcStreamDesc) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            NKikimrScheme::TEvDescribeSchemeResult initialTableDesc;
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                initialTableDesc = DescribePath(runtime, "/MyRoot/Table", true, true);

                TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", cdcStreamDesc);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint32: 2 } }
                    }
                }
            )", TTestTxConfig::FakeHiveTablets));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                UploadRow(runtime, "/MyRoot/Table", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table", 1, {1}, {2}, {TCell::Make(Max<ui32>())}, {TCell::Make(Max<ui32>())});
                CheckRegistrations(runtime, {"/MyRoot/Table", 2}, {"/MyRoot/Table/Stream/streamImpl", 1},
                    &initialTableDesc.GetPathDescription().GetTablePartitions());
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(SplitTableSimple, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        SplitTable(t, R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(SplitTableResolvedTimestamps, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        SplitTable(t, R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
              ResolvedTimestampsIntervalMs: 1000
            }
        )");
    }

    void MergeTable(TTestWithReboots& t, const TString& cdcStreamDesc) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            NKikimrScheme::TEvDescribeSchemeResult initialTableDesc;
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                    UniformPartitionsCount: 2
                    PartitionConfig {
                      PartitioningPolicy {
                        MinPartitionsCount: 1
                      }
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                initialTableDesc = DescribePath(runtime, "/MyRoot/Table", true, true);

                TestCreateCdcStream(runtime, ++t.TxId, "/MyRoot", cdcStreamDesc);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )", TTestTxConfig::FakeHiveTablets + 0, TTestTxConfig::FakeHiveTablets + 1));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                UploadRow(runtime, "/MyRoot/Table", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table", 0, {1}, {2}, {TCell::Make(Max<ui32>())}, {TCell::Make(Max<ui32>())});
                CheckRegistrations(runtime, {"/MyRoot/Table", 1}, {"/MyRoot/Table/Stream/streamImpl", 2},
                    &initialTableDesc.GetPathDescription().GetTablePartitions());
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(MergeTableSimple, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        MergeTable(t, R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(MergeTableResolvedTimestamps, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        MergeTable(t, R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
              ResolvedTimestampsIntervalMs: 1000
            }
        )");
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RacySplitTableAndCreateStream, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
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

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint64: 2 } }
                    }
                }
            )", TTestTxConfig::FakeHiveTablets));

            AsyncCreateCdcStream(runtime, ++t.TxId, "/MyRoot", R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                }
            )");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                CheckRegistrations(runtime, {"/MyRoot/Table", 2}, {"/MyRoot/Table/Stream/streamImpl", 1});
            }
        });
    }

    void PqTransactions(TTestWithReboots& t) {
        t.GetTestEnvOptions()
            .EnableChangefeedInitialScan(true);

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
                  State: ECdcStreamStateScan
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                NKikimrSchemeOp::ECdcStreamState state;
                do {
                    runtime.SimulateSleep(TDuration::Seconds(1));
                    state = DescribePrivatePath(runtime, "/MyRoot/Table/Stream")
                        .GetPathDescription().GetCdcStreamDescription().GetState();
                } while (state != NKikimrSchemeOp::ECdcStreamStateReady);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(WithPqTransactions, 2 /*rebootBuckets*/, 1 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        PqTransactions(t);
    }

} // TCdcStreamWithRebootsTests
