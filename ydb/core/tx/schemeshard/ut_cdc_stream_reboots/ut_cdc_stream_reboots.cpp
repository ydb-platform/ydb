#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

using namespace NSchemeShardUT_Private;

static const TString createTableProto = R"(
    Name: "Table"
    Columns { Name: "key" Type: "Uint64" }
    Columns { Name: "value" Type: "Uint64" }
    KeyColumnNames: ["key"]
)";

static const TString createTableWithIndexProto = R"(
    TableDescription {
        Name: "Table"
        Columns { Name: "key" Type: "Uint64" }
        Columns { Name: "value" Type: "Uint64" }
        KeyColumnNames: ["key"]
    }
    IndexDescription {
        Name: "SyncIndex"
        KeyColumnNames: ["value"]
    }
)";

Y_UNIT_TEST_SUITE(TCdcStreamWithRebootsTests) {
    template <typename T>
    void CreateStream(const TMaybe<NKikimrSchemeOp::ECdcStreamState>& state = Nothing(), bool vt = false, bool tableWithIndex = false) {
        T t;
        t.GetTestEnvOptions().EnableChangefeedInitialScan(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().DisableCdcAutoSwitchingToReadyStateForTests = true;
                if (tableWithIndex) {
                    TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", createTableWithIndexProto);
                } else {
                    TestCreateTable(runtime, ++t.TxId, "/MyRoot", createTableProto);
                }
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
                AllIndexes {}
            )", strDesc.c_str()));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
                NLs::PathExist,
                NLs::StreamVirtualTimestamps(vt),
            });

            if (tableWithIndex) {
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/SyncIndex/indexImplTable/Stream"), {
                    NLs::PathExist,
                    NLs::StreamVirtualTimestamps(vt),
                });
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(CreateStream) {
        CreateStream<T>();
    }

    Y_UNIT_TEST_WITH_REBOOTS(CreateStreamTableWithIndex) {
        CreateStream<T>(Nothing(), false, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS(CreateStreamExplicitReady) {
        CreateStream<T>(NKikimrSchemeOp::ECdcStreamStateReady);
    }

    Y_UNIT_TEST_WITH_REBOOTS(CreateStreamExplicitReadyTableWithIndex) {
        CreateStream<T>(NKikimrSchemeOp::ECdcStreamStateReady, false, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS(CreateStreamWithInitialScan) {
        CreateStream<T>(NKikimrSchemeOp::ECdcStreamStateScan);
    }

    Y_UNIT_TEST_WITH_REBOOTS(CreateStreamWithInitialScanTableWithIndex) {
        CreateStream<T>(NKikimrSchemeOp::ECdcStreamStateScan, false, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS(CreateStreamWithVirtualTimestamps) {
        CreateStream<T>({}, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS(CreateStreamWithAwsRegion) {
        T t;
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

    Y_UNIT_TEST_WITH_REBOOTS(CreateStreamWithResolvedTimestamps) {
        T t;
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

    Y_UNIT_TEST_WITH_REBOOTS(DisableStream) {
        T t;
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

    Y_UNIT_TEST_WITH_REBOOTS(GetReadyStream) {
        T t;
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

    template <typename T>
    void DropStream(const TMaybe<NKikimrSchemeOp::ECdcStreamState>& state = Nothing()) {
        T t;
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

    Y_UNIT_TEST_WITH_REBOOTS(DropStream) {
        DropStream<T>();
    }

    Y_UNIT_TEST_WITH_REBOOTS(DropStreamExplicitReady) {
        DropStream<T>(NKikimrSchemeOp::ECdcStreamStateReady);
    }

    Y_UNIT_TEST_WITH_REBOOTS(DropStreamCreatedWithInitialScan) {
        DropStream<T>(NKikimrSchemeOp::ECdcStreamStateScan);
    }

    Y_UNIT_TEST_WITH_REBOOTS(CreateDropRecreate) {
        T t;
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

    Y_UNIT_TEST_WITH_REBOOTS(Attributes) {
        T t;
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

    Y_UNIT_TEST_WITH_REBOOTS(RacySplitAndDropTable) {
        T t;
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

    Y_UNIT_TEST_WITH_REBOOTS(InitialScan) {
        T t;
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

    struct TItem {
        TString Path;
        ui32 nPartitions;
    };

    void CheckRegistrations(TTestActorRuntime& runtime, const TItem& table, const TItem& topic) {
        auto tableDesc = DescribePath(runtime, table.Path, true, true);
        const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT_VALUES_EQUAL(tablePartitions.size(), table.nPartitions);

        auto topicDesc = DescribePrivatePath(runtime, topic.Path);
        const auto& topicPartitions = topicDesc.GetPathDescription().GetPersQueueGroup().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(topicPartitions.size(), topic.nPartitions);

        while (true) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            bool done = true;

            for (ui32 i = 0; i < topic.nPartitions; ++i) {
                auto request = MakeHolder<TEvPersQueue::TEvRequest>();
                {
                    auto& record = *request->Record.MutablePartitionRequest();
                    record.SetPartition(topicPartitions[i].GetPartitionId());
                    auto& cmd = *record.MutableCmdGetMaxSeqNo();
                    for (const auto& tablePartition : tablePartitions) {
                        cmd.AddSourceId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(tablePartition.GetDatashardId())));
                    }
                }

                const auto& sender = runtime.AllocateEdgeActor();
                ForwardToTablet(runtime, topicPartitions[i].GetTabletId(), sender, request.Release());

                auto response = runtime.GrabEdgeEvent<TEvPersQueue::TEvResponse>(sender);
                {
                    const auto& record = response->Get()->Record.GetPartitionResponse();
                    const auto& result = record.GetCmdGetMaxSeqNoResult().GetSourceIdInfo();

                    UNIT_ASSERT_VALUES_EQUAL(result.size(), table.nPartitions);
                    for (const auto& item: result) {
                        done &= item.GetState() == NKikimrPQ::TMessageGroupInfo::STATE_REGISTERED;
                        if (!done) {
                            break;
                        }
                    }
                }

                if (!done) {
                    break;
                }
            }

            if (done) {
                break;
            }
        }
    }

    Y_UNIT_TEST_WITH_REBOOTS(SplitTable) {
        T t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
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
                UploadRows(runtime, "/MyRoot/Table", 0, {1}, {2}, {1});
                UploadRows(runtime, "/MyRoot/Table", 1, {1}, {2}, {Max<ui32>()});
                CheckRegistrations(runtime, {"/MyRoot/Table", 2}, {"/MyRoot/Table/Stream/streamImpl", 1});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(MergeTable) {
        T t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
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

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/Table", Sprintf(R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )", TTestTxConfig::FakeHiveTablets + 0, TTestTxConfig::FakeHiveTablets + 1));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                UploadRows(runtime, "/MyRoot/Table", 0, {1}, {2}, {1, Max<ui32>()});
                CheckRegistrations(runtime, {"/MyRoot/Table", 1}, {"/MyRoot/Table/Stream/streamImpl", 2});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(RacySplitTableAndCreateStream) {
        T t;
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

} // TCdcStreamWithRebootsTests
