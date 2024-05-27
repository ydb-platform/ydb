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
            NLs::StreamVirtualTimestamps(false),
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

    Y_UNIT_TEST(VirtualTimestamps) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableProtoSourceIdInfo(true)
            .EnableChangefeedDynamoDBStreamsFormat(true)
            .EnableChangefeedDebeziumJsonFormat(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        for (const char* format : TVector<const char*>{"Proto", "Json"}) {
            TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream%s"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormat%s
                  VirtualTimestamps: true
                }
            )", format, format));
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePrivatePath(runtime, Sprintf("/MyRoot/Table/Stream%s", format)), {
                NLs::StreamVirtualTimestamps(true),
            });
        }

        for (const char* format : TVector<const char*>{"DynamoDBStreamsJson", "DebeziumJson"}) {
            TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream%s"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormat%s
                  VirtualTimestamps: true
                }
            )", format, format), {NKikimrScheme::StatusInvalidParameter});
        }
    }

    Y_UNIT_TEST(ResolvedTimestamps) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableProtoSourceIdInfo(true)
            .EnableChangefeedDynamoDBStreamsFormat(true)
            .EnableChangefeedDebeziumJsonFormat(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        for (const char* format : TVector<const char*>{"Proto", "Json"}) {
            TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream%s"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormat%s
                  ResolvedTimestampsIntervalMs: 1000
                }
            )", format, format));
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePrivatePath(runtime, Sprintf("/MyRoot/Table/Stream%s", format)), {
                NLs::StreamResolvedTimestamps(TDuration::MilliSeconds(1000)),
            });
        }

        for (const char* format : TVector<const char*>{"DynamoDBStreamsJson", "DebeziumJson"}) {
            TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                StreamDescription {
                  Name: "Stream%s"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormat%s
                  ResolvedTimestampsIntervalMs: 1000
                }
            )", format, format), {NKikimrScheme::StatusInvalidParameter});
        }
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

    Y_UNIT_TEST(TopicPartitions) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        for (const auto& keyType : TVector<TString>{"Uint64", "Uint32", "Utf8"}) {
            const auto status = keyType != "Utf8"
                ? NKikimrScheme::StatusAccepted
                : NKikimrScheme::StatusInvalidParameter;

            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "Table%s"
                Columns { Name: "key" Type: "%s" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", keyType.c_str(), keyType.c_str()));
            env.TestWaitNotification(runtime, txId);

            TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "Table%s"
                StreamDescription {
                  Name: "Stream"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormatProto
                }
                TopicPartitions: 10
            )", keyType.c_str()), {status});

            if (status != NKikimrScheme::StatusAccepted) {
                continue;
            }

            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePrivatePath(runtime, Sprintf("/MyRoot/Table%s/Stream/streamImpl", keyType.c_str())), {
                NLs::PathExist,
                NLs::CheckPartCount("streamImpl", 10, 2, 5, 10),
            });
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
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
            TopicPartitions: 0
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(Attributes) {
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
              UserAttributes { Key: "key" Value: "value" }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            [=](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                const auto& table = record.GetPathDescription().GetTable();
                UNIT_ASSERT_VALUES_EQUAL(table.CdcStreamsSize(), 1);

                const auto& stream = table.GetCdcStreams(0);
                UNIT_ASSERT_VALUES_EQUAL(stream.UserAttributesSize(), 1);

                const auto& attr = stream.GetUserAttributes(0);
                UNIT_ASSERT_VALUES_EQUAL(attr.GetKey(), "key");
                UNIT_ASSERT_VALUES_EQUAL(attr.GetValue(), "value");
            }
        });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
            NLs::UserAttrsHas({
                {"key", "value"},
            })
        });
    }

    Y_UNIT_TEST(ReplicationAttribute) {
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
              UserAttributes { Key: "__async_replication" Value: "value" }
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        NJson::TJsonValue json;
        json["id"] = "some-id";
        json["path"] = "/some/path";
        const auto jsonString = NJson::WriteJson(json, false);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
              UserAttributes { Key: "__async_replication" Value: "%s" }
            }
        )", EscapeC(jsonString).c_str()));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
            NLs::UserAttrsHas({
                {"__async_replication", jsonString},
            })
        });

        // now it is forbidden to change the scheme
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "extra" Type: "Uint64" }
        )", {NKikimrScheme::StatusPreconditionFailed});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            DropColumns { Name: "value" }
        )", {NKikimrScheme::StatusPreconditionFailed});

        // drop stream
        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
        )");
        env.TestWaitNotification(runtime, txId);

        // now it is allowed to change the scheme
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "extra" Type: "Uint64" }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            DropColumns { Name: "value" }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(DocApi) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableChangefeedDynamoDBStreamsFormat(true)
            .EnableChangefeedDebeziumJsonFormat(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "RowTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "DocumentTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"__document_api_version", "1"}}));
        env.TestWaitNotification(runtime, txId);

        // non-document table
        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "RowTable"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeNewAndOldImages
              Format: ECdcStreamFormatDynamoDBStreamsJson
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        // invalid mode
        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "DocumentTable"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeUpdate
              Format: ECdcStreamFormatDynamoDBStreamsJson
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        // invalid format
        for (const char* format : TVector<const char*>{"Proto", "Json", "DebeziumJson"}) {
            TestCreateCdcStream(runtime, ++txId, "/MyRoot", Sprintf(R"(
                TableName: "DocumentTable"
                StreamDescription {
                  Name: "Stream"
                  Mode: ECdcStreamModeKeysOnly
                  Format: ECdcStreamFormat%s
                  AwsRegion: "foo"
                }
            )", format), {NKikimrScheme::StatusInvalidParameter});
        }

        // ok
        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "DocumentTable"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeNewAndOldImages
              Format: ECdcStreamFormatDynamoDBStreamsJson
              AwsRegion: "ru-central1"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DocumentTable/Stream"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeNewAndOldImages),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson),
            NLs::StreamAwsRegion("ru-central1"),
        });
    }

    Y_UNIT_TEST(DocApiNegative) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableChangefeedDynamoDBStreamsFormat(false));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "DocumentTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"__document_api_version", "1"}}));
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "DocumentTable"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeNewAndOldImages
              Format: ECdcStreamFormatDynamoDBStreamsJson
              AwsRegion: "ru-central1"
            }
        )", {NKikimrScheme::StatusPreconditionFailed});
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

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "StreamWithIndex"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
            IndexName: "NotExistedIndex"
        )", {NKikimrScheme::StatusSchemeError});

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "StreamWithIndex"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
            IndexName: "Index"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Index/indexImplTable/StreamWithIndex/streamImpl"), {NLs::PathExist});

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

        ui32 nStreams = 0;

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

            if (status == NKikimrScheme::StatusAccepted) {
                nStreams++;
            }
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

            if (status == NKikimrScheme::StatusAccepted) {
                nStreams++;
            }
        }

        limits = TSchemeLimits();
        limits.MaxPQPartitions = 3;
        SetSchemeshardSchemaLimits(runtime, limits);

        for (ui32 i = nStreams; i <= limits.MaxPQPartitions; ++i) {
            const auto status = i < limits.MaxPQPartitions
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

    void Metering(bool serverless) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableProtoSourceIdInfo(true)
            .EnablePqBilling(serverless));
        ui64 txId = 100;

        // create shared db
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "Shared"
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "Shared"
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
            StoragePools {
              Name: "pool-2"
              Kind: "pool-kind-2"
            }
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
            ExternalHive: false
        )");
        env.TestWaitNotification(runtime, txId);

        // create serverless db
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", Sprintf(R"(
            Name: "Serverless"
            ResourcesDomainKey {
                SchemeShard: %lu
                PathId: 2
            }
        )", TTestTxConfig::SchemeShard));
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "Serverless"
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
            ExternalHive: false
        )");
        env.TestWaitNotification(runtime, txId);

        TString dbName;
        if (serverless) {
            dbName = "/MyRoot/Serverless";
        } else {
            dbName = "/MyRoot/Shared";
        }

        ui64 schemeShard = 0;
        TestDescribeResult(DescribePath(runtime, dbName), {
            NLs::PathExist,
            NLs::ExtractTenantSchemeshard(&schemeShard)
        });

        UNIT_ASSERT(schemeShard != 0 && schemeShard != TTestTxConfig::SchemeShard);

        TestCreateTable(runtime, schemeShard, ++txId, dbName, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2
        )");
        env.TestWaitNotification(runtime, txId, schemeShard);

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_NOTICE);
        TVector<TString> meteringRecords;
        runtime.SetObserverFunc([&meteringRecords](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != NMetering::TEvMetering::EvWriteMeteringJson) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            meteringRecords.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestCreateCdcStream(runtime, schemeShard, ++txId, dbName, R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId, schemeShard);

        for (int i = 0; i < 10; ++i) {
            env.SimulateSleep(runtime, TDuration::Seconds(10));
        }

        for (const auto& rec : meteringRecords) {
            Cerr << "GOT METERING: " << rec << "\n";
        }

        UNIT_ASSERT_VALUES_EQUAL(meteringRecords.size(), (serverless ? 3 : 0));

        if (!meteringRecords) {
            return;
        }

        NJson::TJsonValue json;
        NJson::ReadJsonTree(meteringRecords[0], &json, true);
        auto& map = json.GetMap();
        UNIT_ASSERT(map.contains("schema"));
        UNIT_ASSERT(map.contains("resource_id"));
        UNIT_ASSERT(map.contains("tags"));
        UNIT_ASSERT(map.find("tags")->second.GetMap().contains("ydb_size"));
        UNIT_ASSERT_VALUES_EQUAL(map.find("schema")->second.GetString(), "ydb.serverless.v1");
        UNIT_ASSERT_VALUES_EQUAL(map.find("resource_id")->second.GetString(), Sprintf("%s/Table/Stream/streamImpl", dbName.c_str()));
        UNIT_ASSERT_VALUES_EQUAL(map.find("tags")->second.GetMap().find("ydb_size")->second.GetInteger(), 0);
    }

    Y_UNIT_TEST(MeteringServerless) {
        Metering(true);
    }

    Y_UNIT_TEST(MeteringDedicated) {
        Metering(false);
    }

    Y_UNIT_TEST(ChangeOwner) {
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

        for (const auto* path : {"Table", "Table/Stream", "Table/Stream/streamImpl"}) {
            TestDescribeResult(DescribePrivatePath(runtime, Sprintf("/MyRoot/%s", path)), {
                NLs::HasOwner("root@builtin"),
            });
        }

        TestModifyACL(runtime, ++txId, "/MyRoot", "Table", "", "user@builtin");
        env.TestWaitNotification(runtime, txId);

        for (const auto* path : {"Table", "Table/Stream", "Table/Stream/streamImpl"}) {
            TestDescribeResult(DescribePrivatePath(runtime, Sprintf("/MyRoot/%s", path)), {
                NLs::HasOwner("user@builtin"),
            });
        }
    }

} // TCdcStreamTests

Y_UNIT_TEST_SUITE(TCdcStreamWithInitialScanTests) {
    void InitialScan(bool enable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableProtoSourceIdInfo(true)
            .EnableChangefeedInitialScan(enable));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const auto expectedStatus = enable
            ? NKikimrScheme::StatusAccepted
            : NKikimrScheme::StatusPreconditionFailed;

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
              State: ECdcStreamStateScan
            }
        )", {expectedStatus});

        if (!enable) {
            return;
        }

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateScan),
        });
    }

    Y_UNIT_TEST(InitialScanEnabled) {
        InitialScan(true);
    }

    Y_UNIT_TEST(InitialScanDisabled) {
        InitialScan(false);
    }

    Y_UNIT_TEST(AlterStream) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableProtoSourceIdInfo(true)
            .EnableChangefeedInitialScan(true));
        runtime.GetAppData().DisableCdcAutoSwitchingToReadyStateForTests = true;
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
              State: ECdcStreamStateScan
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto lockTxId = txId;
        auto testAlterCdcStream = [&runtime](ui64 txId, const TString& parentPath, const TString& schema,
                const TMaybe<ui64>& lockTxId, TEvSchemeShard::EStatus expectedStatus = NKikimrScheme::StatusAccepted)
        {
            auto request = AlterCdcStreamRequest(txId, parentPath, schema);
            if (lockTxId) {
                request->Record.MutableTransaction(0)->MutableLockGuard()->SetOwnerTxId(*lockTxId);
            }

            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request);
            TestModificationResult(runtime, txId, expectedStatus);
        };

        // try to disable
        testAlterCdcStream(++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
            Disable {}
        )", lockTxId, NKikimrScheme::StatusPreconditionFailed);

        // without guard & lockTxId
        testAlterCdcStream(++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
            GetReady {
              LockTxId: 0
            }
        )", {}, NKikimrScheme::StatusMultipleModifications);

        // with guard, without lockTxId
        testAlterCdcStream(++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream"
            GetReady {
              LockTxId: 0
            }
        )", lockTxId, NKikimrScheme::StatusMultipleModifications);

        // without guard, with lockTxId
        testAlterCdcStream(++txId, "/MyRoot", Sprintf(R"(
            TableName: "Table"
            StreamName: "Stream"
            GetReady {
              LockTxId: %lu
            }
        )", lockTxId), {}, NKikimrScheme::StatusMultipleModifications);

        // with guard & lockTxId
        testAlterCdcStream(++txId, "/MyRoot", Sprintf(R"(
            TableName: "Table"
            StreamName: "Stream"
            GetReady {
              LockTxId: %lu
            }
        )", lockTxId), lockTxId);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Stream"), {
            NLs::PathExist,
            NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeKeysOnly),
            NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
        });

        // another try should fail
        testAlterCdcStream(++txId, "/MyRoot", Sprintf(R"(
            TableName: "Table"
            StreamName: "Stream"
            GetReady {
              LockTxId: %lu
            }
        )", lockTxId), {}, NKikimrScheme::StatusPreconditionFailed);
    }

    Y_UNIT_TEST(DropStream) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableProtoSourceIdInfo(true)
            .EnableChangefeedInitialScan(true));
        runtime.GetAppData().DisableCdcAutoSwitchingToReadyStateForTests = true;
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
              Name: "Stream1"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
              State: ECdcStreamStateScan
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // the table is locked now
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "extra" Type: "Uint64" }
        )", {NKikimrScheme::StatusMultipleModifications});

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream2"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusMultipleModifications});

        // drop the stream that locks the table
        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamName: "Stream1"
        )");
        env.TestWaitNotification(runtime, txId);

        // the table is no longer locked
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "extra" Type: "Uint64" }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream2"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    void Metering(bool serverless) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .EnableProtoSourceIdInfo(true)
            .EnableChangefeedInitialScan(true));
        ui64 txId = 100;

        // create shared db
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "Shared"
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "Shared"
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
            StoragePools {
              Name: "pool-2"
              Kind: "pool-kind-2"
            }
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
            ExternalHive: false
        )");
        env.TestWaitNotification(runtime, txId);

        const auto attrs = AlterUserAttrs({
            {"cloud_id", "CLOUD_ID_VAL"},
            {"folder_id", "FOLDER_ID_VAL"},
            {"database_id", "DATABASE_ID_VAL"}
        });

        // create serverless db
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", Sprintf(R"(
            Name: "Serverless"
            ResourcesDomainKey {
                SchemeShard: %lu
                PathId: 2
            }
        )", TTestTxConfig::SchemeShard), attrs);
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "Serverless"
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
            ExternalHive: false
        )");
        env.TestWaitNotification(runtime, txId);

        TString dbName;
        if (serverless) {
            dbName = "/MyRoot/Serverless";
        } else {
            dbName = "/MyRoot/Shared";
        }

        ui64 schemeShard = 0;
        TestDescribeResult(DescribePath(runtime, dbName), {
            NLs::PathExist,
            NLs::ExtractTenantSchemeshard(&schemeShard)
        });

        UNIT_ASSERT(schemeShard != 0 && schemeShard != TTestTxConfig::SchemeShard);

        TestCreateTable(runtime, schemeShard, ++txId, dbName, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, schemeShard);

        bool catchMeteringRecord = false;
        TString meteringRecord;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvCdcStreamScanResponse:
                if (const auto* msg = ev->Get<TEvDataShard::TEvCdcStreamScanResponse>()) {
                    if (msg->Record.GetStatus() == NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE) {
                        catchMeteringRecord = true;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            case NMetering::TEvMetering::EvWriteMeteringJson:
                if (catchMeteringRecord) {
                    meteringRecord = ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        TestCreateCdcStream(runtime, schemeShard, ++txId, dbName, R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
              State: ECdcStreamStateScan
            }
        )");
        env.TestWaitNotification(runtime, txId, schemeShard);

        if (serverless) {
            if (meteringRecord.empty()) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&meteringRecord](IEventHandle&) {
                    return !meteringRecord.empty();
                });
                runtime.DispatchEvents(opts);
            }

            UNIT_ASSERT_STRINGS_EQUAL(meteringRecord, TBillRecord()
                .Id("cdc_stream_scan-72075186233409549-3-72075186233409549-4")
                .CloudId("CLOUD_ID_VAL")
                .FolderId("FOLDER_ID_VAL")
                .ResourceId("DATABASE_ID_VAL")
                .SourceWt(TInstant::FromValue(0))
                .Usage(TBillRecord::RequestUnits(1, TInstant::FromValue(0)))
                .ToString());
        } else {
            for (int i = 0; i < 10; ++i) {
                env.SimulateSleep(runtime, TDuration::Seconds(1));
            }

            UNIT_ASSERT(meteringRecord.empty());
        }
    }

    Y_UNIT_TEST(MeteringServerless) {
        Metering(true);
    }

    Y_UNIT_TEST(MeteringDedicated) {
        Metering(false);
    }

} // TCdcStreamWithInitialScanTests
