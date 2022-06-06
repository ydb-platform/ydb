#include "ut_backup_restore_common.h"

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <util/string/cast.h>
#include <util/string/printf.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr::NWrappers::NTestHelpers;

Y_UNIT_TEST_SUITE(TBackupTests) {
    using TFillFn = std::function<void(TTestBasicRuntime&)>;

    void Backup(TTestBasicRuntime& runtime, const TString& compressionCodec,
            const TString& creationScheme, TFillFn fill, ui32 rowsBatchSize = 128)
    {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", creationScheme);
        env.TestWaitNotification(runtime, txId);

        fill(runtime);

        const auto tableDesc = DescribePath(runtime, "/MyRoot/Table", true, true);
        TString tableSchema;
        UNIT_ASSERT(google::protobuf::TextFormat::PrintToString(tableDesc.GetPathDescription(), &tableSchema));

        TestBackup(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableName: "Table"
            Table {
                %s
            }
            S3Settings {
                Endpoint: "localhost:%d"
                Scheme: HTTP
            }
            ScanSettings {
                RowsBatchSize: %d
            }
            Compression {
                Codec: "%s"
            }
        )", tableSchema.c_str(), port, rowsBatchSize, compressionCodec.c_str()));
        env.TestWaitNotification(runtime, txId);
    }

    void WriteRow(TTestBasicRuntime& runtime, ui64 tabletId, const TString& key, const TString& value) {
        NKikimrMiniKQL::TResult result;
        TString error;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"(
            (
                (let key '( '('key (Utf8 '%s) ) ) )
                (let row '( '('value (Utf8 '%s) ) ) )
                (return (AsList (UpdateRow '__user__Table key row) ))
            )
        )", key.c_str(), value.c_str()), result, error);

        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnSingleShardTable) {
        TTestBasicRuntime runtime;

        Backup(runtime, ToString(Codec), R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", [](TTestBasicRuntime& runtime) {
            WriteRow(runtime, TTestTxConfig::FakeHiveTablets, "a", "valueA");
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnMultiShardTable) {
        TTestBasicRuntime runtime;

        Backup(runtime, ToString(Codec), R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Text: "b" } }
              }
            }
        )", [](TTestBasicRuntime& runtime) {
            WriteRow(runtime, TTestTxConfig::FakeHiveTablets + 0, "a", "valueA");
            WriteRow(runtime, TTestTxConfig::FakeHiveTablets + 1, "b", "valueb");
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnLargeData) {
        TTestBasicRuntime runtime;
        const ui32 batchSize = 10;

        Backup(runtime, ToString(Codec), R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", [](TTestBasicRuntime& runtime) {
            for (ui32 i = 0; i < 2 * batchSize; ++i) {
                WriteRow(runtime, TTestTxConfig::FakeHiveTablets, Sprintf("a%d", i), "valueA");
            }
        }, batchSize);
    }

} // TBackupTests
