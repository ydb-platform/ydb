#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <ut/olap/helpers/typed_local.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

std::pair<ui64, ui64> GetVolumes(
    const TKikimrRunner& runner, const TString& tablePath, const std::vector<TString> columnNames) {
    TString selectQuery = "SELECT * FROM `" + tablePath + "/.sys/primary_index_stats` WHERE Activity == 1";
    if (columnNames.size()) {
        selectQuery += " AND EntityName IN ('" + JoinSeq("','", columnNames) + "')";
    }
    auto tableClient = runner.GetTableClient();
    std::optional<ui64> rawBytesPred;
    std::optional<ui64> bytesPred;
    while (true) {
        auto rows = ExecuteScanQuery(tableClient, selectQuery, false);
        ui64 rawBytes = 0;
        ui64 bytes = 0;
        for (auto&& r : rows) {
            for (auto&& c : r) {
                if (c.first == "RawBytes") {
                    rawBytes += GetUint64(c.second);
                }
                if (c.first == "BlobRangeSize") {
                    bytes += GetUint64(c.second);
                }
            }
        }
        if (rawBytesPred && *rawBytesPred == rawBytes && bytesPred && *bytesPred == bytes) {
            break;
        } else {
            rawBytesPred = rawBytes;
            bytesPred = bytes;
            Cerr << "Wait changes: " << bytes << "/" << rawBytes << Endl;
            Sleep(TDuration::Seconds(5));
        }
    }
    return { rawBytesPred.value(), bytesPred.value() };
}

struct TColumnStorageStats {
    ui64 RawBytes = 0;
    ui64 BlobRangeSize = 0;
    ui64 Rows = 0;
};

TString BuildInsertedColumnStatsQuery(const TString& tablePath, const TString& columnName) {
    return "SELECT RawBytes, BlobRangeSize, Rows FROM `" + tablePath + "/.sys/primary_index_stats`"
           " WHERE Activity == 1"
           " AND Kind == \"INSERTED\""
           " AND EntityType == \"COL\""
           " AND EntityName == '" + columnName + "'";
}

TColumnStorageStats GetStableColumnStats(
    const TKikimrRunner& runner, const TString& tablePath, const TString& columnName) {
    const TString selectQuery = BuildInsertedColumnStatsQuery(tablePath, columnName);
    auto tableClient = runner.GetTableClient();
    std::optional<TColumnStorageStats> statsPred;
    constexpr ui64 maxAttempts = 60;
    for (ui64 attempt = 0; attempt < maxAttempts; ++attempt) {
        TColumnStorageStats stats;
        for (auto&& row : ExecuteScanQuery(tableClient, selectQuery, false)) {
            for (auto&& cell : row) {
                if (cell.first == "RawBytes") {
                    stats.RawBytes += GetUint64(cell.second);
                } else if (cell.first == "BlobRangeSize") {
                    stats.BlobRangeSize += GetUint64(cell.second);
                } else if (cell.first == "Rows") {
                    stats.Rows += GetUint64(cell.second);
                }
            }
        }
        if (statsPred && statsPred->RawBytes == stats.RawBytes && statsPred->BlobRangeSize == stats.BlobRangeSize &&
            statsPred->Rows == stats.Rows) {
            return stats;
        }
        statsPred = stats;
        Sleep(TDuration::Seconds(1));
    }
    UNIT_FAIL(TStringBuilder() << "column stats did not stabilize within timeout for column " << columnName);
    return {};
}

ui64 CountActiveNonInsertedColumnStats(
    const TKikimrRunner& runner, const TString& tablePath, const std::vector<TString>& columnNames) {
    const TString selectQuery =
        "SELECT COUNT(*) AS C FROM `" + tablePath + "/.sys/primary_index_stats`"
        " WHERE Activity == 1"
        " AND EntityType == \"COL\""
        " AND Kind != \"INSERTED\""
        " AND EntityName IN ('" + JoinSeq("','", columnNames) + "')";
    auto tableClient = runner.GetTableClient();
    auto rows = ExecuteScanQuery(tableClient, selectQuery, false);
    UNIT_ASSERT_C(rows.size() == 1, "unexpected stats rows count for non-inserted check");
    return GetUint64(rows.front().at("C"));
}

std::shared_ptr<arrow::RecordBatch> BuildBatchWithSameLongString(
    const ui64 rowsCount, const ui64 stringLen, const TString& pkName, const TString& lz4Name, const TString& zstdName) {
    TString value;
    value.reserve(stringLen);
    for (ui64 i = 0; i < stringLen; ++i) {
        value.push_back(static_cast<char>('a' + (i % 26)));
    }

    arrow::UInt64Builder pkBuilder;
    arrow::StringBuilder lz4Builder;
    arrow::StringBuilder zstdBuilder;
    for (ui64 i = 0; i < rowsCount; ++i) {
        UNIT_ASSERT_C(pkBuilder.Append(i).ok(), "cannot append pk");
        UNIT_ASSERT_C(lz4Builder.Append(value.data(), value.size()).ok(), "cannot append lz4 column value");
        UNIT_ASSERT_C(zstdBuilder.Append(value.data(), value.size()).ok(), "cannot append zstd column value");
    }

    std::shared_ptr<arrow::Array> pkArray;
    std::shared_ptr<arrow::Array> lz4Array;
    std::shared_ptr<arrow::Array> zstdArray;
    UNIT_ASSERT_C(pkBuilder.Finish(&pkArray).ok(), "cannot finish pk builder");
    UNIT_ASSERT_C(lz4Builder.Finish(&lz4Array).ok(), "cannot finish lz4 builder");
    UNIT_ASSERT_C(zstdBuilder.Finish(&zstdArray).ok(), "cannot finish zstd builder");

    auto schema = arrow::schema({
        arrow::field(pkName, arrow::uint64(), false),
        arrow::field(lz4Name, arrow::utf8(), true),
        arrow::field(zstdName, arrow::utf8(), true),
    });
    return arrow::RecordBatch::Make(schema, rowsCount, { pkArray, lz4Array, zstdArray });
}

Y_UNIT_TEST_SUITE(KqpOlapCompression) {
    Y_UNIT_TEST(CreateWithCompression) {
        auto settings = TKikimrSettings()
            .SetEnableOlapCompression(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
                .SetCompressionSetting("algorithm", "zstd").SetCompressionSetting("level", "3")
        };

        TTestHelper::TColumnTable standaloneTable;
        standaloneTable.SetName("/Root/CompKeyTable").SetPrimaryKey({ "key" }).SetSchema(schema);
        testHelper.CreateTableQuery(standaloneTable);
    }

    Y_UNIT_TEST(ChangeCompression) {
        auto settings = TKikimrSettings()
            .SetEnableOlapCompression(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("value").SetType(NScheme::NTypeIds::Uint64)
        };

        TTestHelper::TColumnTable standaloneTable;
        standaloneTable.SetName("/Root/CompValueTable").SetPrimaryKey({ "key" }).SetSchema(schema);
        testHelper.CreateTable(standaloneTable);
        testHelper.ExecuteQuery("ALTER TABLE `/Root/CompValueTable` ALTER COLUMN `value` SET COMPRESSION(algorithm=lz4);");
    }

    std::pair<ui64, ui64> GetVolumesColumnWithCompression(const std::optional<NKikimrConfig::TColumnShardConfig>& CSConfig = {}) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        if (CSConfig.has_value()) {
            *settings.AppConfig.MutableColumnShardConfig() = CSConfig.value();
        }
        TTestHelper testHelper(settings);
        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("pk_int").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
        };

        TString tableName = "/Root/ColumnTableTest";
        TTestHelper::TColumnTable testTable;
        testTable.SetName(tableName).SetPrimaryKey({ "pk_int" }).SetSharding({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        TVector<NArrow::NConstruction::IArrayBuilder::TPtr> dataBuilders;
        dataBuilders.push_back(
            NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable(
                "pk_int", false));
        auto batch = NArrow::NConstruction::TRecordBatchConstructor(dataBuilders).BuildBatch(100000);
        testHelper.BulkUpsert(testTable, batch);
        csController->WaitCompactions(TDuration::Seconds(10));
        return GetVolumes(testHelper.GetKikimr(), tableName, { "pk_int" });
    }

    Y_UNIT_TEST(DefaultCompressionViaCSConfig) {
        auto [rawBytesPK1, bytesPK1] = GetVolumesColumnWithCompression();  // Default compression LZ4
        NKikimrConfig::TColumnShardConfig csConfig = NKikimrConfig::TColumnShardConfig();
        csConfig.SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD);
        csConfig.SetDefaultCompressionLevel(1);
        auto [rawBytesPK2, bytesPK2] = GetVolumesColumnWithCompression(csConfig);
        AFL_VERIFY(rawBytesPK2 == rawBytesPK1)("pk1", rawBytesPK1)("pk2", rawBytesPK2);
        AFL_VERIFY(bytesPK2 < bytesPK1 / 3)("pk1", bytesPK1)("pk2", bytesPK2);
    }

    Y_UNIT_TEST(DifferentColumnCodecsOnInsert) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        auto settings = TKikimrSettings()
            .SetEnableOlapCompression(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();

        static const TString tableName = "/Root/ColumnCompressionCodecs";
        static const TString lz4Column = "col_lz4";
        static const TString zstdColumn = "col_zstd";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("pk").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName(lz4Column).SetType(NScheme::NTypeIds::Utf8)
                .SetCompressionSetting("algorithm", "lz4"),
            TTestHelper::TColumnSchema().SetName(zstdColumn).SetType(NScheme::NTypeIds::Utf8)
                .SetCompressionSetting("algorithm", "zstd").SetCompressionSetting("level", "7"),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName(tableName).SetPrimaryKey({ "pk" }).SetSharding({ "pk" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        const ui64 rowsCount = 256;
        const ui64 stringLen = 32 * 1024;
        testHelper.BulkUpsert(testTable, BuildBatchWithSameLongString(rowsCount, stringLen, "pk", lz4Column, zstdColumn));

        UNIT_ASSERT_VALUES_EQUAL(csController->GetCompactionStartedCounter().Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(csController->GetCompactionFinishedCounter().Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(CountActiveNonInsertedColumnStats(testHelper.GetKikimr(), tableName, { lz4Column, zstdColumn }), 0);

        const auto lz4Stats = GetStableColumnStats(testHelper.GetKikimr(), tableName, lz4Column);
        const auto zstdStats = GetStableColumnStats(testHelper.GetKikimr(), tableName, zstdColumn);

        Cerr << "lz4Stats: " << lz4Stats.RawBytes << " " << lz4Stats.BlobRangeSize << Endl;
        Cerr << "zstdStats: " << zstdStats.RawBytes << " " << zstdStats.BlobRangeSize << Endl;

        UNIT_ASSERT_VALUES_EQUAL(lz4Stats.Rows, rowsCount);
        UNIT_ASSERT_VALUES_EQUAL(zstdStats.Rows, rowsCount);

        UNIT_ASSERT_C(lz4Stats.RawBytes > 0, "lz4 column raw bytes must be positive");
        UNIT_ASSERT_C(zstdStats.RawBytes > 0, "zstd column raw bytes must be positive");
        UNIT_ASSERT_VALUES_EQUAL(lz4Stats.RawBytes, zstdStats.RawBytes);

        UNIT_ASSERT_C(lz4Stats.BlobRangeSize > 0, "lz4 column blob size must be positive");
        UNIT_ASSERT_C(zstdStats.BlobRangeSize > 0, "zstd column blob size must be positive");
        UNIT_ASSERT_C(
            lz4Stats.BlobRangeSize != zstdStats.BlobRangeSize,
            "expected different packed blob sizes for lz4 and zstd codecs, lz4=" << lz4Stats.BlobRangeSize
                << " zstd=" << zstdStats.BlobRangeSize << " raw=" << lz4Stats.RawBytes);
        UNIT_ASSERT_C(
            zstdStats.BlobRangeSize < lz4Stats.BlobRangeSize,
            "expected zstd level 7 to pack repetitive string tighter than lz4, lz4=" << lz4Stats.BlobRangeSize
                << " zstd=" << zstdStats.BlobRangeSize);
    }
}
}
