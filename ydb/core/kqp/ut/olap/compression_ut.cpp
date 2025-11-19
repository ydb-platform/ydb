#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <ut/olap/helpers/typed_local.h>

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

Y_UNIT_TEST_SUITE(KqpOlapCompression) {
    Y_UNIT_TEST(DisabledAlterCompression) {
        auto settings = TKikimrSettings()
            .SetEnableOlapCompression(false)
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("pk_int").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
        };
        TTestHelper::TCompression compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD);

        TTestHelper::TColumnTable standaloneTable;
        standaloneTable.SetName("/Root/StandaloneTable").SetPrimaryKey({ "pk_int" }).SetSharding({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(standaloneTable);
        testHelper.SetCompression(standaloneTable, "pk_int", compression, NYdb::EStatus::SCHEME_ERROR);

        TTestHelper::TColumnTableStore testTableStore;
        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(testTableStore);
        testHelper.SetCompression(testTableStore, "pk_int", compression, NYdb::EStatus::PRECONDITION_FAILED);

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/TableStoreTest/ColumnTableTest").SetPrimaryKey({ "pk_int" }).SetSharding({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(testTable);
        testHelper.SetCompression(testTable, "pk_int", compression, NYdb::EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(OffCompression) {
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("pk_int").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
        };
        TTestHelper::TCompression compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);

        TTestHelper::TColumnTable standaloneTable;
        standaloneTable.SetName("/Root/StandaloneTable").SetPrimaryKey({ "pk_int" }).SetSharding({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(standaloneTable);
        testHelper.SetCompression(standaloneTable, "pk_int", compression);

        TTestHelper::TColumnTableStore testTableStore;
        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(testTableStore);
        testHelper.SetCompression(testTableStore, "pk_int", compression);
    }

    Y_UNIT_TEST(TestAlterCompressionTableInTableStore) {
        TKikimrSettings settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("pk_int").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
        };
        TTestHelper::TCompression compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD);

        TTestHelper::TColumnTableStore testTableStore;
        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(testTableStore);

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/TableStoreTest/ColumnTableTest").SetPrimaryKey({ "pk_int" }).SetSharding({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(testTable);
        testHelper.SetCompression(testTable, "pk_int", compression, NYdb::EStatus::SCHEME_ERROR);
    }

    std::pair<ui64, ui64> GetVolumesColumnWithCompression(const std::optional<NKikimrConfig::TColumnShardConfig>& CSConfig = {}) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        if (CSConfig.has_value()) {
            *settings.AppConfig.MutableColumnShardConfig() = CSConfig.value();
        }
        TTestHelper testHelper(settings);
        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);

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
}
}
