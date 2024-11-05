#include <ydb/core/kqp/ut/common/columnshard.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapCompression) {
    Y_UNIT_TEST(DisabledAlterCompression) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false).SetEnableOlapCompression(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("pk_int").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
        };
        TTestHelper::TCompression compression = TTestHelper::TCompression().SetType(arrow::Compression::type::ZSTD);

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
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("pk_int").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
        };
        TTestHelper::TCompression compression = TTestHelper::TCompression().SetType(arrow::Compression::type::UNCOMPRESSED);

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
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        TTestHelper testHelper(settings);
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("pk_int").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
        };
        TTestHelper::TCompression compression = TTestHelper::TCompression().SetType(arrow::Compression::type::ZSTD);

        TTestHelper::TColumnTableStore testTableStore;
        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(testTableStore);

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/TableStoreTest/ColumnTableTest").SetPrimaryKey({ "pk_int" }).SetSharding({ "pk_int" }).SetSchema(schema);
        testHelper.CreateTable(testTable);
        testHelper.SetCompression(testTable, "pk_int", compression, NYdb::EStatus::SCHEME_ERROR);
    }
}
}
