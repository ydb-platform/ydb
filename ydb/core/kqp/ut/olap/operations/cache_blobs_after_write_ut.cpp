#include <ydb/core/kqp/ut/common/columnshard.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NYdb;

namespace {

TKikimrSettings MakeSettings() {
    return TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
}

TTestHelper::TColumnTable MakeTable() {
    TVector<TTestHelper::TColumnSchema> schema = {
        TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
        TTestHelper::TColumnSchema().SetName("value").SetType(NScheme::NTypeIds::Utf8),
    };
    TTestHelper::TColumnTable table;
    table.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema).SetMinPartitionsCount(1);
    return table;
}

NKikimrSchemeOp::TColumnTableSchemeOptions DescribeOptions(TKikimrRunner& kikimr, const TString& path) {
    auto desc = kikimr.GetTestClient().Ls(path);
    UNIT_ASSERT(desc);
    UNIT_ASSERT(desc->Record.GetPathDescription().HasColumnTableDescription());
    UNIT_ASSERT(desc->Record.GetPathDescription().GetColumnTableDescription().HasSchema());
    return desc->Record.GetPathDescription().GetColumnTableDescription().GetSchema().GetOptions();
}

NYdb::TStatus ExecScheme(TTestHelper& testHelper, const TString& query) {
    return testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
}

void AlterCacheBlobsAfterWrite(TTestHelper& testHelper, const TString& tableName, const bool enabled) {
    const auto query = TStringBuilder() << "ALTER OBJECT `" << tableName
                                        << "` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `CACHE_BLOBS_AFTER_WRITE`=`"
                                        << (enabled ? "true" : "false") << "`)";
    auto result = ExecScheme(testHelper, query);
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

}   // namespace

Y_UNIT_TEST_SUITE(KqpOlapCacheBlobsAfterWrite) {
    Y_UNIT_TEST(DefaultIsOffAfterCreate) {
        TTestHelper testHelper(MakeSettings());
        auto table = MakeTable();
        testHelper.CreateTable(table);

        const auto options = DescribeOptions(testHelper.GetKikimr(), table.GetName());
        UNIT_ASSERT(!options.HasCacheBlobsAfterWrite() || !options.GetCacheBlobsAfterWrite());
    }

    Y_UNIT_TEST(AlterEnablesAndDisablesOption) {
        TTestHelper testHelper(MakeSettings());
        auto table = MakeTable();
        testHelper.CreateTable(table);

        AlterCacheBlobsAfterWrite(testHelper, table.GetName(), true);
        {
            const auto options = DescribeOptions(testHelper.GetKikimr(), table.GetName());
            UNIT_ASSERT(options.HasCacheBlobsAfterWrite());
            UNIT_ASSERT(options.GetCacheBlobsAfterWrite());
        }

        AlterCacheBlobsAfterWrite(testHelper, table.GetName(), false);
        {
            const auto options = DescribeOptions(testHelper.GetKikimr(), table.GetName());
            UNIT_ASSERT(options.HasCacheBlobsAfterWrite());
            UNIT_ASSERT(!options.GetCacheBlobsAfterWrite());
        }
    }

    Y_UNIT_TEST(AlterRejectsInvalidValue) {
        TTestHelper testHelper(MakeSettings());
        auto table = MakeTable();
        testHelper.CreateTable(table);

        const auto query = TStringBuilder() << "ALTER OBJECT `" << table.GetName()
                                            << "` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `CACHE_BLOBS_AFTER_WRITE`=`notabool`)";
        auto result = ExecScheme(testHelper, query);
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().ToString().contains("CACHE_BLOBS_AFTER_WRITE"), result.GetIssues().ToString());
    }
}

}   // namespace NKikimr::NKqp
