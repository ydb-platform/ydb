#include <ydb/core/kqp/ut/olap/helpers/typed_local.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <ydb/core/kqp/ut/olap/helpers/ttl_index_enum.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapStatistics) {
    Y_UNIT_TEST(StatsUsage) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        {
            auto settings = TKikimrSettings().SetWithSampleTables(false);
            TKikimrRunner kikimr(settings);
            Tests::NCommon::TLoggerInit(kikimr).Initialize();
            TTypedLocalHelper helper("Utf8", kikimr);
            helper.CreateTestOlapTable();
            auto tableClient = kikimr.GetTableClient();
            {
                auto alterQuery =
                    TStringBuilder()
                    << R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=max_pk_int, TYPE=MAX, FEATURES=`{\"column_name\": \"pk_int\"}`))";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, "
                                                      "NAME=max_pk_int, TYPE=MAX, FEATURES=`{\"column_name\": \"field\"}`);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, "
                                                      "NAME=max_pk_int, TYPE=MAX, FEATURES=`{\"column_name\": \"pk_int\"}`);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = TStringBuilder()
                                  << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_INDEX, NAME=max_pk_int);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
        }
    }


    Y_UNIT_TEST(StatsUsageNotPK, EIndexForTTLColumn) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        {
            auto settings = TKikimrSettings().SetWithSampleTables(false);
            TKikimrRunner kikimr(settings);
            Tests::NCommon::TLoggerInit(kikimr).Initialize();
            TTypedLocalHelper helper("Utf8", kikimr);
            helper.CreateTestOlapTable();
            auto tableClient = kikimr.GetTableClient();
            {
                auto alterQuery = TStringBuilder() << "ALTER TABLE `/Root/olapStore/olapTable` SET (TTL = Interval(\"P1D\") ON ts);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = (Arg<0>() == EIndexForTTLColumn::MaxIndex) ? 
                    "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=max_ts, TYPE=MAX, FEATURES=`{\"column_name\": \"ts\"}`);" 
                    :
                    "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=min_max_ts, TYPE=MIN_MAX, FEATURES=`{\"column_name\": \"ts\"}`);" 
                    ;
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = TStringBuilder() << "ALTER TABLE `/Root/olapStore/olapTable` SET (TTL = Interval(\"P1D\") ON ts);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = (Arg<0>() == EIndexForTTLColumn::MaxIndex) ? 
                    "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_INDEX, NAME=max_ts);"
                    :
                    "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_INDEX, NAME=min_max_ts);"
                    ;
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
        }
    }
}

}   // namespace NKikimr::NKqp
