#include <ydb/core/kqp/ut/rbo/effects/kqp_effects_helpers.h>

#include <ydb/core/kqp/ut/olap/helpers/local.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

// Ported from ut/olap/kqp_olap_ut.cpp.
Y_UNIT_TEST_SUITE(KqpRboOlapEffects) {

    Y_UNIT_TEST_TWIN(MultiInsertWithSinks, NewRbo) {
        auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:00Z'), 'a'u, '0'u);
            INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:01Z'), 'b'u, 't'u);
            INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:02Z'), 'c'u, 'test'u);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        {
            auto it = db.StreamExecuteQuery(R"(
                --!syntax_v1

                SELECT
                    level, message, new_column1, resource_id, timestamp, uid
                FROM `/Root/olapStore/olapTable` ORDER BY uid
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            CompareYson(result, R"([[#;#;#;["0"];0u;"a"];[#;#;#;["t"];1000000u;"b"];[#;#;#;["test"];2000000u;"c"]])");
        }
    }

} // suite

} // namespace NKikimr::NKqp
