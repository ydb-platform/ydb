#include "helpers/local.h"
#include "helpers/writer.h"
#include "helpers/typed_local.h"
#include "helpers/query_executor.h"
#include "helpers/get_value.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/wrappers/fake_storage.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapSparsed) {

    Y_UNIT_TEST(DefaultValues) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::MilliSeconds(100));
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        const auto addData = [&](const double shiftKff, const ui32 expectation, const ui32 expectationCount) {
            const double frq = 0.9;
            {
                NArrow::NConstruction::TStringPoolFiller sPool(1000, 52, "abcde", frq);
                helper.FillTable(sPool, shiftKff, 1000);
            }
            csController->WaitIndexation(TDuration::Seconds(5));
            csController->WaitCompactions(TDuration::Seconds(5));
            ui32 defCount = 0;
            {
                auto selectQuery = TString(R"(
                SELECT
                    count(*) as count,
                FROM `/Root/olapStore/olapTable`
                WHERE field = 'abcde'
            )");

                auto tableClient = kikimr.GetTableClient();
                auto rows = ExecuteScanQuery(tableClient, selectQuery);
                defCount = GetUint64(rows[0].at("count"));
            }
//            AFL_VERIFY(GetUint64(rows[0].at("count")) < expectation + delta)("count", GetUint64(rows[0].at("count")))("expect", expectation);
//            AFL_VERIFY(expectation - delta <= GetUint64(rows[0].at("count")))("count", GetUint64(rows[0].at("count")))("expect", expectation);
            {
                auto selectQuery = TString(R"(
                SELECT
                    count(*) as count,
                FROM `/Root/olapStore/olapTable`
            )");

                auto tableClient = kikimr.GetTableClient();
                auto rows = ExecuteScanQuery(tableClient, selectQuery);
                AFL_VERIFY(expectationCount == GetUint64(rows[0].at("count")));
            }
            AFL_VERIFY(std::abs(1.0 * defCount / expectationCount - frq) < 0.5 * frq);
            AFL_VERIFY(1.0 * defCount / expectationCount > 0.3 * frq);
        };
        addData(0, 900, 1000);

        helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`, `DEFAULT_VALUE`=`abcde`);");
        addData(0.1, 900, 1100);

        helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
        addData(0.2, 900, 1200);

        helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`);");
        addData(0.3, 900, 1300);

        helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
        addData(0.4, 900, 1400);
    }

}

} // namespace
