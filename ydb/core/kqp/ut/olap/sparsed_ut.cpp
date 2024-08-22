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

    class TSparsedDataTest {
    private:
        const TKikimrSettings Settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner Kikimr;
        NKikimr::NYDBTest::TControllers::TGuard<NKikimr::NYDBTest::NColumnShard::TController> CSController;
    public:
        TSparsedDataTest()
            : Kikimr(Settings)
            , CSController(NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>())
        {

        }

        ui32 GetCount() const {
            auto selectQuery = TString(R"(
                SELECT
                    count(*) as count,
                FROM `/Root/olapStore/olapTable`
            )");

            auto tableClient = Kikimr.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            return GetUint64(rows[0].at("count"));
        }

        ui32 GetDefaultsCount() const {
            auto selectQuery = TString(R"(
                SELECT
                    count(*) as count,
                FROM `/Root/olapStore/olapTable`
                WHERE field == 'abcde'
            )");

            auto tableClient = Kikimr.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            return GetUint64(rows[0].at("count"));
        }

        void FillCircle(const double shiftKff, const ui32 countExpectation) {
            TTypedLocalHelper helper("Utf8", Kikimr);
            const double frq = 0.9;
            {
                NArrow::NConstruction::TStringPoolFiller sPool(1000, 52, "abcde", frq);
                helper.FillTable(sPool, shiftKff, 10000);
            }
            ui32 defCountStart;
            {
                const ui32 defCount = GetDefaultsCount();
                defCountStart = defCount;
                const ui32 count = GetCount();
                AFL_VERIFY(count == countExpectation)("expect", countExpectation)("count", count);
                AFL_VERIFY(1.0 * defCount / count < 0.95)("def", defCount)("count", count);
                AFL_VERIFY(1.0 * defCount / count > 0.85)("def", defCount)("count", count);
            }
            CSController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            CSController->WaitIndexation(TDuration::Seconds(5));
            {
                const ui32 defCount = GetDefaultsCount();
                AFL_VERIFY(defCountStart == defCount);
                const ui32 count = GetCount();
                AFL_VERIFY(count == countExpectation)("expect", countExpectation)("count", count);
                AFL_VERIFY(1.0 * defCount / count < 0.95)("def", defCount)("count", count);
                AFL_VERIFY(1.0 * defCount / count > 0.85)("def", defCount)("count", count);
            }
            CSController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            CSController->WaitCompactions(TDuration::Seconds(5));
            {
                const ui32 defCount = GetDefaultsCount();
                AFL_VERIFY(defCountStart == defCount);
                const ui32 count = GetCount();
                AFL_VERIFY(count == countExpectation)("expect", countExpectation)("count", count);
                AFL_VERIFY(1.0 * defCount / count < 0.95)("def", defCount)("count", count);
                AFL_VERIFY(1.0 * defCount / count > 0.85)("def", defCount)("count", count);
            }

            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        }

        void Execute() {
            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            CSController->SetOverridePeriodicWakeupActivationPeriod(TDuration::MilliSeconds(100));

            Tests::NCommon::TLoggerInit(Kikimr).Initialize();
            TTypedLocalHelper helper("Utf8", Kikimr);
            helper.CreateTestOlapTable();

            FillCircle(0, 10000);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`, `DEFAULT_VALUE`=`abcde`);");
            FillCircle(0.1, 11000);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            FillCircle(0.2, 12000);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`);");
            FillCircle(0.3, 13000);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            FillCircle(0.4, 14000);
        }
    };

    Y_UNIT_TEST(Switching) {
        TSparsedDataTest test;
        test.Execute();
    }
}

} // namespace
