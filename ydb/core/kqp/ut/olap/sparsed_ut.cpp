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
        const TString StoreName;
        ui32 MultiColumnRepCount = 100;
        static const ui32 FIELD_COUNT = 5;
    public:
        TSparsedDataTest(const char* storeName)
            : Kikimr(Settings)
            , CSController(NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>())
            , StoreName(storeName)
        {

        }

        ui32 GetCount() const {
            auto selectQuery = TString(R"(
                SELECT
                    count(*) as count,
                FROM `/Root/)") + (StoreName.empty() ? "" : StoreName + "/") + "olapTable`";

            auto tableClient = Kikimr.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            return GetUint64(rows[0].at("count"));
        }

        ui32 GetDefaultsCount(const char* fieldName, const char* defValueStr) const {
            auto selectQueryTmpl = TString(R"(
                SELECT
                    count(*) as count,
                FROM `/Root/)") + (StoreName.empty() ? "" : StoreName + "/") + R"(olapTable`
                WHERE %s == %s
            )";

            auto tableClient = Kikimr.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, Sprintf(selectQueryTmpl.c_str(), fieldName, defValueStr));
            return GetUint64(rows[0].at("count"));
        }

        void GetAllDefaultsCount(ui64* counts) {
            TString query = "SELECT field_utf0 == 'abcde' AS def_utf0, field_uint0 == 0 AS def_uint0, field_int0 == 0 AS def_int0, field_float0 == 0 AS def_float0, field_double0 == 0 AS def_double0 FROM `/Root/olapStore/olapTable`";
            auto tableClient = Kikimr.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, query);

            Fill(&counts[0], &counts[FIELD_COUNT], 0);

            for (auto& row: rows) {
                auto incCounts = [&](ui32 i, const char* column) {
                    if (*NYdb::TValueParser(row.at(column)).GetOptionalBool()) {
                        counts[i]++;
                    }
                };
                incCounts(0, "def_utf0");
                incCounts(1, "def_uint0");
                incCounts(2, "def_int0");
                incCounts(3, "def_float0");
                incCounts(4, "def_double0");
             }
        }

        void CheckAllFieldsTable(bool firstCall, ui32 countExpectation, ui32* defCountStart) {
            ui64 defCounts[FIELD_COUNT];
            const ui32 count = GetCount();
            GetAllDefaultsCount(defCounts);
            for (ui32 i = 0; i < FIELD_COUNT; i++) {
                if (firstCall) {
                    defCountStart[i] = defCounts[i];
                } else {
                    AFL_VERIFY(defCountStart[i] == defCounts[i]);
                }
                AFL_VERIFY(count == countExpectation)("expect", countExpectation)("count", count);
                AFL_VERIFY(1.0 * defCounts[i] / count < 0.95)("def", defCounts[i])("count", count);
                AFL_VERIFY(1.0 * defCounts[i] / count > 0.85)("def", defCounts[i])("count", count);
            }
        }

        void CheckTable(const char* fieldName, const char* defValueStr, bool firstCall, ui32 countExpectation, ui32& defCountStart) {
            const ui32 defCount = GetDefaultsCount(fieldName, defValueStr);
            if (firstCall) {
                defCountStart = defCount;
            } else {
                AFL_VERIFY(defCountStart == defCount);
            }
            const ui32 count = GetCount();
            AFL_VERIFY(count == countExpectation)("expect", countExpectation)("count", count);
            AFL_VERIFY(1.0 * defCount / count < 0.95)("def", defCount)("count", count);
            AFL_VERIFY(1.0 * defCount / count > 0.85)("def", defCount)("count", count);
        }

        template<class TFillTable, class TCheckTable>
        void FillCircleImpl(TFillTable&& fillTable, TCheckTable&& checkTable) {
            auto start = TInstant::Now().Seconds();

            auto printTime = [&](const char* prefix) {
                auto finish = TInstant::Now().Seconds();
                fprintf(stderr, "Timing: %s took %lu seconds\n", prefix, finish - start);
                start = finish;
            };

            fillTable();
            printTime("fillTable");
            checkTable(true);
            printTime("checkTable");

            CSController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            CSController->WaitIndexation(TDuration::Seconds(5));
            printTime("wait");

            checkTable(false);
            printTime("checkTable");

            CSController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            CSController->WaitCompactions(TDuration::Seconds(5));
            printTime("wait");

            checkTable(false);
            printTime("checkTable");

            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            printTime("wait");
        }

        void FillCircle(const double shiftKff, const ui32 countExpectation) {
            ui32 defCountStart = (ui32)-1;
            FillCircleImpl([&]() {
                TTypedLocalHelper helper("Utf8", Kikimr, "olapTable", StoreName);
                const double frq = 0.9;
                NArrow::NConstruction::TStringPoolFiller sPool(1000, 52, "abcde", frq);
                helper.FillTable(sPool, shiftKff, 10000);
            },
            [&](bool firstCall) {
                CheckTable("field", "'abcde'", firstCall, countExpectation, defCountStart);
            });
        }

        void FillMultiColumnCircle(const double shiftKff, const ui32 countExpectation) {
            ui32 defCountStart[FIELD_COUNT];
            FillCircleImpl([&]() {
                TTypedLocalHelper helper("Utf8", Kikimr);
                helper.FillMultiColumnTable(MultiColumnRepCount, shiftKff, 10000);
            },
            [&](bool firstCall) {
                CheckAllFieldsTable(firstCall, countExpectation, defCountStart);
            });
        }

        void Execute() {
            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            CSController->SetOverridePeriodicWakeupActivationPeriod(TDuration::MilliSeconds(100));

            Tests::NCommon::TLoggerInit(Kikimr).Initialize();
            TTypedLocalHelper helper("Utf8", Kikimr, "olapTable", StoreName);
            if (!StoreName.empty()) {
                helper.CreateTestOlapTable();
            } else {
                auto tableClient = Kikimr.GetTableClient();
                auto session = tableClient.CreateSession().GetValueSync().GetSession();

                auto query = TStringBuilder() << R"(
                    --!syntax_v1
                    CREATE TABLE `/Root/olapTable`
                    (
                        pk_int int64 NOT NULL,
                        field )" << "Utf8" << R"(,
                        ts TimeStamp,
                        PRIMARY KEY (pk_int)
                    )
                    PARTITION BY HASH(pk_int)
                    WITH (
                        STORE = COLUMN
                    ))";
                auto result = session.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            }

            TString type = StoreName.empty() ? "TABLE" : "TABLESTORE";
            TString name = StoreName.empty() ? "olapTable" : "olapStore";

            FillCircle(0, 10000);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/" + name + "`(TYPE " + type + ") SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`, `DEFAULT_VALUE`=`abcde`);");
            FillCircle(0.1, 11000);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/" + name + "`(TYPE " + type + ") SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            FillCircle(0.2, 12000);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/" + name + "`(TYPE " + type + ") SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`);");
            FillCircle(0.3, 13000);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/" + name + "`(TYPE " + type + ") SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            FillCircle(0.4, 14000);
        }

        void ExecuteMultiColumn() {
            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            CSController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            CSController->SetOverridePeriodicWakeupActivationPeriod(TDuration::MilliSeconds(100));

            Tests::NCommon::TLoggerInit(Kikimr).Initialize();
            TTypedLocalHelper helper("Utf8", Kikimr);
            helper.CreateMultiColumnOlapTableWithStore(MultiColumnRepCount);

            auto start = TInstant::Now().Seconds();

            auto printTime = [&](const char* prefix) {
                auto finish = TInstant::Now().Seconds();
                fprintf(stderr, "Timing: %s took %lu seconds\n", prefix, finish - start);
                start = finish;
            };

            FillMultiColumnCircle(0, 10000);
            printTime("Fill");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_utf0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`, `DEFAULT_VALUE`=`abcde`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_int0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`, `DEFAULT_VALUE`=`0`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_uint0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`, `DEFAULT_VALUE`=`0`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_float0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`, `DEFAULT_VALUE`=`0`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_double0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`, `DEFAULT_VALUE`=`0`);");
            printTime("Alter");
            FillMultiColumnCircle(0.1, 11000);
            printTime("Fill");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_utf0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_int0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_uint0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_float0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_double0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            printTime("Alter");
            FillMultiColumnCircle(0.2, 12000);
            printTime("Fill");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_utf0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_int0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_uint0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_float0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_double0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`);");
            printTime("Alter");
            FillMultiColumnCircle(0.3, 13000);
            printTime("Fill");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_utf0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_int0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_uint0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_float0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field_double0, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            printTime("Alter");
            FillMultiColumnCircle(0.4, 14000);
            printTime("Fill");
        }
    };

    Y_UNIT_TEST(Switching) {
        TSparsedDataTest test("olapStore");
        test.Execute();
    }

    Y_UNIT_TEST(SwitchingMultiColumn) {
        TSparsedDataTest test("olapStore");
        test.ExecuteMultiColumn();
    }

    Y_UNIT_TEST(SwitchingStandalone) {
        TSparsedDataTest test("");
        test.Execute();
    }
}

} // namespace
