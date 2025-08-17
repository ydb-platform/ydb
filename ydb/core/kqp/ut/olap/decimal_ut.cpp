#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/uuid/uuid.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

enum class EQueryMode {
    SCAN_QUERY,
    EXECUTE_QUERY
};

Y_UNIT_TEST_SUITE(KqpDecimalColumnShard) {
    class TDecimalTestCase {
    public:
        TDecimalTestCase(ui32 precision, ui32 scale)
            : TestHelper(TKikimrSettings().SetWithSampleTables(false))
            , Precision(precision)
            , Scale(scale)
        {}

        TTestHelper::TUpdatesBuilder Inserter() {
            return TTestHelper::TUpdatesBuilder(TestTable.GetArrowSchema(Schema));
        }

        void Upsert(TTestHelper::TUpdatesBuilder& inserter) {
            TestHelper.BulkUpsert(TestTable, inserter);
        }

        void CheckQuery(const TString& query, const TString& expected, EQueryMode mode = EQueryMode::SCAN_QUERY) const {
            switch (mode) {
            case EQueryMode::SCAN_QUERY:
                TestHelper.ReadData(query, expected);
                break;
            case EQueryMode::EXECUTE_QUERY: {
                TestHelper.ExecuteQuery(query);
                break;
            }
            }
        }

        void ExecuteDataQuery(const TString& query) const {
            TestHelper.ExecuteQuery(query);
        }

        void PrepareTable1() {
            Schema = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("dec").SetType(NScheme::TDecimalType(Precision, Scale)),
            };
            TestTable.SetName("/Root/Table1").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(Schema);
            TestHelper.CreateTable(TestTable);

            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(1).Add(4).Add(TDecimalValue("3.14", Precision, Scale));
                inserter.AddRow().Add(2).Add(3).Add(TDecimalValue("8.16", Precision, Scale));
                Upsert(inserter);
            }
            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(4).Add(1).Add(TDecimalValue("12.46", Precision, Scale));
                inserter.AddRow().Add(3).Add(2).Add(TDecimalValue("8.492", Precision, Scale));

                Upsert(inserter);
            }
        }

        void PrepareTable2() {
            Schema = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("table1_id").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("dec").SetType(NScheme::TDecimalType(Precision, Scale)),
            };
            TestTable.SetName("/Root/Table2").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(Schema);
            TestHelper.CreateTable(TestTable);

            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(1).Add(1).Add(TDecimalValue("12.46", Precision, Scale));
                inserter.AddRow().Add(2).Add(1).Add(TDecimalValue("8.16", Precision, Scale));
                inserter.AddRow().Add(3).Add(2).Add(TDecimalValue("12.46", Precision, Scale));
                inserter.AddRow().Add(4).Add(2).Add(TDecimalValue("8.16", Precision, Scale));
                Upsert(inserter);
            }
        }

    private:
        TTestHelper TestHelper;

        TVector<TTestHelper::TColumnSchema> Schema;
        TTestHelper::TColumnTable TestTable;

        YDB_READONLY_DEF(ui32, Precision);
        YDB_READONLY_DEF(ui32, Scale);
    };

    Y_UNIT_TEST_DUO(TestSimpleQueries, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        TDecimalTestCase tester35(35, 10);
        tester35.PrepareTable1();
        TDecimalTestCase tester12(12, 2);
        tester12.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TDecimalTestCase& tester) {
            tester.CheckQuery("SELECT * FROM `/Root/Table1` WHERE id=1", "[[[\"3.14\"];1;[4]]]", mode);
            
            TString expected;
            if (tester.GetPrecision() == 12 && tester.GetScale() == 2) {
                expected = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.49\"];3;[2]];[[\"12.46\"];4;[1]]]";
            } else {
                expected = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.492\"];3;[2]];[[\"12.46\"];4;[1]]]";
            }

            tester.CheckQuery("SELECT * FROM `/Root/Table1` order by id", expected, mode);
        };

        check(tester22);
        check(tester35);
        check(tester12);
    }

    Y_UNIT_TEST_DUO(TestFilterEqual, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        TDecimalTestCase tester35(35, 10);
        tester35.PrepareTable1();
        TDecimalTestCase tester12(12, 2);
        tester12.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TDecimalTestCase& tester, ui32 precision, ui32 scale) {
            tester.CheckQuery(TString::Join("SELECT * FROM `/Root/Table1` WHERE dec == cast(\"3.14\" as decimal(", ToString(precision), ",", ToString(scale), "))"), "[[[\"3.14\"];1;[4]]]", mode);
            
            TString expected;
            if (precision == 12 && scale == 2) {
                expected = "[[[\"8.16\"];2;[3]];[[\"8.49\"];3;[2]];[[\"12.46\"];4;[1]]]";
            } else {
                expected = "[[[\"8.16\"];2;[3]];[[\"8.492\"];3;[2]];[[\"12.46\"];4;[1]]]";
            }

            tester.CheckQuery(TString::Join("SELECT * FROM `/Root/Table1` WHERE dec != cast(\"3.14\" as decimal(", ToString(precision), ",", ToString(scale), ")) order by id"), expected, mode);
        };

        check(tester22, 22, 9);
        check(tester35, 35, 10);
        check(tester12, 12, 2);
    }

    Y_UNIT_TEST_DUO(TestFilterNulls, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        TDecimalTestCase tester35(35, 10);
        tester35.PrepareTable1();
        TDecimalTestCase tester12(12, 2);
        tester12.PrepareTable1();

        auto insert = [](TDecimalTestCase& tester) {
            TTestHelper::TUpdatesBuilder inserter = tester.Inserter();
            inserter.AddRow().Add(5).Add(5).AddNull();
            inserter.AddRow().Add(6).Add(6).AddNull();
            tester.Upsert(inserter);            
        };

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TDecimalTestCase& tester) {
            tester.CheckQuery("SELECT * FROM `/Root/Table1` WHERE dec is NULL order by id", "[[#;5;[5]];[#;6;[6]]]", mode);

            TString expected;
            if (tester.GetPrecision() == 12 && tester.GetScale() == 2) {
                expected = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.49\"];3;[2]];[[\"12.46\"];4;[1]]]";
            } else {
                expected = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.492\"];3;[2]];[[\"12.46\"];4;[1]]]";
            }

            tester.CheckQuery("SELECT * FROM `/Root/Table1` WHERE dec is not NULL order by id", expected, mode);
        };

        insert(tester22);
        insert(tester35);
        insert(tester12);

        check(tester22);
        check(tester35);
        check(tester12);
    }

    Y_UNIT_TEST_DUO(TestFilterCompare, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        TDecimalTestCase tester35(35, 10);
        tester35.PrepareTable1();
        TDecimalTestCase tester12(12, 2);
        tester12.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TDecimalTestCase& tester, ui32 precision, ui32 scale) {
            TString expected1, expected2, expected3, expected4;
            
            if (precision == 12 && scale == 2) {
                expected1 = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.49\"];3;[2]]]";
                expected2 = "[[[\"8.49\"];3;[2]];[[\"12.46\"];4;[1]]]";
                expected3 = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.49\"];3;[2]];[[\"12.46\"];4;[1]]]";
                expected4 = "[[[\"8.49\"];3;[2]];[[\"12.46\"];4;[1]]]";
            } else {
                expected1 = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.492\"];3;[2]]]";
                expected2 = "[[[\"8.492\"];3;[2]];[[\"12.46\"];4;[1]]]";
                expected3 = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.492\"];3;[2]];[[\"12.46\"];4;[1]]]";
                expected4 = "[[[\"8.492\"];3;[2]];[[\"12.46\"];4;[1]]]";
            }
            
            tester.CheckQuery(TString::Join("SELECT * FROM `/Root/Table1` WHERE dec < cast(\"12.46\" as decimal(", ToString(precision), ",", ToString(scale), ")) order by id"), expected1, mode);

            tester.CheckQuery(TString::Join("SELECT * FROM `/Root/Table1` WHERE dec > cast(\"8.16\" as decimal(", ToString(precision), ",", ToString(scale), ")) order by id"), expected2, mode);

            tester.CheckQuery(TString::Join("SELECT * FROM `/Root/Table1` WHERE dec <= cast(\"12.46\" as decimal(", ToString(precision), ",", ToString(scale), ")) order by id"), expected3, mode);

            tester.CheckQuery(TString::Join("SELECT * FROM `/Root/Table1` WHERE dec >= cast(\"8.492\" as decimal(", ToString(precision), ",", ToString(scale), ")) order by id"), expected4, mode);
        };

        check(tester22, 22, 9);
        check(tester35, 35, 10);
        check(tester12, 12, 2);
    }

    Y_UNIT_TEST_DUO(TestOrderByDecimal, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        TDecimalTestCase tester35(35, 10);
        tester35.PrepareTable1();
        TDecimalTestCase tester12(12, 2);
        tester12.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TDecimalTestCase& tester) {
            TString expected;
            if (tester.GetPrecision() == 12 && tester.GetScale() == 2) {
                expected = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.49\"];3;[2]];[[\"12.46\"];4;[1]]]";
            } else {
                expected = "[[[\"3.14\"];1;[4]];[[\"8.16\"];2;[3]];[[\"8.492\"];3;[2]];[[\"12.46\"];4;[1]]]";
            }

            tester.CheckQuery("SELECT * FROM `/Root/Table1` order by dec", expected, mode);
        };

        check(tester22);
        check(tester35);
        check(tester12);
    }

    Y_UNIT_TEST_DUO(TestGroupByDecimal, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        TDecimalTestCase tester35(35, 10);
        tester35.PrepareTable1();
        TDecimalTestCase tester12(12, 2);
        tester12.PrepareTable1();

        auto insert = [](TDecimalTestCase& tester) {
            TTestHelper::TUpdatesBuilder inserter = tester.Inserter();
            inserter.AddRow().Add(5).Add(12).Add(TDecimalValue("8.492", tester.GetPrecision(), tester.GetScale()));
            inserter.AddRow().Add(6).Add(30).Add(TDecimalValue("12.46", tester.GetPrecision(), tester.GetScale()));
            tester.Upsert(inserter);
        };

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TDecimalTestCase& tester) {
            TString expected;
            if (tester.GetPrecision() == 12 && tester.GetScale() == 2) {
                expected = "[[[\"3.14\"];1u];[[\"8.16\"];1u];[[\"8.49\"];2u];[[\"12.46\"];2u]]";
            } else {
                expected = "[[[\"3.14\"];1u];[[\"8.16\"];1u];[[\"8.492\"];2u];[[\"12.46\"];2u]]";
            }

            tester.CheckQuery("SELECT dec, count(*) FROM `/Root/Table1` group by dec order by dec", expected, mode);
        };

        insert(tester22);
        insert(tester35);
        insert(tester12);

        check(tester22);
        check(tester35);
        check(tester12);
    }

    Y_UNIT_TEST_DUO(TestAggregation, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        TDecimalTestCase tester35(35, 10);
        tester35.PrepareTable1();
        TDecimalTestCase tester12(12, 2);
        tester12.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TDecimalTestCase& tester) {
            tester.CheckQuery("SELECT min(dec) FROM `/Root/Table1`", "[[[\"3.14\"]]]", mode);
            tester.CheckQuery("SELECT max(dec) FROM `/Root/Table1`", "[[[\"12.46\"]]]", mode);
            
            TString expectedSum;
            if (tester.GetPrecision() == 12 && tester.GetScale() == 2) {
                expectedSum = "[[[\"32.25\"]]]";
            } else {
                expectedSum = "[[[\"32.252\"]]]";
            }

            tester.CheckQuery("SELECT sum(dec) FROM `/Root/Table1`", expectedSum, mode);
        };

        check(tester22);
        check(tester35);
        check(tester12);
    }

    Y_UNIT_TEST_DUO(TestJoinById, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        tester22.PrepareTable2();
        TDecimalTestCase tester35(35, 10);
        tester35.PrepareTable1();
        tester35.PrepareTable2();
        TDecimalTestCase tester12(12, 2);
        tester12.PrepareTable1();
        tester12.PrepareTable2();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TDecimalTestCase& tester) {
            tester.CheckQuery(
                "SELECT t1.id, t1.dec, t2.dec FROM `/Root/Table1` as t1 join `/Root/Table2` as t2 on t1.id = t2.table1_id order by t1.id, t1.dec, "
                "t2.dec",
                R"([[1;["3.14"];["8.16"]];[1;["3.14"];["12.46"]];[2;["8.16"];["8.16"]];[2;["8.16"];["12.46"]]])", mode);
        };

        check(tester22);
        check(tester35);
        check(tester12);
    }

    Y_UNIT_TEST_DUO(TestJoinByDecimal, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        tester22.PrepareTable2();
        TDecimalTestCase tester35(35, 10);
        tester35.PrepareTable1();
        tester35.PrepareTable2();
        TDecimalTestCase tester12(12, 2);
        tester12.PrepareTable1();
        tester12.PrepareTable2();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TDecimalTestCase& tester) {
            tester.CheckQuery(
                "SELECT t1.id, t2.id, t1.dec FROM `/Root/Table1` as t1 join `/Root/Table2` as t2 on t1.dec = t2.dec order by t1.id, t2.id, t1.dec",
                R"([[2;2;["8.16"]];[2;4;["8.16"]];[4;1;["12.46"]];[4;3;["12.46"]]])", mode);
        };

        check(tester22);
        check(tester35);
        check(tester12);
    }

    Y_UNIT_TEST_DUO(TestPMInfDecimal, UseScanQuery) {
        TDecimalTestCase tester22(22, 9);
        tester22.PrepareTable1();
        auto inserter = tester22.Inserter();
        inserter.AddRow().Add(1).Add(5).Add(TDecimalValue("999999999999999999999", 22, 9));
        inserter.AddRow().Add(2).Add(6).Add(TDecimalValue("-999999999999999999999", 22, 9));
        tester22.Upsert(inserter);
        
        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;
        
        tester22.CheckQuery("SELECT max(dec) FROM `/Root/Table1`", "[[[\"inf\"]]]", mode);
        tester22.CheckQuery("SELECT min(dec) FROM `/Root/Table1`", "[[[\"-inf\"]]]", mode);
    }
}

}   // namespace NKqp
}   // namespace NKikimr
