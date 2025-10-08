#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/uuid/uuid.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

enum class EQueryMode {
    SCAN_QUERY,
    EXECUTE_QUERY
};

Y_UNIT_TEST_SUITE(KqpBoolColumnShard) {
    class TBoolTestCase {
    public:
        TBoolTestCase()
            : TestHelper(TKikimrSettings().SetWithSampleTables(false)) {
        }

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
                TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool),
            };

            TestTable.SetName("/Root/Table1").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(Schema);
            TestHelper.CreateTable(TestTable);

            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(1).Add(4).Add(true);
                inserter.AddRow().Add(2).Add(3).Add(false);
                Upsert(inserter);
            }
            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(4).Add(1).Add(true);
                inserter.AddRow().Add(3).Add(2).Add(true);

                Upsert(inserter);
            }
        }

        void PrepareTable2() {
            Schema = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("table1_id").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool),
            };
            TestTable.SetName("/Root/Table2").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(Schema);
            TestHelper.CreateTable(TestTable);

            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(1).Add(1).Add(true);
                inserter.AddRow().Add(2).Add(1).Add(false);
                inserter.AddRow().Add(3).Add(2).Add(true);
                inserter.AddRow().Add(4).Add(2).Add(false);
                Upsert(inserter);
            }
        }

    private:
        TTestHelper TestHelper;

        TVector<TTestHelper::TColumnSchema> Schema;
        TTestHelper::TColumnTable TestTable;
    };

    Y_UNIT_TEST_DUO(TestSimpleQueries, UseScanQuery) {
        return;
        TBoolTestCase tester;
        tester.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TBoolTestCase& t) {
            t.CheckQuery("SELECT * FROM `/Root/Table1` WHERE id=1", "[[[%true];1;[4]]]", mode);

            t.CheckQuery("SELECT * FROM `/Root/Table1` order by id", "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]", mode);
        };

        check(tester);
    }

    Y_UNIT_TEST_DUO(TestFilterEqual, UseScanQuery) {
        return;
        TBoolTestCase tester;
        tester.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TBoolTestCase& t) {
            t.CheckQuery("SELECT * FROM `/Root/Table1` WHERE b == true", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", mode);

            t.CheckQuery("SELECT * FROM `/Root/Table1` WHERE b != true order by id", "[[[%false];2;[3]]]", mode);
        };

        check(tester);
    }

    Y_UNIT_TEST_DUO(TestFilterNulls, UseScanQuery) {
        return;
        TBoolTestCase tester;
        tester.PrepareTable1();

        auto insert = [](TBoolTestCase& t) {
            TTestHelper::TUpdatesBuilder inserter = t.Inserter();
            inserter.AddRow().Add(5).Add(5).AddNull();
            inserter.AddRow().Add(6).Add(6).AddNull();
            t.Upsert(inserter);
        };

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TBoolTestCase& t) {
            t.CheckQuery("SELECT * FROM `/Root/Table1` WHERE b is NULL order by id", "[[[#];5;[5]];[[#];6;[6]]]", mode);

            t.CheckQuery("SELECT * FROM `/Root/Table1` WHERE b is not NULL order by id",
                "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]", mode);
        };

        insert(tester);
        check(tester);
    }

    Y_UNIT_TEST_DUO(TestFilterCompare, UseScanQuery) {
        return;
        TBoolTestCase tester;
        tester.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TBoolTestCase& t) {
            t.CheckQuery("SELECT * FROM `/Root/Table1` WHERE b < true order by id", "[[[%false];2;[3]]]", mode);

            t.CheckQuery("SELECT * FROM `/Root/Table1` WHERE b > false order by id", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", mode);

            t.CheckQuery("SELECT * FROM `/Root/Table1` WHERE b <= true order by id",
                "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]", mode);

            t.CheckQuery("SELECT * FROM `/Root/Table1` WHERE b >= true order by id", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", mode);
        };

        check(tester);
    }

    Y_UNIT_TEST_DUO(TestOrderByBool, UseScanQuery) {
        return;
        TBoolTestCase tester;
        tester.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TBoolTestCase& t) {
            t.CheckQuery("SELECT * FROM `/Root/Table1` order by b", "[[[%false];2;[3]];[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", mode);
        };

        check(tester);
    }

    Y_UNIT_TEST_DUO(TestGroupByBool, UseScanQuery) {
        return;
        TBoolTestCase tester;
        tester.PrepareTable1();

        auto insert = [](TBoolTestCase& t) {
            TTestHelper::TUpdatesBuilder inserter = t.Inserter();
            inserter.AddRow().Add(5).Add(12).Add(true);
            inserter.AddRow().Add(6).Add(30).Add(false);
            t.Upsert(inserter);
        };

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TBoolTestCase& t) {
            t.CheckQuery("SELECT b, count(*) FROM `/Root/Table1` group by b order by b", "[[[%false];2u];[[%true];4u]]", mode);
        };

        insert(tester);
        check(tester);
    }

    Y_UNIT_TEST_DUO(TestAggregation, UseScanQuery) {
        return;
        TBoolTestCase tester;
        tester.PrepareTable1();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TBoolTestCase& t) {
            t.CheckQuery("SELECT min(b) FROM `/Root/Table1`", "[[[%false]]]", mode);
            t.CheckQuery("SELECT max(b) FROM `/Root/Table1`", "[[[%true]]]", mode);
        };

        check(tester);
    }

    Y_UNIT_TEST_DUO(TestJoinById, UseScanQuery) {
        return;
        TBoolTestCase tester;
        tester.PrepareTable1();
        tester.PrepareTable2();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TBoolTestCase& t) {
            t.CheckQuery(
                "SELECT t1.id, t1.b, t2.b FROM `/Root/Table1` as t1 join `/Root/Table2` as t2 on t1.id = t2.table1_id order by t1.id, t1.b, "
                "t2.b",
                R"([[1;[%true];[%false]];[1;[%true];[%true]];[2;[%false];[%false]];[2;[%false];[%true]]])", mode);
        };

        check(tester);
    }

    Y_UNIT_TEST_DUO(TestJoinByBool, UseScanQuery) {
        return;
        TBoolTestCase tester;
        tester.PrepareTable1();
        tester.PrepareTable2();

        EQueryMode mode = UseScanQuery ? EQueryMode::SCAN_QUERY : EQueryMode::EXECUTE_QUERY;

        auto check = [mode](const TBoolTestCase& t) {
            t.CheckQuery(
                "SELECT t1.id, t2.id, t1.b FROM `/Root/Table1` as t1 join `/Root/Table2` as t2 on t1.b = t2.b order by t1.id, t2.id, t1.b",
                R"([[1;1;[%true]];[1;3;[%true]];[2;2;[%false]];[2;4;[%false]];[3;1;[%true]];[3;3;[%true]];[4;1;[%true]];[4;3;[%true]]])", mode);
        };

        check(tester);
    }
}

}   // namespace NKqp
}   // namespace NKikimr
