#include <ydb/core/kqp/common/stats/query_stats.h>
#include <ydb/core/kqp/common/stats/ru_calc.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/public/api/protos/ydb_query_stats.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpPublicQueryStats) {
    Y_UNIT_TEST(RequestUnitsAccountForIoAndCpu) {
        TProgressStatEntry stats;
        stats.ReadIOStat.Rows = 2;
        stats.ReadIOStat.Bytes = 9000;
        UNIT_ASSERT_VALUES_EQUAL(NRuCalc::CalcRequestUnit(stats), 3);

        stats.ComputeTime = TDuration::MicroSeconds(6000);
        UNIT_ASSERT_VALUES_EQUAL(NRuCalc::CalcRequestUnit(stats), 4);
        UNIT_ASSERT_VALUES_EQUAL(NRuCalc::CalcRequestUnit({}), 1);
    }

    Y_UNIT_TEST(QueryStatsIncludeCpuAndSortedTableAccesses) {
        NKikimrKqp::TQueryResponse response;
        auto& stats = *response.MutableQueryStats();
        stats.SetWorkerCpuTimeUs(3);
        stats.MutableCompilation()->SetCpuTimeUs(11);
        auto& execution = *stats.AddExecutions();
        execution.SetCpuTimeUs(7);
        auto& secondTable = *execution.AddTables();
        secondTable.SetTablePath("/Root/B");
        secondTable.SetReadRows(2);
        secondTable.SetReadBytes(100);
        execution.AddTables()->SetTablePath("/Root/A");
        response.SetQueryPlan("query plan");

        Ydb::TableStats::QueryStats result;
        FillQueryStats(result, response);
        UNIT_ASSERT_VALUES_EQUAL(result.total_cpu_time_us(), 21);
        UNIT_ASSERT_VALUES_EQUAL(result.process_cpu_time_us(), 3);
        UNIT_ASSERT_VALUES_EQUAL(result.query_plan(), "query plan");
        const auto& tables = result.query_phases(0).table_access();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(tables[0].name(), "/Root/A");
        UNIT_ASSERT_VALUES_EQUAL(tables[1].name(), "/Root/B");
        UNIT_ASSERT_VALUES_EQUAL(tables[1].reads().rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(tables[1].reads().bytes(), 100);
    }
}

} // namespace NKikimr::NKqp
