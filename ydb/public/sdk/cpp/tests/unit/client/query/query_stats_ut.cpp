#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/stats.h>

#include <ydb/public/api/protos/ydb_query_stats.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(QueryStats) {
    Y_UNIT_TEST(MapsProtoValues) {
        Ydb::TableStats::QueryStats proto;
        proto.set_process_cpu_time_us(10);
        proto.set_total_duration_us(20);
        proto.set_total_cpu_time_us(30);
        proto.set_query_plan("plan");
        proto.set_query_ast("ast");
        proto.set_query_meta("meta");

        auto* compilation = proto.mutable_compilation();
        compilation->set_from_cache(true);
        compilation->set_duration_us(40);
        compilation->set_cpu_time_us(50);

        auto* phase = proto.add_query_phases();
        phase->set_duration_us(60);
        phase->set_cpu_time_us(70);
        phase->set_affected_shards(80);
        phase->set_literal_phase(true);

        auto* table = phase->add_table_access();
        table->set_name("table");
        table->set_partitions_count(90);
        table->mutable_reads()->set_rows(100);
        table->mutable_reads()->set_bytes(110);
        table->mutable_updates()->set_rows(120);
        table->mutable_updates()->set_bytes(130);
        table->mutable_deletes()->set_rows(140);
        table->mutable_deletes()->set_bytes(150);

        const TExecStats stats(proto);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetProcessCpuTimeUs(), 10);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetProcessCpuTime().MicroSeconds(), 10);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTotalDurationUs(), 20);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTotalDuration().MicroSeconds(), 20);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTotalCpuTimeUs(), 30);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTotalCpuTime().MicroSeconds(), 30);
        const auto plan = stats.GetPlan();
        const auto ast = stats.GetAst();
        const auto meta = stats.GetMeta();
        UNIT_ASSERT_VALUES_EQUAL(*plan, "plan");
        UNIT_ASSERT_VALUES_EQUAL(*ast, "ast");
        UNIT_ASSERT_VALUES_EQUAL(*meta, "meta");

        const auto compilationStats = stats.GetCompilation();
        UNIT_ASSERT(compilationStats);
        UNIT_ASSERT(compilationStats->IsFromCache());
        UNIT_ASSERT_VALUES_EQUAL(compilationStats->GetDurationUs(), 40);
        UNIT_ASSERT_VALUES_EQUAL(compilationStats->GetDuration().MicroSeconds(), 40);
        UNIT_ASSERT_VALUES_EQUAL(compilationStats->GetCpuTimeUs(), 50);
        UNIT_ASSERT_VALUES_EQUAL(compilationStats->GetCpuTime().MicroSeconds(), 50);

        const auto phases = stats.GetQueryPhases();
        UNIT_ASSERT_VALUES_EQUAL(phases.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(phases[0].GetDurationUs(), 60);
        UNIT_ASSERT_VALUES_EQUAL(phases[0].GetDuration().MicroSeconds(), 60);
        UNIT_ASSERT_VALUES_EQUAL(phases[0].GetCpuTimeUs(), 70);
        UNIT_ASSERT_VALUES_EQUAL(phases[0].GetCpuTime().MicroSeconds(), 70);
        UNIT_ASSERT_VALUES_EQUAL(phases[0].GetAffectedShards(), 80);
        UNIT_ASSERT(phases[0].IsLiteralPhase());

        const auto& tables = phases[0].GetTableAccess();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tables[0].GetName(), "table");
        UNIT_ASSERT_VALUES_EQUAL(tables[0].GetPartitionsCount(), 90);
        UNIT_ASSERT_VALUES_EQUAL(tables[0].GetReads().GetRows(), 100);
        UNIT_ASSERT_VALUES_EQUAL(tables[0].GetReads().GetBytes(), 110);
        UNIT_ASSERT_VALUES_EQUAL(tables[0].GetUpdates().GetRows(), 120);
        UNIT_ASSERT_VALUES_EQUAL(tables[0].GetUpdates().GetBytes(), 130);
        UNIT_ASSERT_VALUES_EQUAL(tables[0].GetDeletes().GetRows(), 140);
        UNIT_ASSERT_VALUES_EQUAL(tables[0].GetDeletes().GetBytes(), 150);
    }

    Y_UNIT_TEST(KeepsMissingOptionalFieldsEmpty) {
        const TExecStats stats(Ydb::TableStats::QueryStats{});
        UNIT_ASSERT(!stats.GetCompilation());
        UNIT_ASSERT(!stats.GetPlan());
        UNIT_ASSERT(!stats.GetAst());
        UNIT_ASSERT(!stats.GetMeta());
        UNIT_ASSERT(stats.GetQueryPhases().empty());
    }
}
