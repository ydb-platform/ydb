#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void PrintMemoryStats(const TString& testName, const TDataQueryResult& result) {
    if (!result.GetStats()) {
        return;
    }

    auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(stats.query_plan(), &plan, true);

    std::function<void(const NJson::TJsonValue&)> walkPlan = [&](const NJson::TJsonValue& node) {
        if (node.Has("Stats")) {
            const auto& s = node["Stats"];
            auto nodeType = node.Has("Node Type") ? node["Node Type"].GetStringRobust() : "?";
            ui64 stageId = s.Has("PhysicalStageId") ? s["PhysicalStageId"].GetUIntegerRobust() : 0;
            ui64 peakMem = 0;
            if (s.Has("MaxMemoryUsage") && s["MaxMemoryUsage"].Has("Sum")) {
                peakMem = s["MaxMemoryUsage"]["Sum"].GetUIntegerRobust();
            }
            ui64 outputBytes = s.Has("OutputBytes") ? s["OutputBytes"]["Sum"].GetUIntegerRobust() : 0;
            ui64 inputBytes = s.Has("InputBytes") ? s["InputBytes"]["Sum"].GetUIntegerRobust() : 0;
            ui64 durationUs = 0;
            if (s.Has("DurationUs") && s["DurationUs"].Has("Sum")) {
                durationUs = s["DurationUs"]["Sum"].GetUIntegerRobust();
            }
            ui64 cpuUs = 0;
            if (s.Has("CpuTimeUs") && s["CpuTimeUs"].Has("Sum")) {
                cpuUs = s["CpuTimeUs"]["Sum"].GetUIntegerRobust();
            }

            Cerr << "  Stage " << stageId << " (" << nodeType << ")"
                 << ": PeakMemory=" << (peakMem / 1024 / 1024) << "MB"
                 << ", Duration=" << (durationUs / 1000) << "ms"
                 << ", Cpu=" << (cpuUs / 1000) << "ms"
                 << ", In=" << (inputBytes / 1024 / 1024) << "MB"
                 << ", Out=" << (outputBytes / 1024 / 1024) << "MB"
                 << Endl;
        }

        if (node.Has("Plans")) {
            for (const auto& child : node["Plans"].GetArraySafe()) {
                walkPlan(child);
            }
        }
    };

    Cerr << "=== " << testName << " memory stats ===" << Endl;
    Cerr << "  TotalDuration=" << (stats.total_duration_us() / 1000) << "ms"
         << ", TotalCpu=" << (stats.total_cpu_time_us() / 1000) << "ms" << Endl;
    walkPlan(plan["Plan"]);
    Cerr << "================================================" << Endl;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpStreamLookup) {

    Y_UNIT_TEST(StreamLookupManyPartitions) {
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        constexpr ui64 TotalRows = 100000;
        constexpr ui64 BatchSize = 1000;

        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/MainTable` (
                    Key Uint64,
                    Fk Uint64,
                    Value String,
                    PRIMARY KEY (Key),
                    INDEX FkIndex GLOBAL ON (Fk)
                ) WITH (
                    UNIFORM_PARTITIONS = 2000,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2000
                );
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TString padding(10240, 'x');
        for (ui64 offset = 0; offset < TotalRows; offset += BatchSize) {
            auto params = db.GetParamsBuilder()
                .AddParam("$offset").Uint64(offset).Build()
                .AddParam("$padding").String(padding).Build()
                .Build();

            auto result = session.ExecuteDataQuery(Q1_(R"(
                DECLARE $offset AS Uint64;
                DECLARE $padding AS String;

                $data = ListMap(
                    ListFromRange($offset, $offset + 1000ul),
                    ($i) -> { RETURN AsStruct($i * 184467440737095ul AS Key, 1ul AS Fk, $padding AS Value); }
                );

                UPSERT INTO `/Root/MainTable`
                SELECT * FROM AS_TABLE($data);
            )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);

            auto result = session.ExecuteDataQuery(Q1_(R"(
                $q = SELECT Value FROM `/Root/MainTable` VIEW FkIndex WHERE Fk = 1;

                SELECT COUNT(Value) AS cnt FROM (
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                );
            )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            PrintMemoryStats("StreamLookupManyPartitions", result);

            auto rs = result.GetResultSet(0);
            TResultSetParser parser(rs);
            UNIT_ASSERT(parser.TryNextRow());
            ui64 cnt = parser.ColumnParser("cnt").GetUint64();
            Cerr << "StreamLookupManyPartitions: count = " << cnt << Endl;
            UNIT_ASSERT_VALUES_EQUAL(cnt, 8 * TotalRows);
        }
    }

    Y_UNIT_TEST(StreamLookupJoinManyPartitions) {
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        constexpr ui64 TotalRows = 100000;
        constexpr ui64 BatchSize = 1000;

        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/RightTable` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                ) WITH (
                    UNIFORM_PARTITIONS = 2000,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2000
                );

                CREATE TABLE `/Root/LeftTable` (
                    Id Uint64,
                    Fk Uint64,
                    PRIMARY KEY (Id)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TString padding(10240, 'x');
        for (ui64 offset = 0; offset < TotalRows; offset += BatchSize) {
            auto params = db.GetParamsBuilder()
                .AddParam("$offset").Uint64(offset).Build()
                .AddParam("$padding").String(padding).Build()
                .Build();

            auto result = session.ExecuteDataQuery(Q1_(R"(
                DECLARE $offset AS Uint64;
                DECLARE $padding AS String;

                $right = ListMap(
                    ListFromRange($offset, $offset + 1000ul),
                    ($i) -> { RETURN AsStruct($i * 184467440737095ul AS Key, $padding AS Value); }
                );

                UPSERT INTO `/Root/RightTable`
                SELECT * FROM AS_TABLE($right);

                $left = ListMap(
                    ListFromRange($offset, $offset + 1000ul),
                    ($i) -> { RETURN AsStruct($i AS Id, $i * 184467440737095ul AS Fk); }
                );

                UPSERT INTO `/Root/LeftTable`
                SELECT * FROM AS_TABLE($left);
            )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);

            auto result = session.ExecuteDataQuery(Q1_(R"(
                $q = SELECT b.Value AS Value
                    FROM `/Root/LeftTable` a
                    JOIN `/Root/RightTable` b ON a.Fk = b.Key;

                SELECT COUNT(Value) AS cnt FROM (
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                );
            )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            PrintMemoryStats("StreamLookupJoinManyPartitions", result);

            auto rs = result.GetResultSet(0);
            TResultSetParser parser(rs);
            UNIT_ASSERT(parser.TryNextRow());
            ui64 cnt = parser.ColumnParser("cnt").GetUint64();
            Cerr << "StreamLookupJoinManyPartitions: count = " << cnt << Endl;
            UNIT_ASSERT_VALUES_EQUAL(cnt, 8 * TotalRows);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
