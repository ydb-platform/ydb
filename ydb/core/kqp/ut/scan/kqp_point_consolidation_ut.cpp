#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

NKikimrConfig::TAppConfig GetAppConfig(bool pointConsolidation = false) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableParallelPointReadConsolidation(pointConsolidation);
    return appConfig;
}

void CreateSampleTable(TSession& session) {
    {
        const auto query = Q_(R"(
            CREATE TABLE `TestSharded` (
                Id Uint32 NOT NULL,
                Value Uint64,
                PRIMARY KEY(Id)
            ) WITH (
                UNIFORM_PARTITIONS = 16
            );
        )");

        const auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    {
        const auto query = Q_(R"(
            UPSERT INTO `TestSharded` (Id, Value) VALUES
                (0, 0), (70640909, 4), (141281818, 3), (211922728, 3),
                (268435455, 2), (353204547, 2), (423845456, 1), (494486365, 0),
                (536870911, 0), (635768184, 4), (706409094, 4), (777050003, 3),
                (805306367, 3), (918331822, 2), (988972731, 1), (1073741823, 1),
                (1130254550, 0), (1200895460, 0), (1271536369, 4), (1342177279, 4);
        )");

        const auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpPointConsolidation) {
    Y_UNIT_TEST_TWIN(TasksCount, PointConsolidation) {
        TKikimrRunner kikimr(GetAppConfig(PointConsolidation));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        {
            /*
                There are only 2 points in the range:
                [0;0], [268435455;268435455]

                With EnableParallelPointReadConsolidation the query will use 1 task for reading, without it 2 tasks.
            */

            const auto query = Q_(R"(
                SELECT Id, Value FROM `TestSharded`
                WHERE Id IN (0, 268435455);
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));

            const auto tasksCount = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe();
            UNIT_ASSERT_VALUES_EQUAL(tasksCount, PointConsolidation ? 1 : 2);
        }
        {
            /*
                There are only 6 points in the range:
                [0;0], [268435455;268435455], [536870911;536870911], ...

                With EnableParallelPointReadConsolidation the query will use 1 task for reading, without it 4 (max) tasks.
            */

            const auto query = Q_(R"(
                SELECT Id, Value FROM `TestSharded`
                WHERE Id IN (0, 268435455, 536870911, 805306367, 1073741823, 1342177279);
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));

            const auto tasksCount = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe();
            UNIT_ASSERT_VALUES_EQUAL(tasksCount, PointConsolidation ? 1 : 4);
        }
        {
            /*
                There is one segment with another points as ranges:
                [0;1], [268435455;268435455], [536870911;536870911], ...

                The query will use 4 (max) tasks regardless of the consolidation setting.
            */

            const auto query = Q_(R"(
                SELECT Id, Value FROM `TestSharded`
                WHERE Id IN (0, 1, 268435455, 536870911, 805306367, 1073741823, 1342177279);
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));

            const auto tasksCount = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe();
            UNIT_ASSERT_VALUES_EQUAL(tasksCount, PointConsolidation ? 4 : 4);
        }
        {
            /*
                This is a full range scan starting from 0 to infinity:
                [0;+infinity)

                The query will use 4 (max) tasks regardless of the consolidation setting.
            */

            const auto query = Q_(R"(
                SELECT Id, Value FROM `TestSharded`
                WHERE Id >= 0;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));

            const auto tasksCount = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe();
            UNIT_ASSERT_VALUES_EQUAL(tasksCount, PointConsolidation ? 4 : 4);
        }
    }

    Y_UNIT_TEST(SequentialPointRead) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                CREATE TABLE `TestSharded` (
                    Id Uint32,
                    Value Uint64,
                    PRIMARY KEY(Id)
                ) WITH (
                    UNIFORM_PARTITIONS = 16
                );
            )");
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto query = Q_(R"(
                SELECT Id, Value FROM TestSharded
                WHERE Id IN (0, 268435455, 536870911, 805306367, 1073741823, 1342177279)
                LIMIT 15;
            )");
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), 
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 1);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
