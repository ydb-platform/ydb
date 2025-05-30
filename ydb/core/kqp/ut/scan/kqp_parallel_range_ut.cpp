#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpParallelRange) {
    Y_UNIT_TEST(ParallelPointRead) {
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
                WHERE Id IN (0, 268435455, 536870911, 805306367, 1073741823, 1342177279);
            )");
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 4);
        }
        {
            auto query = Q_(R"(
                SELECT Id, Value FROM TestSharded
                WHERE Id >= 123;
            )");
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                                               TExecuteQuerySettings{}.StatsMode(EStatsMode::Full))
                              .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 4);
        }
        {
            auto query = Q_(R"(
                SELECT Id, Value FROM TestSharded;
            )");
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                                               TExecuteQuerySettings{}.StatsMode(EStatsMode::Full))
                              .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 4);
        }
        {
            auto query = Q_(R"(
                SELECT Id, Value FROM TestSharded
                WHERE Id IN (0, 1, 268435455);
            )");
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                                               TExecuteQuerySettings{}.StatsMode(EStatsMode::Full))
                              .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 2);
        }
        {
            auto query = Q_(R"(
                SELECT Id, Value FROM TestSharded
                WHERE Id IN (0, 268435455, 536870911, 805306367, 1073741823, 1342177279);
            )");
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 4);
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
