#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

#include <library/cpp/threading/local_executor/local_executor.h>

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(ConsistentIndexRead)
{
    Y_UNIT_TEST(InteractiveTx)
    {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto tableClient = TTableClient(driver);
        auto session = tableClient.GetSession().GetValueSync().GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/local/SecondaryKeys` (
                Key Int32,
                Fk Int32,
                Value String,
                PRIMARY KEY (Key),
                INDEX Index GLOBAL ON (Fk)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        static const TString deleteRow(R"(
            DELETE FROM `/local/SecondaryKeys` ON (Key) VALUES (2)
        )");

        static const TString addRow(R"(
            REPLACE INTO `/local/SecondaryKeys` (Key, Fk, Value) VALUES
            (2,    2,    "PPP")
        )");

        static const TString selectRow(R"(
            SELECT * FROM `/local/SecondaryKeys` VIEW Index WHERE Fk = 2;
        )");

        {
            auto result = session.ExecuteDataQuery(
                            addRow,
                            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                       .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        Sleep(TDuration::Seconds(10));

        NPar::LocalExecutor().RunAdditionalThreads(12);
        NPar::LocalExecutor().ExecRange([=, &tableClient](int id) mutable {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);

            size_t i = 5000;
            while (--i) {
                auto sessionResult = tableClient.GetSession().GetValueSync();
                if (!sessionResult.IsSuccess()) {
                    Cerr << sessionResult.GetStatus() << " " << sessionResult.GetIssues().ToString() << Endl;
                    continue;
                }
                auto s = sessionResult.GetSession();
                switch (id % 3) {
                    case 0: {
                        auto result = s.ExecuteDataQuery(
                                deleteRow,
                                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                                execSettings)
                            .ExtractValueSync();
                        UNIT_ASSERT_C(result.IsSuccess() || result.GetStatus() == EStatus::ABORTED, result.GetIssues().ToString());
                        break;
                    }
                    case 1: {
                        auto result = s.ExecuteDataQuery(
                                addRow,
                                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                                execSettings)
                           .ExtractValueSync();
                        UNIT_ASSERT_C(result.IsSuccess() || result.GetStatus() == EStatus::ABORTED, result.GetIssues().ToString());
                        break;
                    }
                    case 2: {
                        auto result = s.ExecuteDataQuery(
                                selectRow,
                                TTxControl::BeginTx(TTxSettings::SerializableRW()),
                                execSettings)
                            .ExtractValueSync();

                        // We expect read is always success
                        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                        if (result.IsSuccess()) {
                            auto tx = result.GetTransaction();
                            auto commitResult = tx->Commit().ExtractValueSync();
                            UNIT_ASSERT_C(commitResult.IsSuccess() || commitResult.GetStatus() == EStatus::ABORTED, commitResult.GetIssues().ToString());
                        }
                        break;
                    }
                }
            }
        }, 0, 12, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
    }
}

Y_UNIT_TEST_SUITE(KqpExtTest)
{
    Y_UNIT_TEST(SecondaryIndexSelectUsingScripting) {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        NYdb::NScripting::TScriptingClient client(driver);
        {
            const TString createTableSql(R"(
                --!syntax_v1
                CREATE TABLE `/local/SharedHouseholds` (
                    guest_huid Uint64, guest_id Uint64, owner_huid Uint64, owner_id Uint64, household_id String,
                    PRIMARY KEY (guest_huid, owner_huid, household_id),
                    INDEX shared_households_owner_huid GLOBAL SYNC ON (`owner_huid`)
                );)");
            auto result = client.ExecuteYqlScript(createTableSql).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const TString query(R"(
                --!syntax_v1
                SELECT
                    guest_id
                FROM
                    SharedHouseholds VIEW shared_households_owner_huid
                WHERE
                    owner_huid == 1 AND
                    household_id == "1";
            )");

            auto result = client.ExecuteYqlScript(query).GetValueSync();

            UNIT_ASSERT_C(result.GetIssues().Empty(), result.GetIssues().ToString());
            UNIT_ASSERT(result.IsSuccess());
        }
    }
}
