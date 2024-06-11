#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_replication.h>

#include <library/cpp/threading/local_executor/local_executor.h>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NReplication;

Y_UNIT_TEST_SUITE(Replication)
{
    Y_UNIT_TEST(Pause) {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto tableClient = TTableClient(driver);
        auto session = tableClient.GetSession().GetValueSync().GetSession();
        auto repl = TReplicationClient(driver);

        {
            auto res = session.ExecuteSchemeQuery(R"(
                CREATE TABLE ProducerWithIndex (
                    Key Uint32,
                    PRIMARY KEY (Key)
                )
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }
        {
            auto sessionResult = tableClient.GetSession().GetValueSync();
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
            auto s = sessionResult.GetSession();

            {
                // add initial value to master
                const TString query = "INSERT INTO ProducerWithIndex(Key) Values(1);";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }

            {
                // create replication to check pause
                const TString query = Sprintf("CREATE ASYNC REPLICATION replication1 FOR"
                    "`ProducerWithIndex` AS `ProducerWithIndex_replica1`"
                    "WITH ("
                        "CONNECTION_STRING = 'grpc://%s',"
                        "TOKEN = 'root@builtin'"
                    ");", connectionString.data());
                auto res = s.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                Sleep(TDuration::Seconds(10));
            }

            {
                // create replication to check not pause
                const TString query = Sprintf("CREATE ASYNC REPLICATION replication2 FOR"
                    "`ProducerWithIndex` AS `ProducerWithIndex_replica2`"
                    "WITH ("
                        "CONNECTION_STRING = 'grpc://%s',"
                        "TOKEN = 'root@builtin'"
                    ");", connectionString.data());
                auto res = s.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                Sleep(TDuration::Seconds(10));
            }

            {
                // check initial value is in replication1
                const TString query = "SELECT * FROM ProducerWithIndex_replica1";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(res.GetResultSets().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(res.GetResultSet(0).RowsCount(), 1);
            }

            {
                // add value to master
                const TString query = "INSERT INTO ProducerWithIndex(Key) Values(2);";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }

            {
                // check new value is in replication1
                const TString query = "SELECT * FROM ProducerWithIndex_replica1";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(res.GetResultSets().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(res.GetResultSet(0).RowsCount(), 2);
            }

            {
                // pause replication1
                const TString query = Sprintf("ALTER ASYNC REPLICATION replication1 "
                    "SET ("
                        "STATE = 'PAUSED'"
                    ");");
                auto res = s.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                Sleep(TDuration::Seconds(10));
            }

            {
                // add new value to master
                const TString query = "INSERT INTO ProducerWithIndex(Key) Values(3);";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }

            {
                // check new value is not in paused replica
                const TString query = "SELECT * FROM ProducerWithIndex_replica1";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(res.GetResultSets().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(res.GetResultSet(0).RowsCount(), 2);
            }

            {
                // check new value is in not paused replica
                const TString query = "SELECT * FROM ProducerWithIndex_replica2";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(res.GetResultSets().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(res.GetResultSet(0).RowsCount(), 3);
            }
        }
    }
}
