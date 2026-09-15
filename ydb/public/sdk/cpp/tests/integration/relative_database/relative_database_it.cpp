#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/datetime/base.h>

#include <cstdlib>
#include <string>

namespace NYdb::inline Dev {
    namespace {

        template <typename TResult>
        TResult Await(NThreading::TFuture<TResult> future) {
            return future.ExtractValue(TDuration::Seconds(60));
        }

        void CheckSuccess(const TStatus& status) {
            ASSERT_TRUE(status.IsSuccess()) << status.GetIssues().ToString();
        }

        void CheckRows(const TResultSet& result, size_t& rows) {
            TResultSetParser parser(result);
            ASSERT_EQ(parser.ColumnsCount(), 1u);
            while (parser.TryNextRow()) {
                ASSERT_EQ(parser.ColumnParser(0).GetUint64(), 42u);
                ++rows;
            }
        }

        template <typename TResult>
        void CheckQueryResult(const TResult& result) {
            ASSERT_NO_FATAL_FAILURE(CheckSuccess(result));
            ASSERT_EQ(result.GetResultSets().size(), 1u);
            size_t rows = 0;
            ASSERT_NO_FATAL_FAILURE(CheckRows(result.GetResultSet(0), rows));
            ASSERT_EQ(rows, 1u);
        }

        void CheckDiscovery(TDriver& driver) {
            NDiscovery::TDiscoveryClient client(driver);
            const auto endpoints = Await(client.ListEndpoints());
            ASSERT_NO_FATAL_FAILURE(CheckSuccess(endpoints));
            ASSERT_FALSE(endpoints.GetEndpointsInfo().empty());
            ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(client.WhoAmI())));
        }

        void CheckTable(TDriver& driver, const std::string& table, const std::string& absoluteTable) {
            SCOPED_TRACE("Table");
            NTable::TTableClient client(driver);
            const auto created = Await(client.CreateSession());
            ASSERT_NO_FATAL_FAILURE(CheckSuccess(created));
            auto session = created.GetSession();
            ASSERT_FALSE(session.GetId().empty());
            ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(session.KeepAlive())));

            ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(session.ExecuteSchemeQuery(
                "CREATE TABLE `" + table + "` (Key Uint64 NOT NULL, PRIMARY KEY (Key));"))));
            ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(session.ExecuteDataQuery(
                "UPSERT INTO `" + table + "` (Key) VALUES (42u);",
                NTable::TTxControl::BeginTx().CommitTx()))));

            for (const auto& path : {table, absoluteTable}) {
                SCOPED_TRACE(path);
                const auto sql = "SELECT Key FROM `" + path + "`;";
                ASSERT_NO_FATAL_FAILURE(CheckQueryResult(Await(session.ExecuteDataQuery(
                    sql, NTable::TTxControl::BeginTx().CommitTx()))));

                const auto prepared = Await(session.PrepareDataQuery(sql));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(prepared));
                ASSERT_FALSE(prepared.GetQuery().GetId().empty());
                ASSERT_NO_FATAL_FAILURE(CheckQueryResult(Await(prepared.GetQuery().Execute(
                    NTable::TTxControl::BeginTx().CommitTx()))));

                const auto explained = Await(session.ExplainDataQuery(sql));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(explained));
                ASSERT_FALSE(explained.GetPlan().empty());

                auto stream = Await(client.StreamExecuteScanQuery(sql));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(stream));
                size_t rows = 0;
                for (;;) {
                    const auto part = Await(stream.ReadNext());
                    if (part.EOS()) {
                        break;
                    }
                    ASSERT_NO_FATAL_FAILURE(CheckSuccess(part));
                    if (part.HasResultSet()) {
                        ASSERT_NO_FATAL_FAILURE(CheckRows(part.GetResultSet(), rows));
                    }
                }
                ASSERT_EQ(rows, 1u);
            }

            for (const bool commit : {true, false}) {
                SCOPED_TRACE(commit ? "CommitTransaction" : "RollbackTransaction");
                const auto begun = Await(session.BeginTransaction(NTable::TTxSettings::SerializableRW()));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(begun));
                auto tx = begun.GetTransaction();
                ASSERT_NO_FATAL_FAILURE(CheckQueryResult(Await(session.ExecuteDataQuery(
                    "SELECT Key FROM `" + table + "`;", NTable::TTxControl::Tx(tx)))));
                if (commit) {
                    ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(tx.Commit())));
                } else {
                    ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(tx.Rollback())));
                }
            }
            // Close sends Table.DeleteSession.
            ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(session.Close())));
        }

        void CheckQuery(TDriver& driver, const std::string& table, const std::string& absoluteTable) {
            SCOPED_TRACE("Query");
            NQuery::TQueryClient client(driver);
            // A fresh client's GetSession sends CreateSession and waits for AttachSession.
            const auto created = Await(client.GetSession());
            ASSERT_NO_FATAL_FAILURE(CheckSuccess(created));
            auto session = created.GetSession();
            ASSERT_FALSE(session.GetId().empty());

            for (const auto& path : {table, absoluteTable}) {
                SCOPED_TRACE(path);
                ASSERT_NO_FATAL_FAILURE(CheckQueryResult(Await(session.ExecuteQuery(
                    "SELECT Key FROM `" + path + "`;", NQuery::TTxControl::BeginTx().CommitTx()))));
            }

            for (const bool commit : {true, false}) {
                SCOPED_TRACE(commit ? "CommitTransaction" : "RollbackTransaction");
                const auto begun = Await(session.BeginTransaction(NQuery::TTxSettings::SerializableRW()));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(begun));
                auto tx = begun.GetTransaction();
                ASSERT_NO_FATAL_FAILURE(CheckQueryResult(Await(session.ExecuteQuery(
                    "SELECT Key FROM `" + table + "`;", NQuery::TTxControl::Tx(tx)))));
                if (commit) {
                    ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(tx.Commit())));
                } else {
                    ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(tx.Rollback())));
                }
            }
            ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(client.DeleteSession(session.GetId()))));
        }

        void CheckScripting(TDriver& driver, const std::string& table, const std::string& absoluteTable) {
            SCOPED_TRACE("Scripting");
            NScripting::TScriptingClient client(driver);
            for (const auto& path : {table, absoluteTable}) {
                SCOPED_TRACE(path);
                const auto sql = "SELECT Key FROM `" + path + "`;";

                // SDK names for ExecuteYql, ExplainYql and StreamExecuteYql.
                ASSERT_NO_FATAL_FAILURE(CheckQueryResult(Await(client.ExecuteYqlScript(sql))));
                const auto explained = Await(client.ExplainYqlScript(sql,
                                                                     NScripting::TExplainYqlRequestSettings().Mode(NScripting::ExplainYqlRequestMode::Plan)));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(explained));
                ASSERT_FALSE(explained.GetPlan().empty());

                auto stream = Await(client.StreamExecuteYqlScript(sql));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(stream));
                size_t rows = 0;
                for (;;) {
                    const auto part = Await(stream.ReadNext());
                    if (part.EOS()) {
                        break;
                    }
                    ASSERT_NO_FATAL_FAILURE(CheckSuccess(part));
                    if (part.HasPartialResult()) {
                        ASSERT_EQ(part.GetPartialResult().GetResultSetIndex(), 0u);
                        ASSERT_NO_FATAL_FAILURE(CheckRows(part.GetPartialResult().GetResultSet(), rows));
                    }
                }
                ASSERT_EQ(rows, 1u);
            }
        }

        void CheckScriptOperations(TDriver& driver, const std::string& table, const std::string& absoluteTable) {
            SCOPED_TRACE("Query scripts and Operation");
            NQuery::TQueryClient queryClient(driver);
            NOperation::TOperationClient operationClient(driver);
            using TOperation = NQuery::TScriptExecutionOperation;

            for (const auto& path : {table, absoluteTable}) {
                SCOPED_TRACE(path);
                auto operation = Await(queryClient.ExecuteScript("SELECT Key FROM `" + path + "`;"));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(operation.Status()));
                const auto id = operation.Id();
                const auto deadline = TInstant::Now() + TDuration::Seconds(60);
                // Always call Get, including when ExecuteScript returned a ready operation.
                do {
                    ASSERT_LT(TInstant::Now(), deadline) << "Script did not complete";
                    operation = Await(operationClient.Get<TOperation>(id));
                    ASSERT_NO_FATAL_FAILURE(CheckSuccess(operation.Status()));
                } while (!operation.Ready());
                ASSERT_EQ(operation.Id().ToString(), id.ToString());
                ASSERT_EQ(operation.Metadata().ExecStatus, NQuery::EExecStatus::Completed);

                const auto fetched = Await(queryClient.FetchScriptResults(id, 0));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(fetched));
                ASSERT_TRUE(fetched.HasResultSet());
                ASSERT_EQ(fetched.GetResultSetIndex(), 0u);
                ASSERT_TRUE(fetched.GetNextFetchToken().empty());
                size_t rows = 0;
                ASSERT_NO_FATAL_FAILURE(CheckRows(fetched.GetResultSet(), rows));
                ASSERT_EQ(rows, 1u);

                const auto listed = Await(operationClient.List<TOperation>(10));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(listed));
                ASSERT_EQ(listed.GetList().size(), 1u);
                ASSERT_EQ(listed.GetList().front().Id().ToString(), id.ToString());

                // CancelOperation must find this completed script and reject cancellation.
                // This does not test cancellation of a running script.
                const auto cancelled = Await(operationClient.Cancel(id));
                ASSERT_EQ(cancelled.GetStatus(), EStatus::PRECONDITION_FAILED) << cancelled.GetIssues().ToString();

                ASSERT_NO_FATAL_FAILURE(CheckSuccess(Await(operationClient.Forget(id))));
                const auto forgotten = Await(operationClient.Get<TOperation>(id));
                ASSERT_EQ(forgotten.Status().GetStatus(), EStatus::NOT_FOUND);
                const auto emptyList = Await(operationClient.List<TOperation>(10));
                ASSERT_NO_FATAL_FAILURE(CheckSuccess(emptyList));
                ASSERT_TRUE(emptyList.GetList().empty());
            }
        }

        class TRelativeDatabase: public testing::TestWithParam<std::string> {};

        TEST_P(TRelativeDatabase, MethodsWithoutResourcePaths) {
            const auto* endpoint = std::getenv("YDB_ENDPOINT");
            const auto* database = std::getenv("YDB_DATABASE");
            ASSERT_NE(endpoint, nullptr);
            ASSERT_NE(database, nullptr);
            ASSERT_EQ(std::string(database), "/Root/mydb");

            TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDatabase(GetParam()));
            const std::string table = "sdk_relative_database";
            const auto absoluteTable = std::string(database) + "/" + table;

            ASSERT_NO_FATAL_FAILURE(CheckDiscovery(driver));
            ASSERT_NO_FATAL_FAILURE(CheckTable(driver, table, absoluteTable));

            // An independent driver with the absolute database checks where DDL and writes landed.
            {
                TDriver absoluteDriver(TDriverConfig().SetEndpoint(endpoint).SetDatabase(database));
                NQuery::TQueryClient client(absoluteDriver);
                ASSERT_NO_FATAL_FAILURE(CheckQueryResult(Await(client.ExecuteQuery(
                    "SELECT Key FROM `" + absoluteTable + "`;", NQuery::TTxControl::BeginTx().CommitTx()))));
            }

            ASSERT_NO_FATAL_FAILURE(CheckQuery(driver, table, absoluteTable));
            ASSERT_NO_FATAL_FAILURE(CheckScripting(driver, table, absoluteTable));
            ASSERT_NO_FATAL_FAILURE(CheckScriptOperations(driver, table, absoluteTable));
            // DescribeTableOptions has no public C++ SDK method.
        }

        INSTANTIATE_TEST_SUITE_P(DatabasePaths, TRelativeDatabase,
                                 testing::Values(std::string("/Root/mydb"), std::string("Root/mydb"), std::string("mydb")));

    } // namespace
} // namespace NYdb::inline Dev
