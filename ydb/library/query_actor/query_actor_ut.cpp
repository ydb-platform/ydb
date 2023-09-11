#include "query_actor.h"
#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr {

struct TTestQueryActorBase : public TQueryBase {
    struct TResult {
        Ydb::StatusIds::StatusCode StatusCode;
        NYql::TIssues Issues;
        std::vector<NYdb::TResultSet> ResultSets;

        TResult(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues, std::vector<NYdb::TResultSet> resultSets)
            : StatusCode(statusCode)
            , Issues(std::move(issues))
            , ResultSets(std::move(resultSets))
        {}
    };

    TTestQueryActorBase()
        : TQueryBase(NKikimrServices::KQP_PROXY, "", Tests::TestDomainName)
    {}

    NThreading::TFuture<TResult> GetResult() const {
        return Result.GetFuture();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        Result.SetValue(TResult(status, std::move(issues), std::move(ResultSets)));
    }

    NThreading::TPromise<TResult> Result = NThreading::NewPromise<TResult>();
};

struct TTestServer {
    TTestServer() {
        Init();
    }

    void Init() {
        Settings = MakeIntrusive<Tests::TServerSettings>(Pm.GetPort());
        Settings->SetGrpcPort(Pm.GetPort());
        Server = MakeHolder<Tests::TServer>(Settings);
        Client = MakeHolder<Tests::TClient>(*Settings);
        Client->InitRootScheme(Tests::TestDomainName);

        Client->CreateTable(Tests::TestDomainName, R"(
            Name: "TestTable"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "Value", Type: "String" }
            KeyColumnNames: ["Key"]
            )"
        );
    }

    template <class TQueryActor, class... TParams>
    NThreading::TFuture<TTestQueryActorBase::TResult> RunQueryActorAsync(TParams&&... params) {
        auto* actor = new TQueryActor(std::forward<TParams>(params)...);
        auto result = actor->GetResult();
        Server->GetRuntime()->Register(actor);
        return result;
    }

    template <class TQueryActor, class... TParams>
    TTestQueryActorBase::TResult RunQueryActor(TParams&&... params) {
        return RunQueryActorAsync<TQueryActor>(std::forward<TParams>(params)...).ExtractValueSync();
    }

    THolder<Tests::TServer> Server;
    THolder<Tests::TClient> Client;
    TIntrusivePtr<Tests::TServerSettings> Settings;
    TPortManager Pm;
};

Y_UNIT_TEST_SUITE(QueryActorTest) {
    Y_UNIT_TEST(SimpleQuery) {
        TTestServer server;

        struct TQuery : public TTestQueryActorBase {
            void OnRunQuery() override {
                RunDataQuery("SELECT 42");
            }

            void OnQueryResult() override {
                Finish();
            }
        };
        auto result = server.RunQueryActor<TQuery>();
        UNIT_ASSERT_VALUES_EQUAL(result.StatusCode, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(Rollback) {
        TTestServer server;

        struct TQuery : public TTestQueryActorBase {
            TQuery(ui64 val, bool commit)
                : Val(val)
                , Commit(commit)
            {}

            void OnRunQuery() override {
                NYdb::TParamsBuilder params;
                params
                    .AddParam("$k")
                        .Uint64(Val)
                        .Build()
                    .AddParam("$v")
                        .String(ToString(Val))
                        .Build();
                RunDataQuery(R"(
                    DECLARE $k As Uint64;
                    DECLARE $v As String;

                    UPSERT INTO TestTable (Key, Value) VALUES ($k, $v)
                    )", &params, Commit ? TTxControl::BeginAndCommitTx() : TTxControl::BeginTx());
            }

            void OnQueryResult() override {
                Finish(Commit ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::PRECONDITION_FAILED, "test issue");
            }

        private:
            ui64 Val = 0;
            bool Commit = false;
        };

        struct TSelectQuery : public TTestQueryActorBase {
            void OnRunQuery() override {
                RunDataQuery("SELECT * FROM TestTable");
            }

            void OnQueryResult() override {
                Finish();
            }
        };

        {
            auto result = server.RunQueryActor<TQuery>(42, true);
            UNIT_ASSERT_VALUES_EQUAL(result.StatusCode, Ydb::StatusIds::SUCCESS);
        }

        auto assertValues = [&](){
            auto result = server.RunQueryActor<TSelectQuery>();
            UNIT_ASSERT_VALUES_EQUAL(result.StatusCode, Ydb::StatusIds::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(result.ResultSets.size(), 1);
            NYdb::TResultSetParser parser(result.ResultSets[0]);
            UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 1);

            parser.TryNextRow();
            auto k = parser.ColumnParser("Key").GetOptionalUint64();
            UNIT_ASSERT(k);
            UNIT_ASSERT_VALUES_EQUAL(*k, 42);

            auto v = parser.ColumnParser("Value").GetOptionalString();
            UNIT_ASSERT(v);
            UNIT_ASSERT_VALUES_EQUAL(*v, "42");
        };
        assertValues();

        {
            auto result = server.RunQueryActor<TQuery>(10, false);
            UNIT_ASSERT_VALUES_EQUAL(result.StatusCode, Ydb::StatusIds::PRECONDITION_FAILED);
        }

        assertValues();
    }

    Y_UNIT_TEST(Commit) {
        TTestServer server;

        struct TSelectQuery : public TTestQueryActorBase {
            void OnRunQuery() override {
                RunDataQuery("SELECT * FROM TestTable", nullptr, TTxControl::BeginTx());
                SetQueryResultHandler(&TSelectQuery::MyResultHandler);
            }

            void MyResultHandler() {
                CommitTransaction(); // Finish will be after successful commit
            }

            void OnQueryResult() override {
                UNIT_ASSERT(false);
            }
        };

        auto result = server.RunQueryActor<TSelectQuery>();
        UNIT_ASSERT_VALUES_EQUAL(result.StatusCode, Ydb::StatusIds::SUCCESS);
    }
}

} // namespace NKikimr
