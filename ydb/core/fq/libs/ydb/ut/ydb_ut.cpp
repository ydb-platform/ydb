#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/util.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/base/backtrace.h>

namespace NFq {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NKikimr;
using TTxControl = NFq::ISession::TTxControl;

namespace {

template<typename TValue>
class TProxyActor : public TActorBootstrapped<TProxyActor<TValue>> {
public:
    TProxyActor(NThreading::TPromise<TValue> p, std::function<TValue()> operation)
        : Promise(p)
        , Operation(operation) { }

    void Bootstrap() { Promise.SetValue(Operation()); }
    NThreading::TPromise<TValue> Promise;
    std::function<TValue()> Operation;
};

////////////////////////////////////////////////////////////////////////////////

TFuture<TStatus> UpsertDummyInTransaction(const TFuture<TStatus>& future, const TGenerationContextPtr& context) {
    return future.Apply(
        [context] (const TFuture<TStatus>& future) {
            if (future.HasException() || !future.GetValue().IsSuccess()) {
                return future;
            }

            if (!context->Session->HasActiveTransaction()) {
                auto status = MakeErrorStatus(EStatus::INTERNAL_ERROR, "no transaction");
                return MakeFuture(status);
            }

            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");

                UPSERT INTO dummy (id) VALUES
                    ("ID DQD");
            )", context->TablePathPrefix.c_str());

            auto ttxControl = TTxControl::ContinueAndCommitTx();
            return context->Session->ExecuteDataQuery(query, ttxControl, nullptr).Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
                });
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template<bool UseYdbSdk>
class TRegisterCheckTestBase: public NUnitTest::TTestBase {
    using TSelf = TRegisterCheckTestBase<UseYdbSdk>;

    IYdbConnection::TPtr Connection;
    TPortManager PortManager;
    ui16 MsgBusPort = 0;
    ui16 GrpcPort = 0;
    THolder<Tests::TServerSettings> ServerSettings;
    THolder<Tests::TServer> Server;
    THolder<Tests::TClient> Client;
public:

    IYdbConnection::TPtr MakeConnection() {
        NConfig::TYdbStorageConfig config;

        config.SetEndpoint(GetEnv("YDB_ENDPOINT"));
        config.SetDatabase(GetEnv("YDB_DATABASE"));
        config.SetToken("");
        config.SetTablePrefix(CreateGuidAsString());

        NYdb::TDriver driver({});
        if (UseYdbSdk) {
            Connection = CreateSdkYdbConnection(config, NKikimr::CreateYdbCredentialsProviderFactory, driver);
        } else {
            Connection = CreateLocalYdbConnection(Server->GetRuntime()->GetAppData().TenantName, ".metadata/streaming/checkpoints");
        }

        auto desc = TTableBuilder()
            .AddNullableColumn("id", EPrimitiveType::String)
            .AddNullableColumn("generation", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("id")
            .Build();

        auto status = CreateTable("test", std::move(desc)).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        return Connection;
    }

    void SetUp() override {
        if (!UseYdbSdk) {
            InitTestServer();
        }
        Connection = MakeConnection();
    }

    void TearDown() override {
        if (!UseYdbSdk) {
            return; // DropTable is not supported
        }
        if (Connection) {
            auto tablePath = JoinPath(Connection->GetTablePathPrefix(), "test");
            RetryOperation(
                [tablePath = std::move(tablePath)] (ISession::TPtr session) mutable {
                    return session->DropTable(tablePath);
                }).GetValueSync();;
        }
    }

     static void BackTraceSignalHandler(int signal) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cerr);
        Cerr << colors.Red() << "======= " << signal << " call stack ========" << colors.Default() << Endl;
        FormatBackTrace(&Cerr);
        Cerr << colors.Red() << "===============================================" << colors.Default() << Endl;
        abort();
    }

    void InitTestServer() {
        EnableYDBBacktraceFormat();
        for (auto sig : {SIGILL, SIGSEGV}) {
            signal(sig, &TSelf::BackTraceSignalHandler);
        }

        MsgBusPort = PortManager.GetPort(2134);
        NKikimrProto::TAuthConfig authConfig;
        ServerSettings = MakeHolder<Tests::TServerSettings>(MsgBusPort, authConfig);
        ServerSettings->NodeCount = 1;
        Server = MakeHolder<Tests::TServer>(*ServerSettings);
        Client = MakeHolder<Tests::TClient>(*ServerSettings);
        Client->InitRootScheme();

        Sleep(TDuration::Seconds(1));
        Cerr << "\n\n\n--------------------------- INIT FINISHED ---------------------------\n\n\n";
    }

    TFuture<TStatus> CheckTransactionClosed(const TFuture<TStatus>& future, const TGenerationContextPtr& context) {
        return future.Apply(
            [context] (const TFuture<TStatus>& future) {
                if (context->Session->HasActiveTransaction()) {
                    auto status = MakeErrorStatus(EStatus::INTERNAL_ERROR, "unfinished transaction");
                    return MakeFuture(status);
                }
                return future;
            });
    }

    template<typename TValue>
    auto Call(std::function<TValue()> operation) {
        if (UseYdbSdk) {
            return operation();     
        }
        auto promise = NThreading::NewPromise<TValue>();
        Server->GetRuntime()->Register(new TProxyActor<TValue>(promise, operation));
        return promise.GetFuture().GetValueSync();
    }

    NYdb::TAsyncStatus RetryOperation(
        std::function<NYdb::TAsyncStatus(ISession::TPtr)> operation) {
        auto f = [&](){ return Connection->GetTableClient()->RetryOperation(std::move(operation)); };
        return Call<NYdb::TAsyncStatus>(f);
    }

    NYdb::TAsyncStatus CreateTable(
        const TString& name,
        NYdb::NTable::TTableDescription&& description) {
        auto f = [&]() { return NFq::CreateTable(Connection, name, std::move(description));};
        return Call<NYdb::TAsyncStatus>(f);
    }

    UNIT_TEST_SUITE_DEMANGLE(TSelf);
    UNIT_TEST(ShouldRegisterCheckNewGeneration);
    UNIT_TEST(ShouldRegisterCheckSameGeneration);
    UNIT_TEST(ShouldRegisterCheckNextGeneration);
    UNIT_TEST(ShouldNotRegisterCheckPrevGeneration);
    UNIT_TEST(ShouldNotRegisterCheckPrevGeneration2);
    UNIT_TEST(ShouldRegisterCheckNewGenerationAndTransact);
    UNIT_TEST(ShouldRegisterCheckSameGenerationAndTransact);
    UNIT_TEST(ShouldRollbackTransactionWhenCheckFails);
    UNIT_TEST(ShouldRollbackTransactionWhenCheckFails2);
    UNIT_TEST_SUITE_END();

    void ShouldRegisterCheckNewGeneration() {
        auto future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix(), this] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                auto future = RegisterCheckGeneration(context);
                return CheckTransactionClosed(future, context);
            });

        auto status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    void ShouldRegisterCheckSameGeneration()
    {
        auto future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                return RegisterCheckGeneration(context);
            });

        auto status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix(), this] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                auto future = RegisterCheckGeneration(context);
                return CheckTransactionClosed(future, context);
            });

        status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    void ShouldRegisterCheckNextGeneration()
    {
        auto future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                return RegisterCheckGeneration(context);
            });

        auto status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    12UL);

                return RegisterCheckGeneration(context);
            });

        status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    void ShouldNotRegisterCheckPrevGeneration()
    {
        auto future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                return RegisterCheckGeneration(context);
            });

        auto status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix(), this] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    10UL);

                auto future = RegisterCheckGeneration(context);
                return CheckTransactionClosed(future, context);
            });

        status = future.GetValueSync();
        UNIT_ASSERT(!status.IsSuccess());
    }

    void ShouldNotRegisterCheckPrevGeneration2()
    {
        auto future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                return RegisterCheckGeneration(context);
            });

        auto status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix(), this] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    false, // the only difference with ShouldNotRegisterCheckPrevGeneration
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    10UL);

                auto future = RegisterCheckGeneration(context);
                return CheckTransactionClosed(future, context);
            });

        status = future.GetValueSync();
        UNIT_ASSERT(!status.IsSuccess());
    }

    void ShouldRegisterCheckNewGenerationAndTransact()
    {
        auto desc = TTableBuilder()
            .AddNullableColumn("id", EPrimitiveType::String)
            .SetPrimaryKeyColumn("id")
            .Build();

        auto status = CreateTable("dummy", std::move(desc)).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    false,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                auto future = RegisterCheckGeneration(context);
                return UpsertDummyInTransaction(future, context);
            });

        status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    void ShouldRegisterCheckSameGenerationAndTransact()
    {
        auto desc = TTableBuilder()
            .AddNullableColumn("id", EPrimitiveType::String)
            .SetPrimaryKeyColumn("id")
            .Build();

        auto status = CreateTable("dummy", std::move(desc)).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                return RegisterCheckGeneration(context);
            });

        status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    false,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                auto future = RegisterCheckGeneration(context);
                return UpsertDummyInTransaction(future, context);
            });

        status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

    }

    void ShouldRollbackTransactionWhenCheckFails()
    {
        auto future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                return RegisterGeneration(context);
            });

        auto status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix(), this] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    false,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    10UL);

                auto future = RegisterCheckGeneration(context);
                return CheckTransactionClosed(future, context);
            });

        status = future.GetValueSync();
        UNIT_ASSERT(!status.IsSuccess());
    }

    void ShouldRollbackTransactionWhenCheckFails2()
    {
        auto future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix()] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true,
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    11UL);

                return RegisterGeneration(context);
            });

        auto status = future.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        future = RetryOperation(
            [prefix = Connection->GetTablePathPrefix(), this] (ISession::TPtr session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    true, // the only difference with ShouldRollbackTransactionWhenCheckFails
                    prefix,
                    "test",
                    "id",
                    "generation",
                    "masterId",
                    10UL);

                auto future = RegisterCheckGeneration(context);
                return CheckTransactionClosed(future, context);
            });

        status = future.GetValueSync();
        UNIT_ASSERT(!status.IsSuccess());
    }
};

using TRegisterCheckTest = TRegisterCheckTestBase<true>;
using TRegisterCheckLocalTest = TRegisterCheckTestBase<false>;

UNIT_TEST_SUITE_REGISTRATION(TRegisterCheckTest);
UNIT_TEST_SUITE_REGISTRATION(TRegisterCheckLocalTest);

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFqYdbTest) {
    Y_UNIT_TEST(ShouldStatusToIssuesProcessExceptions)
    {
        auto promise = NThreading::NewPromise<NYdb::TStatus>();
        auto future = promise.GetFuture();
        TString text("Test exception");
        promise.SetException(text);
        NThreading::TFuture<NYql::TIssues> future2 = NFq::StatusToIssues(future);

        NYql::TIssues issues = future2.GetValueSync();
        UNIT_ASSERT(issues.Size() == 1);
        UNIT_ASSERT(issues.ToString().Contains(text));
    }

    Y_UNIT_TEST(ShouldStatusToIssuesProcessEmptyIssues)
    {
        auto promise = NThreading::NewPromise<NYdb::TStatus>();
        auto future = promise.GetFuture();
        promise.SetValue(TStatus(EStatus::BAD_REQUEST, NYdb::NIssue::TIssues{}));
        NThreading::TFuture<NYql::TIssues> future2 = NFq::StatusToIssues(future);

        NYql::TIssues issues = future2.GetValueSync();
        UNIT_ASSERT_C(issues.Size() == 1, issues.ToString());
        UNIT_ASSERT(issues.ToString().Contains("empty issues"));
    }
}

} // namespace NFq
