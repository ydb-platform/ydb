#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/util.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>
#include <ydb/core/testlib/test_client.h>

namespace NFq {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NKikimr;
using TTxControl = NFq::ISession::TTxControl;

namespace {

////////////////////////////////////////////////////////////////////////////////

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

template<bool UseSdkConnection>
class TRegisterCheckTestBase: public NUnitTest::TTestBase/*, public TMyFixture<UseSdkConnection>*/ {
    using TSelf = TRegisterCheckTestBase<UseSdkConnection>;

    IYdbConnection::TPtr Connection;
    TPortManager PortManager;
    ui16 MsgBusPort = 0;
    ui16 GrpcPort = 0;
    THolder<Tests::TServerSettings> ServerSettings;
    THolder<Tests::TServer> Server;
    THolder<Tests::TClient> Client;
public:

    IYdbConnection::TPtr MakeConnection(const char* tablePrefix) {
        NKikimrConfig::TExternalStorage config;

        config.SetEndpoint(GetEnv("YDB_ENDPOINT"));
        config.SetDatabase(GetEnv("YDB_DATABASE"));
        config.SetToken("");
        config.SetTablePrefix(tablePrefix);

        NYdb::TDriver driver({});
        if (UseSdkConnection) {
            Connection = CreateSdkYdbConnection(config, NKikimr::CreateYdbCredentialsProviderFactory, driver);
        } else {
            InitLocalConnection();
            Connection = CreateLocalYdbConnection("db", "test/checkpoints");
        }

        // auto status = Connection->SchemeClient.MakeDirectory(Connection->TablePathPrefix).GetValueSync();
        // UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto desc = TTableBuilder()
            .AddNullableColumn("id", EPrimitiveType::String)
            .AddNullableColumn("generation", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("id")
            .Build();

        auto status = CreateTable(Connection, "test", std::move(desc)).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        return Connection;
    }

    void TearDown() override {
        if (Connection) {
            auto tablePath = JoinPath(Connection->GetTablePathPrefix(), "test");
            Connection->GetTableClient()->RetryOperation(
                [tablePath = std::move(tablePath)] (ISession::TPtr session) mutable {
                    return session->DropTable(tablePath);
                }).GetValueSync();;
        }
    }

    void InitLocalConnection() {
        MsgBusPort = PortManager.GetPort(2134);
        GrpcPort = PortManager.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBuiltinDomain(true);
        ServerSettings = MakeHolder<Tests::TServerSettings>(MsgBusPort, authConfig);
        ServerSettings->AppConfig->MutableQueryServiceConfig()->MutableCheckpointsConfig()->SetEnabled(true);
        ServerSettings->AppConfig->MutableFeatureFlags()->SetEnableStreamingQueries(true);

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableStreamingQueries(true);
        ServerSettings->SetFeatureFlags(featureFlags);

        ServerSettings->SetEnableScriptExecutionOperations(true);
        ServerSettings->SetInitializeFederatedQuerySetupFactory(true);
        ServerSettings->SetGrpcPort(GrpcPort);
        ServerSettings->NodeCount = 1;
        Server = MakeHolder<Tests::TServer>(*ServerSettings);
        Client = MakeHolder<Tests::TClient>(*ServerSettings);
     //   Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        Server->GetRuntime()->SetLogPriority(NKikimrServices::STREAMS_STORAGE_SERVICE, NActors::NLog::PRI_DEBUG);
       // Server->GetRuntime()->SetDispatchTimeout(TestTimeout);
        Server->EnableGRpc(GrpcPort);
        Client->InitRootScheme();

        Sleep(TDuration::Seconds(5));
        Cerr << "\n\n\n--------------------------- INIT FINISHED ---------------------------\n\n\n";
    }

   // static constexpr char const* SuiteName = "TRegisterCheckTestBase";//#UseSdkConnection;

    // UNIT_TEST_SUITE(TRegisterCheckTestBase);
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
        auto connection = MakeConnection("ShouldRegisterCheckNewGeneration");

        auto future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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
        auto connection = MakeConnection("ShouldRegisterCheckSameGeneration");

        auto future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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

        future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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
        auto connection = MakeConnection("ShouldRegisterCheckNextGeneration");

        auto future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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

        future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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
        auto connection = MakeConnection("ShouldNotRegisterCheckPrevGeneration");

        auto future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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

        future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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
        auto connection = MakeConnection("ShouldNotRegisterCheckPrevGeneration2");

        auto future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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

        future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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
        auto connection = MakeConnection("ShouldRegisterCheckNewGenerationAndTransact");

        auto desc = TTableBuilder()
            .AddNullableColumn("id", EPrimitiveType::String)
            .SetPrimaryKeyColumn("id")
            .Build();

        auto status = CreateTable(connection, "dummy", std::move(desc)).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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
        auto connection = MakeConnection("ShouldRegisterCheckNewGenerationAndTransact");

        auto desc = TTableBuilder()
            .AddNullableColumn("id", EPrimitiveType::String)
            .SetPrimaryKeyColumn("id")
            .Build();

        auto status = CreateTable(connection, "dummy", std::move(desc)).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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

        future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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
        auto connection = MakeConnection("ShouldRollbackTransactionWhenCheckFails");

        auto future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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

        future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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
        auto connection = MakeConnection("ShouldRollbackTransactionWhenCheckFails2");

        auto future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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

        future = connection->GetTableClient()->RetryOperation(
            [prefix = connection->GetTablePathPrefix()] (ISession::TPtr session) {
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
