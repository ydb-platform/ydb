#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>

namespace NFq {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

namespace {

////////////////////////////////////////////////////////////////////////////////

TYdbConnectionPtr MakeConnection(const char* tablePrefix) {
    NConfig::TYdbStorageConfig config;

    config.SetEndpoint(GetEnv("YDB_ENDPOINT"));
    config.SetDatabase(GetEnv("YDB_DATABASE"));
    config.SetToken("");
    config.SetTablePrefix(tablePrefix);

    NYdb::TDriver driver({});
    auto connection = NewYdbConnection(config, NKikimr::CreateYdbCredentialsProviderFactory, driver);

    auto status = connection->SchemeClient.MakeDirectory(connection->TablePathPrefix).GetValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

    auto desc = TTableBuilder()
        .AddNullableColumn("id", EPrimitiveType::String)
        .AddNullableColumn("generation", EPrimitiveType::Uint64)
        .SetPrimaryKeyColumn("id")
        .Build();

    status = CreateTable(connection, "test", std::move(desc)).GetValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

    return connection;
}

TFuture<TStatus> CheckTransactionClosed(const TFuture<TStatus>& future, const TGenerationContextPtr& context) {
    return future.Apply(
        [context] (const TFuture<TStatus>& future) {
            if (context->Transaction && context->Transaction->IsActive()) {
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

            if (!context->Transaction || !context->Transaction->IsActive()) {
                auto status = MakeErrorStatus(EStatus::INTERNAL_ERROR, "no transaction");
                return MakeFuture(status);
            }

            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");

                UPSERT INTO dummy (id) VALUES
                    ("ID DQD");
            )", context->TablePathPrefix.c_str());

            auto ttxControl = TTxControl::Tx(*context->Transaction).CommitTx();
            return context->Session.ExecuteDataQuery(query, ttxControl).Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
                });
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRegisterCheckTest) {
    Y_UNIT_TEST(ShouldRegisterCheckNewGeneration)
    {
        auto connection = MakeConnection("ShouldRegisterCheckNewGeneration");

        auto future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

    Y_UNIT_TEST(ShouldRegisterCheckSameGeneration)
    {
        auto connection = MakeConnection("ShouldRegisterCheckSameGeneration");

        auto future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

        future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

    Y_UNIT_TEST(ShouldRegisterCheckNextGeneration)
    {
        auto connection = MakeConnection("ShouldRegisterCheckNextGeneration");

        auto future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

        future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

    Y_UNIT_TEST(ShouldNotRegisterCheckPrevGeneration)
    {
        auto connection = MakeConnection("ShouldNotRegisterCheckPrevGeneration");

        auto future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

        future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

    Y_UNIT_TEST(ShouldNotRegisterCheckPrevGeneration2)
    {
        auto connection = MakeConnection("ShouldNotRegisterCheckPrevGeneration2");

        auto future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

        future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

    Y_UNIT_TEST(ShouldRegisterCheckNewGenerationAndTransact)
    {
        auto connection = MakeConnection("ShouldRegisterCheckNewGenerationAndTransact");

        auto desc = TTableBuilder()
            .AddNullableColumn("id", EPrimitiveType::String)
            .SetPrimaryKeyColumn("id")
            .Build();

        auto status = CreateTable(connection, "dummy", std::move(desc)).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

    Y_UNIT_TEST(ShouldRegisterCheckSameGenerationAndTransact)
    {
        auto connection = MakeConnection("ShouldRegisterCheckNewGenerationAndTransact");

        auto desc = TTableBuilder()
            .AddNullableColumn("id", EPrimitiveType::String)
            .SetPrimaryKeyColumn("id")
            .Build();

        auto status = CreateTable(connection, "dummy", std::move(desc)).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

        future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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
}

////////////////////////////////////////////////////////////////////////////////

// most of logic is tested inside libs/storage, thus we don't test here again
Y_UNIT_TEST_SUITE(TCheckGenerationTest) {
    Y_UNIT_TEST(ShouldRollbackTransactionWhenCheckFails)
    {
        auto connection = MakeConnection("ShouldRollbackTransactionWhenCheckFails");

        auto future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

        future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

    Y_UNIT_TEST(ShouldRollbackTransactionWhenCheckFails2)
    {
        auto connection = MakeConnection("ShouldRollbackTransactionWhenCheckFails");

        auto future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
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

        future = connection->TableClient.RetryOperation(
            [prefix = connection->TablePathPrefix] (TSession session) {
                auto context = MakeIntrusive<TGenerationContext>(
                    session,
                    false, // the only difference with ShouldRollbackTransactionWhenCheckFails
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
}

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
}

} // namespace NFq
