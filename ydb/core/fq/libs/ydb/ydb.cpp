#include "ydb.h"

#include "util.h"

#include <util/stream/str.h>
#include <util/string/printf.h>

namespace NFq {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

using NYql::TIssues;

namespace {

////////////////////////////////////////////////////////////////////////////////

TFuture<TDataQueryResult> SelectGeneration(const TGenerationContextPtr& context) {
    // TODO: use prepared queries

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        SELECT %s, %s
        FROM %s
        WHERE %s = "%s";
    )", context->TablePathPrefix.c_str(),
        context->PrimaryKeyColumn.c_str(),
        context->GenerationColumn.c_str(),
        context->Table.c_str(),
        context->PrimaryKeyColumn.c_str(),
        context->PrimaryKey.c_str());

    auto ttxControl = TTxControl::BeginTx(TTxSettings::SerializableRW());
    if (context->OperationType == TGenerationContext::Check && context->CommitTx) {
        ttxControl.CommitTx();
    }

    return context->Session.ExecuteDataQuery(query, ttxControl);
}

TFuture<TStatus> CheckGeneration(
    const TDataQueryResult& selectResult,
    const TGenerationContextPtr& context)
{
    if (!selectResult.IsSuccess()) {
        return MakeFuture<TStatus>(selectResult);
    }


    TResultSetParser parser(selectResult.GetResultSet(0));
    if (parser.TryNextRow()) {
        context->GenerationRead = parser.ColumnParser(context->GenerationColumn).GetOptionalUint64().GetOrElse(0);
    }

    bool isOk = false;
    bool requiresTransaction = false;
    switch (context->OperationType) {
    case TGenerationContext::Register:
    {
        isOk = context->GenerationRead < context->Generation;
        requiresTransaction = true;
        break;
    }
    case TGenerationContext::RegisterCheck:
    {
        isOk = context->GenerationRead <= context->Generation;
        requiresTransaction = true;
        break;
    }
    case TGenerationContext::Check:
    {
        isOk = context->GenerationRead == context->Generation;
        if (!context->CommitTx) {
            requiresTransaction = true;
        }
        break;
    }
    }

    context->Transaction = selectResult.GetTransaction();

    if (!isOk) {
        RollbackTransaction(context); // don't care about result

        TStringStream ss;
        ss << "Table: " << JoinPath(context->TablePathPrefix, context->Table)
           << ", pk: " << context->PrimaryKey
           << ", current generation: " << context->GenerationRead
           << ", expected/new generation: " << context->Generation
           << ", operation: " << context->OperationType;

        return MakeFuture(MakeErrorStatus(EStatus::ALREADY_EXISTS, ss.Str()));
    }

    if (requiresTransaction && !context->Transaction) {
        // just sanity check, normally should not happen.
        // note that we use retriable error
        TStringStream ss;
        ss << "Table: " << JoinPath(context->TablePathPrefix, context->Table)
           << ", pk: " << context->PrimaryKey
           << ", current generation: " << context->GenerationRead
           << ", expected/new generation: " << context->Generation
           << ", failed to get transaction after select";

        return MakeFuture(MakeErrorStatus(EStatus::ABORTED, ss.Str(), NYql::TSeverityIds::S_WARNING));
    }

    return MakeFuture<TStatus>(selectResult);
}

TFuture<TStatus> SelectGenerationWithCheck(const TGenerationContextPtr& context) {
    auto future = SelectGeneration(context);
    return future.Apply(
        [context] (const TFuture<TDataQueryResult>& future) {
            return CheckGeneration(future.GetValue(), context);
        });
}

TFuture<TStatus> UpsertGeneration(const TGenerationContextPtr& context) {
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        UPSERT INTO %s (%s, %s) VALUES
            ("%s", %lu);
    )", context->TablePathPrefix.c_str(),
        context->Table.c_str(),
        context->PrimaryKeyColumn.c_str(),
        context->GenerationColumn.c_str(),
        context->PrimaryKey.c_str(),
        context->Generation);

    auto ttxControl = TTxControl::Tx(*context->Transaction);
    if (context->CommitTx) {
        ttxControl.CommitTx();
        context->Transaction.Clear();
    }

    return context->Session.ExecuteDataQuery(query, ttxControl).Apply(
        [] (const TFuture<TDataQueryResult>& future) {
            TStatus status = future.GetValue();
            return status;
        });
}

TFuture<TStatus> RegisterGenerationWrapper(
    const TFuture<TStatus>& selectGenerationFuture,
    const TGenerationContextPtr& context)
{
    return selectGenerationFuture.Apply(
        [context] (const TFuture<TStatus>& selectGenerationFuture) {
            const auto& selectResult = selectGenerationFuture.GetValue();
            if (!selectResult.IsSuccess()) {
                return MakeFuture(selectResult);
            }
            return UpsertGeneration(context);
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYdbConnection::TYdbConnection(const NConfig::TYdbStorageConfig& config,
                               const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
                               const NYdb::TDriver& driver)
    : Driver(driver)
    , TableClient(Driver, GetClientSettings<NYdb::NTable::TClientSettings>(config, credProviderFactory))
    , SchemeClient(Driver, GetClientSettings<NYdb::TCommonClientSettings>(config, credProviderFactory))
    , CoordinationClient(Driver, GetClientSettings<NYdb::TCommonClientSettings>(config, credProviderFactory))
    , RateLimiterClient(Driver, GetClientSettings<NYdb::TCommonClientSettings>(config, credProviderFactory))
    , DB(config.GetDatabase())
    , TablePathPrefix(JoinPath(DB, config.GetTablePrefix()))
{
}

////////////////////////////////////////////////////////////////////////////////

TYdbConnectionPtr NewYdbConnection(const NConfig::TYdbStorageConfig& config,
                                   const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
                                   const NYdb::TDriver& driver) {
    return MakeIntrusive<TYdbConnection>(config, credProviderFactory, driver);
}

TStatus MakeErrorStatus(
    EStatus code,
    const TString& msg,
    NYql::ESeverity severity)
{
    NYql::TIssues issues;
    issues.AddIssue(msg);
    auto& issue = issues.back();
    issue.SetCode((ui32)code, severity);

    return TStatus(code, std::move(issues));
}

NYql::TIssues StatusToIssues(const NYdb::TStatus& status) {
    TIssues issues;
    if (!status.IsSuccess()) {
        issues = status.GetIssues();
    }
    return issues;
}

TFuture<TIssues> StatusToIssues(const TFuture<TStatus>& future) {
    return future.Apply(
        [] (const TFuture<TStatus>& future) {
            try {
                return StatusToIssues(future.GetValue());
            } catch (...) {
                TIssues issues;
                issues.AddIssue("StatusToIssues failed with exception: " + CurrentExceptionMessage());
                return issues;
            }
    });
}

TFuture<TStatus> CreateTable(
    const TYdbConnectionPtr& ydbConnection,
    const TString& name,
    TTableDescription&& description)
{
    auto tablePath = JoinPath(ydbConnection->TablePathPrefix, name.c_str());

    return ydbConnection->TableClient.RetryOperation(
        [tablePath = std::move(tablePath), description = std::move(description)] (TSession session) mutable {
            return session.CreateTable(tablePath, TTableDescription(description));
        });
}

bool IsTableCreated(const NYdb::TStatus& status) {
    return status.IsSuccess() ||
        status.GetStatus() == NYdb::EStatus::ALREADY_EXISTS ||
            status.GetStatus() == NYdb::EStatus::CLIENT_CANCELLED;
}

bool IsTableDeleted(const NYdb::TStatus& status) {
    return status.IsSuccess() ||
        status.GetStatus() == NYdb::EStatus::NOT_FOUND ||
            status.GetStatus() == NYdb::EStatus::CLIENT_CANCELLED;
}

TFuture<TStatus> RegisterGeneration(const TGenerationContextPtr& context) {
    context->OperationType = TGenerationContext::Register;
    auto future = SelectGenerationWithCheck(context);
    return RegisterGenerationWrapper(future, context);
}

TFuture<TStatus> RegisterCheckGeneration(const TGenerationContextPtr& context) {
    context->OperationType = TGenerationContext::RegisterCheck;
    auto future = SelectGenerationWithCheck(context);

    return future.Apply(
        [context] (const TFuture<TStatus>& future) {
            if (future.HasException()) {
                return future;
            }
            const auto& status = future.GetValue();
            if (!status.IsSuccess()) {
                return future;
            }

            // check successful, which means that either:
            // - generation in DB is same as in context
            // - generation in DB is lower than in context
            if (context->Generation > context->GenerationRead) {
                // Check was OK, but we still have to register our generation
                return RegisterGenerationWrapper(future, context);
            } else {
                // Check was OK, but we need to care about transaction:
                // we will not upsert, if user requested to finish transaction
                // we need to finish it (rare case, nobody probably needs check
                // without transaction)
                if (context->CommitTx) {
                    // we don't check result of rollback, because don't care
                    RollbackTransaction(context);
                    return future;
                }
                return future;
            }
        });
}

TFuture<TStatus> CheckGeneration(const TGenerationContextPtr& context) {
    context->OperationType = TGenerationContext::Check;
    return SelectGenerationWithCheck(context);
}

TFuture<TStatus> RollbackTransaction(const TGenerationContextPtr& context) {
    if (!context->Transaction || !context->Transaction->IsActive()) {
        auto status = MakeErrorStatus(EStatus::INTERNAL_ERROR, "trying to rollback non-active transaction");
        return MakeFuture(status);
    }

    auto future = context->Transaction->Rollback();
    context->Transaction.Clear();
    return future;
}

NKikimr::TYdbCredentialsSettings GetYdbCredentialSettings(const NConfig::TYdbStorageConfig& config) {
    TString oauth;
    if (config.GetToken()) {
        oauth = config.GetToken();
    } else if (config.GetOAuthFile()) {
        oauth = StripString(TFileInput(config.GetOAuthFile()).ReadAll());
    } else {
        oauth = GetEnv("YDB_TOKEN");
    }

    NKikimr::TYdbCredentialsSettings credSettings;
    credSettings.UseLocalMetadata = config.GetUseLocalMetadataService();
    credSettings.OAuthToken = oauth;
    credSettings.SaKeyFile = config.GetSaKeyFile();
    credSettings.IamEndpoint = config.GetIamEndpoint();
    return credSettings;
}

} // namespace NFq
