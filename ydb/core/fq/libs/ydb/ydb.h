#pragma once

#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/env.h>

#include <ydb/core/fq/libs/ydb/table_client.h>

namespace NKikimrConfig {

class TStreamingQueriesConfig_TExternalStorageConfig;

} // namespace NKikimrConfig

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

class TExternalStorageSettings {
public:
    TExternalStorageSettings() = default;
    TExternalStorageSettings(const NConfig::TYdbStorageConfig& config);
    TExternalStorageSettings(const NKikimrConfig::TStreamingQueriesConfig_TExternalStorageConfig& config);

private:
    YDB_ACCESSOR_DEF(TString, Endpoint);
    YDB_ACCESSOR_DEF(TString, Database);
    YDB_ACCESSOR_DEF(TString, PathPrefix);
    YDB_ACCESSOR_DEF(TString, Token);
    YDB_ACCESSOR_DEF(TString, TokenFile);
    YDB_ACCESSOR_DEF(TString, SaKeyFile);
    YDB_ACCESSOR_DEF(TString, CaCertFile);
    YDB_ACCESSOR(bool, UseSsl, false);
    YDB_ACCESSOR(bool, UseLocalMetadataService, false);
    YDB_ACCESSOR_DEF(TString, IamEndpoint);
    YDB_ACCESSOR(ui64, MaxActiveQuerySessions, 50); // 50 - default in TSessionPoolSettings
    YDB_ACCESSOR_DEF(TDuration, ClientTimeout);
    YDB_ACCESSOR_DEF(TDuration, OperationTimeout);
    YDB_ACCESSOR_DEF(TDuration, CancelAfter);
};

struct TYdbConnection : public TThrRefBase {
    NYdb::TDriver Driver;
    NYdb::NTable::TTableClient TableClient;
    NYdb::NScheme::TSchemeClient SchemeClient;
    NYdb::NCoordination::TClient CoordinationClient;
    NYdb::NRateLimiter::TRateLimiterClient RateLimiterClient;
    const TString DB;
    const TString TablePathPrefix;

    TYdbConnection(
        const TExternalStorageSettings& config,
        const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
        const NYdb::TDriver& driver);
};

using TYdbConnectionPtr = TIntrusivePtr<TYdbConnection>;

struct IYdbConnection : public TThrRefBase {
    using TPtr = TIntrusivePtr<IYdbConnection>;

    virtual IYdbTableClient::TPtr GetTableClient() const = 0;
    virtual TString GetTablePathPrefix() const = 0;
    virtual TString GetDb() const = 0;
    virtual TString GetTablePathPrefixWithoutDb() const = 0;
};

IYdbConnection::TPtr CreateLocalYdbConnection(
    const TString& db,
    const TString& tablePathPrefix);

IYdbConnection::TPtr CreateSdkYdbConnection(
    const TExternalStorageSettings& config,
    const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
    const NYdb::TDriver& driver);


////////////////////////////////////////////////////////////////////////////////

struct TGenerationContext : public TThrRefBase {
    enum EOperationType {
        Register,
        RegisterCheck,
        Check,
    };

    // set internally, do not touch it
    EOperationType OperationType = Register;

    // within this session we execute transaction
    ISession::TPtr Session;

    // - In Register and RegisterCheck operation - whether
    // to commit or not after upserting new generation (usually true)
    // - In Check operation - whether to commit or not
    // after selecting generation (normally false)
    //
    // - When check succeeds, and flag is true, it's up to caller
    // to finish transaction.
    // - When check fails then transaction is aborted automatically
    const bool CommitTx;

    const TString TablePathPrefix;
    const TString Table;
    const TString PrimaryKeyColumn;
    const TString GenerationColumn;

    const TString PrimaryKey;

    // - In Register operation - it is new generation to be registered,
    // caller with higher generation wins
    // - In RegisterCheck operation - it is new generation to be registered,
    // if DB contains smaller generation, when generation is the same - operation
    // equals to Check
    // - In Check operation - expected generation, caller normally uses
    // it with Transaction (must have CommitTx = false)
    const ui64 Generation;

    // result of Select
    ui64 GenerationRead = 0;

    NYdb::NTable::TExecDataQuerySettings ExecDataQuerySettings;

    TGenerationContext(ISession::TPtr session,
                       bool commitTx,
                       const TString& tablePathPrefix,
                       const TString& table,
                       const TString& primaryKeyColumn,
                       const TString& generationColumn,
                       const TString& primaryKey,
                       ui64 generation,
                       const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings = {})
        : Session(std::move(session))
        , CommitTx(commitTx)
        , TablePathPrefix(tablePathPrefix)
        , Table(table)
        , PrimaryKeyColumn(primaryKeyColumn)
        , GenerationColumn(generationColumn)
        , PrimaryKey(primaryKey)
        , Generation(generation)
        , ExecDataQuerySettings(execDataQuerySettings)
    {
    }
};

using TGenerationContextPtr = TIntrusivePtr<TGenerationContext>;

////////////////////////////////////////////////////////////////////////////////

TYdbConnectionPtr NewYdbConnection(const TExternalStorageSettings& config, const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory, const NYdb::TDriver& driver);

NYdb::TStatus MakeErrorStatus(
    NYdb::EStatus code,
    const TString& msg,
    NYql::ESeverity severity = NYql::TSeverityIds::S_WARNING);

NYql::TIssues StatusToIssues(const NYdb::TStatus& status);

NThreading::TFuture<NYql::TIssues> StatusToIssues(
    const NThreading::TFuture<NYdb::TStatus>& future);

NThreading::TFuture<NYdb::TStatus> CreateTable(
    const TYdbConnectionPtr& ydbConnection,
    const TString& name,
    NYdb::NTable::TTableDescription&& description);

NThreading::TFuture<NYdb::TStatus> CreateTable(
    const IYdbConnection::TPtr& ydbConnection,
    const TString& name,
    NYdb::NTable::TTableDescription&& description);

bool IsTableCreated(const NYdb::TStatus& status);
bool IsTableDeleted(const NYdb::TStatus& status);

// Succeeds only if specified generation strictly greater than in DB
NThreading::TFuture<NYdb::TStatus> RegisterGeneration(const TGenerationContextPtr& context);

// Succeeds if specified generation is greater or equal than in DB
NThreading::TFuture<NYdb::TStatus> RegisterCheckGeneration(const TGenerationContextPtr& context);

// Checks that generation is equal to the one in DB
NThreading::TFuture<NYdb::TStatus> CheckGeneration(const TGenerationContextPtr& context);

NThreading::TFuture<NYdb::TStatus> RollbackTransaction(const TGenerationContextPtr& context);

NKikimr::TYdbCredentialsSettings GetYdbCredentialSettings(const TExternalStorageSettings& config);

template <class TSettings>
TSettings GetClientSettings(const TExternalStorageSettings& config, const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory) {
    TSettings settings;
    settings
        .DiscoveryEndpoint(config.GetEndpoint())
        .Database(config.GetDatabase());

    settings.CredentialsProviderFactory(credProviderFactory(GetYdbCredentialSettings(config)));

    if (config.GetUseLocalMetadataService() || config.GetUseSsl()) {
        settings.SslCredentials(NYdb::TSslCredentials(true));
    }

    if (config.GetCaCertFile()) {
        auto cert = StripString(TFileInput(config.GetCaCertFile()).ReadAll());
        settings.SslCredentials(NYdb::TSslCredentials(true, cert));
    }

    if constexpr (std::is_same_v<TSettings, NYdb::NTable::TClientSettings>) {
        settings.SessionPoolSettings(NYdb::NTable::TSessionPoolSettings()
            .MaxActiveSessions(config.GetMaxActiveQuerySessions()));
    }

    return settings;
}

} // namespace NFq
