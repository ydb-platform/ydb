#pragma once

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/core/fq/libs/config/protos/storage.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_coordination/coordination.h>
#include <ydb/public/sdk/cpp/client/ydb_rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/env.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

struct TYdbConnection : public TThrRefBase {
    NYdb::TDriver Driver;
    NYdb::NTable::TTableClient TableClient;
    NYdb::NScheme::TSchemeClient SchemeClient;
    NYdb::NCoordination::TClient CoordinationClient;
    NYdb::NRateLimiter::TRateLimiterClient RateLimiterClient;
    const TString DB;
    const TString TablePathPrefix;

    TYdbConnection(
        const NConfig::TYdbStorageConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
        const NYdb::TDriver& driver);
};

using TYdbConnectionPtr = TIntrusivePtr<TYdbConnection>;

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
    NYdb::NTable::TSession Session;

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

    TMaybe<NYdb::NTable::TTransaction> Transaction;

    // result of Select
    ui64 GenerationRead = 0;

    TGenerationContext(NYdb::NTable::TSession session,
                       bool commitTx,
                       const TString& tablePathPrefix,
                       const TString& table,
                       const TString& primaryKeyColumn,
                       const TString& generationColumn,
                       const TString& primaryKey,
                       ui64 generation)
        : Session(session)
        , CommitTx(commitTx)
        , TablePathPrefix(tablePathPrefix)
        , Table(table)
        , PrimaryKeyColumn(primaryKeyColumn)
        , GenerationColumn(generationColumn)
        , PrimaryKey(primaryKey)
        , Generation(generation)
    {
    }
};

using TGenerationContextPtr = TIntrusivePtr<TGenerationContext>;

////////////////////////////////////////////////////////////////////////////////

TYdbConnectionPtr NewYdbConnection(const NConfig::TYdbStorageConfig& config, const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory, const NYdb::TDriver& driver);

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

bool IsTableCreated(const NYdb::TStatus& status);
bool IsTableDeleted(const NYdb::TStatus& status);

// Succeeds only if specified generation strictly greater than in DB
NThreading::TFuture<NYdb::TStatus> RegisterGeneration(const TGenerationContextPtr& context);

// Succeeds if specified generation is greater or equal than in DB
NThreading::TFuture<NYdb::TStatus> RegisterCheckGeneration(const TGenerationContextPtr& context);

// Checks that generation is equal to the one in DB
NThreading::TFuture<NYdb::TStatus> CheckGeneration(const TGenerationContextPtr& context);

NThreading::TFuture<NYdb::TStatus> RollbackTransaction(const TGenerationContextPtr& context);

NKikimr::TYdbCredentialsSettings GetYdbCredentialSettings(const NConfig::TYdbStorageConfig& config);

template <class TSettings>
TSettings GetClientSettings(const NConfig::TYdbStorageConfig& config,
                            const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory) {
    TSettings settings;
    settings
        .DiscoveryEndpoint(config.GetEndpoint())
        .Database(config.GetDatabase());

    settings.CredentialsProviderFactory(credProviderFactory(GetYdbCredentialSettings(config)));

    if (config.GetUseLocalMetadataService()) {
        settings.SslCredentials(NYdb::TSslCredentials(true));
    }

    if (config.GetCertificateFile()) {
        auto cert = StripString(TFileInput(config.GetCertificateFile()).ReadAll());
        settings.SslCredentials(NYdb::TSslCredentials(true, cert));
    }

    return settings;
}

} // namespace NFq
