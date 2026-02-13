#include "replication_utils.h"
#include "query_utils.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <format>

namespace NYdb::NDump {

namespace {

TString BuildConnectionString(const NReplication::TConnectionParams& params) {
    return TStringBuilder()
        << (params.GetEnableSsl() ? "grpcs://" : "grpc://")
        << params.GetDiscoveryEndpoint()
        << "/?database=" << params.GetDatabase();
}

inline TString BuildTarget(const char* src, const char* dst) {
    return TStringBuilder() << "  `" << src << "` AS `" << dst << "`";
}

inline TString Quote(const char* value) {
    return TStringBuilder() << "'" << value << "'";
}

template <typename StringType>
inline TString Quote(const StringType& value) {
    return Quote(value.c_str());
}

inline TString BuildOption(const char* key, const TString& value) {
    return TStringBuilder() << "  " << key << " = " << value << "";
}

inline TString Interval(const TDuration& value) {
    return TStringBuilder() << "Interval('PT" << value.Seconds() << "S')";
}

TString GetSecretSettingName(TStringBuf secretName, const TString& secretSettingPrefix) {
    return secretName.StartsWith('/') ? secretSettingPrefix + "_PATH" : secretSettingPrefix + "_NAME";
}

void AddSecretSettingIfNotEmpty(const std::string& secretName, const TString& secretSettingName, TVector<TString>& options) {
    if (!secretName.empty()) {
        options.push_back(BuildOption(GetSecretSettingName(secretName, secretSettingName).c_str(), Quote(secretName)));
    }
}

void AddConnectionOptions(const NReplication::TConnectionParams& connectionParams, TVector<TString>& options) {
    options.push_back(BuildOption("CONNECTION_STRING", Quote(BuildConnectionString(connectionParams))));
    switch (connectionParams.GetCredentials()) {
        case NReplication::TConnectionParams::ECredentials::Static:
            options.push_back(BuildOption("USER", Quote(connectionParams.GetStaticCredentials().User)));
            AddSecretSettingIfNotEmpty(connectionParams.GetStaticCredentials().PasswordSecretName,
                "PASSWORD_SECRET", options);
            break;
        case NReplication::TConnectionParams::ECredentials::OAuth:
            AddSecretSettingIfNotEmpty(connectionParams.GetOAuthCredentials().TokenSecretName,
                "TOKEN_SECRET", options);
            break;
    }
}

TString ExtractTransformationLambdaName(const TString& lambdaCreateQuery) {
    const TString lambdaNameStartPattern = TStringBuilder() << TRANSFER_LAMBDA_DEFAULT_NAME << " = ";
    const TString lambdaNameEndPattern = ";";

    size_t startPos = lambdaCreateQuery.find(lambdaNameStartPattern);
    if (startPos == TString::npos) {
        return "";
    }

    startPos += lambdaNameStartPattern.length();

    size_t endPos = lambdaCreateQuery.rfind(lambdaNameEndPattern);
    if (endPos == TString::npos) {
        return "";
    }

    if (startPos >= endPos) {
        return "";
    }

    return lambdaCreateQuery.substr(startPos, endPos - startPos);
}

void CleanQuery(TString& query, const TString& patternToRemove) {
    if (patternToRemove.empty()) {
        return;
    }

    size_t patternLength = patternToRemove.length();
    size_t position;
    while ((position = query.find(patternToRemove)) != TString::npos) {
        query.erase(position, patternLength);
    }
}

} // anonymous namespace

TString BuildCreateReplicationQuery(
    const TString& db,
    const TString& backupRoot,
    const TString& name,
    const NReplication::TReplicationDescription& desc)
{
    TVector<TString> targets(::Reserve(desc.GetItems().size()));
    for (const auto& item : desc.GetItems()) {
        if (!item.DstPath.ends_with("/indexImplTable")) { // TODO(ilnaz): get rid of this hack
            targets.push_back(BuildTarget(item.SrcPath.c_str(), item.DstPath.c_str()));
        }
    }

    TVector<TString> opts(::Reserve(5 /* max options */));

    const auto& params = desc.GetConnectionParams();
    AddConnectionOptions(params, opts);

    opts.push_back(BuildOption("CONSISTENCY_LEVEL", Quote(ToString(desc.GetConsistencyLevel()))));
    if (desc.GetConsistencyLevel() == NReplication::TReplicationDescription::EConsistencyLevel::Global) {
        opts.push_back(BuildOption("COMMIT_INTERVAL", Interval(desc.GetGlobalConsistency().GetCommitInterval())));
    }

    return std::format(
        "-- database: \"{}\"\n"
        "-- backup root: \"{}\"\n"
        "CREATE ASYNC REPLICATION `{}`\n"
        "FOR\n"
        "{}\n"
        "WITH (\n"
        "{}\n"
        ");",
        db.c_str(),
        backupRoot.c_str(),
        name.c_str(),
        JoinSeq(",\n", targets).c_str(),
        JoinSeq(",\n", opts).c_str()
    );
}

TString BuildCreateTransferQuery(
    const TString& db,
    const TString& backupRoot,
    const TString& name,
    const NReplication::TTransferDescription& desc)
{
    TVector<TString> options(::Reserve(7));

    const auto& connectionParams = desc.GetConnectionParams();
    AddConnectionOptions(connectionParams, options);
    if (!desc.GetConsumerName().empty()) {
        options.push_back(BuildOption("CONSUMER", Quote(desc.GetConsumerName())));
    }

    const auto& batchingSettings = desc.GetBatchingSettings();
    options.push_back(BuildOption("BATCH_SIZE_BYTES", ToString(batchingSettings.SizeBytes)));
    options.push_back(BuildOption("FLUSH_INTERVAL", Interval(batchingSettings.FlushInterval)));

    TString lambdaCreateQuery = desc.GetTransformationLambda().c_str();
    TString lambdaName = ExtractTransformationLambdaName(lambdaCreateQuery);

    TString cleanedLambdaCreateQuery = lambdaCreateQuery;
    CleanQuery(cleanedLambdaCreateQuery, "PRAGMA OrderedColumns;");
    CleanQuery(cleanedLambdaCreateQuery, TStringBuilder() << TRANSFER_LAMBDA_DEFAULT_NAME << " = " << lambdaName << ";");

    return std::format(
        "-- database: \"{}\"\n"
        "-- backup root: \"{}\"\n"
        "{}\n\n"
        "CREATE TRANSFER `{}`\n"
        "FROM `{}` TO `{}` USING {}\n"
        "WITH (\n"
        "{}\n"
        ");",
        db.c_str(),
        backupRoot.c_str(),
        cleanedLambdaCreateQuery.c_str(),
        name.c_str(),
        desc.GetSrcPath().c_str(), desc.GetDstPath().c_str(), lambdaName.c_str(),
        JoinSeq(",\n", options).c_str()
    );
}

bool RewriteCreateAsyncReplicationQueryNoSecrets(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues)
{
    if (!RewriteObjectRefs(query, dbRestoreRoot, issues)) {
        return false;
    }
    return RewriteCreateQuery(query, "CREATE ASYNC REPLICATION `{}`", dbPath, issues);
}

bool RewriteCreateTransferQueryNoSecrets(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues)
{
    if (!RewriteObjectRefs(query, dbRestoreRoot, issues)) {
        return false;
    }
    return RewriteCreateQuery(query, "CREATE TRANSFER `{}`", dbPath, issues);
}

bool RewriteCreateAsyncReplicationQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues)
{
    if (!RewriteQuerySecretsNoCheck(query, dbRestoreRoot, issues)) {
        return false;
    }
    return RewriteCreateAsyncReplicationQueryNoSecrets(query, dbRestoreRoot, dbPath, issues);
}

bool RewriteCreateTransferQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues)
{
    if (!RewriteQuerySecretsNoCheck(query, dbRestoreRoot, issues)) {
        return false;
    }
    return RewriteCreateTransferQueryNoSecrets(query, dbRestoreRoot, dbPath, issues);
}

} // namespace NYdb::NDump
