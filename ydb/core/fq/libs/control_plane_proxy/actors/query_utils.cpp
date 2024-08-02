#include "query_utils.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <util/generic/maybe.h>

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/lib/fq/scope.h>

namespace NFq {
namespace NPrivate {

TString MakeCreateExternalDataTableQuery(const FederatedQuery::BindingContent& content,
                                         const TString& connectionName,
                                         bool replaceIfExists) {
    using namespace fmt::literals;

    auto bindingName         = content.name();
    auto objectStorageParams = content.setting().object_storage();
    const auto& subset       = objectStorageParams.subset(0);

    // Schema
    NYql::TExprContext context;
    auto columnsTransformFunction = [&context](const Ydb::Column& column) -> TString {
        NYdb::TTypeParser typeParser(column.type());
        auto node     = MakeType(typeParser, context);
        auto typeName = NYql::FormatType(node);
        const TString notNull =
            (node->GetKind() == NYql::ETypeAnnotationKind::Optional) ? "" : "NOT NULL";
        return fmt::format("    {columnName} {columnType} {notNull}",
                           "columnName"_a = EncloseAndEscapeString(column.name(), '`'),
                           "columnType"_a = typeName,
                           "notNull"_a    = notNull);
    };

    // WithOptions
    auto withOptions = std::unordered_map<TString, TString>{};
    withOptions.insert({"DATA_SOURCE", TStringBuilder{} << '"' << connectionName << '"'});
    withOptions.insert({"LOCATION", EncloseAndEscapeString(subset.path_pattern(), '"')});
    if (!subset.format().Empty()) {
        withOptions.insert({"FORMAT", EncloseAndEscapeString(subset.format(), '"')});
    }
    if (!subset.compression().Empty()) {
        withOptions.insert(
            {"COMPRESSION", EncloseAndEscapeString(subset.compression(), '"')});
    }
    for (auto& kv : subset.format_setting()) {
        withOptions.insert({EncloseAndEscapeString(kv.first, '`'),
                            EncloseAndEscapeString(kv.second, '"')});
    }

    if (!subset.partitioned_by().empty()) {
        auto partitionBy = TStringBuilder{}
                           << "\"["
                           << JoinMapRange(", ",
                                           subset.partitioned_by().begin(),
                                           subset.partitioned_by().end(),
                                           [](const TString& value) {
                                               return EscapeString(value, '"');
                                           })
                           << "]\"";
        withOptions.insert({"PARTITIONED_BY", partitionBy});
    }

    for (auto& kv : subset.projection()) {
        withOptions.insert({EncloseAndEscapeString(kv.first, '`'),
                            EncloseAndEscapeString(kv.second, '"')});
    }

    return fmt::format(
        R"(
                CREATE {replaceIfSupported} EXTERNAL TABLE {externalTableName} (
                    {columns}
                ) WITH (
                    {withOptions}
                );)",
        "replaceIfSupported"_a = replaceIfExists ? "OR REPLACE" : "",
        "externalTableName"_a = EncloseAndEscapeString(bindingName, '`'),
        "columns"_a = JoinMapRange(",\n", subset.schema().column().begin(),
                                   subset.schema().column().end(),
                                   columnsTransformFunction),
        "withOptions"_a =
            JoinMapRange(",\n", withOptions.begin(), withOptions.end(),
                         [](const std::pair<TString, TString> &kv) -> TString {
                           return TStringBuilder{} << "   " << kv.first << " = "
                                                   << kv.second;
                         }));
}

TString SignAccountId(const TString& id, const TSigner::TPtr& signer) {
    return signer ? signer->SignAccountId(id) : TString{};
}

TMaybe<TString> CreateSecretObjectQuery(const FederatedQuery::ConnectionSetting& setting,
                                const TString& name,
                                const TSigner::TPtr& signer,
                                const TString& scope) {
    const TString folderId = NYdb::NFq::TScope{scope}.ParseFolder();
    using namespace fmt::literals;
    TString secretObjects;
    auto serviceAccountId = ExtractServiceAccountId(setting);
    if (serviceAccountId) {
        secretObjects = signer ? fmt::format(
            R"(
                    UPSERT OBJECT {sa_secret_name} (TYPE SECRET) WITH value={signature};
                )",
            "sa_secret_name"_a = EncloseAndEscapeString(TStringBuilder{} << "f1_" << folderId << name, '`'),
            "signature"_a       = EncloseSecret(EncloseAndEscapeString(SignAccountId(serviceAccountId, signer), '"'))) : std::string{};
    }

    auto password = GetPassword(setting);
    if (password) {
        secretObjects += fmt::format(
                R"(
                    UPSERT OBJECT {password_secret_name} (TYPE SECRET) WITH value={password};
                )",
            "password_secret_name"_a = EncloseAndEscapeString(TStringBuilder{} << "f2_" << folderId << name, '`'),
            "password"_a = EncloseSecret(EncloseAndEscapeString(*password, '"')));
    }

    return secretObjects ? secretObjects : TMaybe<TString>{};
}

TString CreateAuthParamsQuery(const FederatedQuery::ConnectionSetting& setting,
                              const TString& name,
                              const TSigner::TPtr& signer) {
    using namespace fmt::literals;
    auto authMethod = GetYdbComputeAuthMethod(setting);
    switch (authMethod) {
        case EYdbComputeAuth::UNKNOWN:
            return {};
        case EYdbComputeAuth::NONE:
            return fmt::format(R"(, AUTH_METHOD="{auth_method}")", "auth_method"_a = ToString(authMethod));
        case EYdbComputeAuth::SERVICE_ACCOUNT:
            return fmt::format(
                R"(,
                    AUTH_METHOD="{auth_method}",
                    SERVICE_ACCOUNT_ID={service_account_id},
                    SERVICE_ACCOUNT_SECRET_NAME={sa_secret_name}
                )",
            "auth_method"_a = ToString(authMethod),
            "service_account_id"_a = EncloseAndEscapeString(ExtractServiceAccountId(setting), '"'),
            "sa_secret_name"_a = EncloseAndEscapeString(signer ? "k1" + name : TString{}, '"'));
        case EYdbComputeAuth::BASIC:
            return fmt::format(
                    R"(,
                        AUTH_METHOD="{auth_method}",
                        LOGIN={login},
                        PASSWORD_SECRET_NAME={password_secret_name}
                    )",
                "auth_method"_a = ToString(authMethod),
                "login"_a = EncloseAndEscapeString(GetLogin(setting).GetOrElse({}), '"'),
                "password_secret_name"_a = EncloseAndEscapeString("k2" + name, '"'));
        case EYdbComputeAuth::MDB_BASIC:
            return fmt::format(
                R"(,
                        AUTH_METHOD="{auth_method}",
                        SERVICE_ACCOUNT_ID={service_account_id},
                        SERVICE_ACCOUNT_SECRET_NAME={sa_secret_name},
                        LOGIN={login},
                        PASSWORD_SECRET_NAME={password_secret_name}
                    )",
                "auth_method"_a = ToString(authMethod),
                "service_account_id"_a = EncloseAndEscapeString(ExtractServiceAccountId(setting), '"'),
                "sa_secret_name"_a = EncloseAndEscapeString(signer ? "k1" + name : TString{}, '"'),
                "login"_a = EncloseAndEscapeString(GetLogin(setting).GetOrElse({}), '"'),
                "password_secret_name"_a = EncloseAndEscapeString("k2" + name, '"'));
    }
}

TString MakeCreateExternalDataSourceQuery(
    const FederatedQuery::ConnectionContent& connectionContent,
    const TSigner::TPtr& signer,
    const NConfig::TCommonConfig& common,
    bool replaceIfExists) {
    using namespace fmt::literals;

    TString properties;
    switch (connectionContent.setting().connection_case()) {
        case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
        case FederatedQuery::ConnectionSetting::kYdbDatabase:
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="Ydb",
                    DATABASE_ID={database_id},
                    USE_TLS="{use_tls}"
                )",
                "database_id"_a = EncloseAndEscapeString(connectionContent.setting().ydb_database().database_id(), '"'),
                "use_tls"_a = common.GetDisableSslForGenericDataSources() ? "false" : "true");
        break;
        case FederatedQuery::ConnectionSetting::kClickhouseCluster:
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="ClickHouse",
                    MDB_CLUSTER_ID={mdb_cluster_id},
                    DATABASE_NAME={database_name},
                    PROTOCOL="{protocol}",
                    USE_TLS="{use_tls}"
                )",
                "mdb_cluster_id"_a = EncloseAndEscapeString(connectionContent.setting().clickhouse_cluster().database_id(), '"'),
                "database_name"_a = EncloseAndEscapeString(connectionContent.setting().clickhouse_cluster().database_name(), '"'),
                "protocol"_a = common.GetUseNativeProtocolForClickHouse() ? "NATIVE" : "HTTP",
                "use_tls"_a = common.GetDisableSslForGenericDataSources() ? "false" : "true");
        break;
        case FederatedQuery::ConnectionSetting::kDataStreams:
        break;
        case FederatedQuery::ConnectionSetting::kObjectStorage: {
            auto bucketName = connectionContent.setting().object_storage().bucket();
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}"
                )",
                "location"_a = common.GetObjectStorageEndpoint() + "/" + EscapeString(bucketName, '"') + "/");
            break;
        }
        case FederatedQuery::ConnectionSetting::kMonitoring:
        break;
        case FederatedQuery::ConnectionSetting::kPostgresqlCluster: {
            const auto pgschema = connectionContent.setting().postgresql_cluster().schema();
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="PostgreSQL",
                    MDB_CLUSTER_ID={mdb_cluster_id},
                    DATABASE_NAME={database_name},
                    PROTOCOL="NATIVE",
                    USE_TLS="{use_tls}"
                    {schema}
                )",
                "mdb_cluster_id"_a = EncloseAndEscapeString(connectionContent.setting().postgresql_cluster().database_id(), '"'),
                "database_name"_a = EncloseAndEscapeString(connectionContent.setting().postgresql_cluster().database_name(), '"'),
                "use_tls"_a = common.GetDisableSslForGenericDataSources() ? "false" : "true",
                "schema"_a =  pgschema ? ", SCHEMA=" + EncloseAndEscapeString(pgschema, '"') : TString{});
        }
        break;
        case FederatedQuery::ConnectionSetting::kGreenplumCluster: {
            const auto gpschema = connectionContent.setting().greenplum_cluster().schema();
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="Greenplum",
                    MDB_CLUSTER_ID={mdb_cluster_id},
                    DATABASE_NAME={database_name},
                    USE_TLS="{use_tls}"
                    {schema}
                )",
                "mdb_cluster_id"_a = EncloseAndEscapeString(connectionContent.setting().greenplum_cluster().database_id(), '"'),
                "database_name"_a = EncloseAndEscapeString(connectionContent.setting().greenplum_cluster().database_name(), '"'),
                "use_tls"_a = common.GetDisableSslForGenericDataSources() ? "false" : "true",
                "schema"_a =  gpschema ? ", SCHEMA=" + EncloseAndEscapeString(gpschema, '"') : TString{});

        }
        case FederatedQuery::ConnectionSetting::kMysqlCluster: {
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="MySQL",
                    MDB_CLUSTER_ID={mdb_cluster_id},
                    DATABASE_NAME={database_name},
                    USE_TLS="{use_tls}"
                )",
                "mdb_cluster_id"_a = EncloseAndEscapeString(connectionContent.setting().mysql_cluster().database_id(), '"'),
                "database_name"_a = EncloseAndEscapeString(connectionContent.setting().mysql_cluster().database_name(), '"'),
                "use_tls"_a = common.GetDisableSslForGenericDataSources() ? "false" : "true");

        }
        break;
    }

    auto sourceName = connectionContent.name();
    return fmt::format(
        R"(
                CREATE {replaceIfSupported} EXTERNAL DATA SOURCE {external_source} WITH (
                    {properties}
                    {auth_params}
                );
            )",
        "replaceIfSupported"_a = replaceIfExists ? "OR REPLACE" : "",
        "external_source"_a = EncloseAndEscapeString(sourceName, '`'),
        "properties"_a = properties,
        "auth_params"_a =
            CreateAuthParamsQuery(connectionContent.setting(),
                                  connectionContent.name(),
                                  signer));
}

TMaybe<TString> DropSecretObjectQuery(const TString& name, const TString& scope) {
    const TString folderId = NYdb::NFq::TScope{scope}.ParseFolder();
    using namespace fmt::literals;
    return fmt::format(
            R"(
                DROP OBJECT {secret_name1} (TYPE SECRET);
                DROP OBJECT {secret_name2} (TYPE SECRET);
                DROP OBJECT {secret_name3} (TYPE SECRET); -- for backward compatibility
                DROP OBJECT {secret_name4} (TYPE SECRET); -- for backward compatibility
                DROP OBJECT {secret_name5} (TYPE SECRET); -- for backward compatibility
            )",
        "secret_name1"_a = EncloseAndEscapeString(TStringBuilder{} << "f1_" << folderId << name, '`'),
        "secret_name2"_a = EncloseAndEscapeString(TStringBuilder{} << "f2_" << folderId << name, '`'),
        "secret_name3"_a = EncloseAndEscapeString(TStringBuilder{} << "k1" << name, '`'),
        "secret_name4"_a = EncloseAndEscapeString(TStringBuilder{} << "k2" << name, '`'),
        "secret_name5"_a = EncloseAndEscapeString(name, '`'));
}

TString MakeDeleteExternalDataTableQuery(const TString& tableName) {
    using namespace fmt::literals;
    return fmt::format("DROP EXTERNAL TABLE {external_table};",
                       "external_table"_a = EncloseAndEscapeString(tableName, '`'));
}

TString MakeDeleteExternalDataSourceQuery(const TString& sourceName) {
    using namespace fmt::literals;
    return fmt::format("DROP EXTERNAL DATA SOURCE {external_source};",
                       "external_source"_a = EncloseAndEscapeString(sourceName, '`'));
}

} // namespace NPrivate
} // namespace NFq
