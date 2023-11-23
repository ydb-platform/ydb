#include "query_utils.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <util/generic/maybe.h>

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

namespace NFq {
namespace NPrivate {

TString MakeCreateExternalDataTableQuery(const FederatedQuery::BindingContent& content,
                                         const TString& connectionName) {
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
                CREATE EXTERNAL TABLE {externalTableName} (
                    {columns}
                ) WITH (
                    {withOptions}
                );)",
        "externalTableName"_a = EncloseAndEscapeString(bindingName, '`'),
        "columns"_a           = JoinMapRange(",\n",
                                   subset.schema().column().begin(),
                                   subset.schema().column().end(),
                                   columnsTransformFunction),
        "withOptions"_a       = JoinMapRange(",\n",
                                       withOptions.begin(),
                                       withOptions.end(),
                                       [](const std::pair<TString, TString>& kv) -> TString {
                                           return TStringBuilder{} << "   " << kv.first
                                                                   << " = " << kv.second;
                                       }));
}

TString SignAccountId(const TString& id, const TSigner::TPtr& signer) {
    return signer ? signer->SignAccountId(id) : TString{};
}

TMaybe<TString> CreateSecretObjectQuery(const FederatedQuery::ConnectionSetting& setting,
                                const TString& name,
                                const TSigner::TPtr& signer) {
    using namespace fmt::literals;
    TString secretObjects;
    auto serviceAccountId = ExtractServiceAccountId(setting);
    if (serviceAccountId) {
        secretObjects = signer ? fmt::format(
            R"(
                    UPSERT OBJECT {sa_secret_name} (TYPE SECRET) WITH value={signature};
                )",
            "sa_secret_name"_a = EncloseAndEscapeString("k1" + name, '`'),
            "signature"_a       = EncloseSecret(EncloseAndEscapeString(SignAccountId(serviceAccountId, signer), '"'))) : std::string{};
    }

    auto password = GetPassword(setting);
    if (password) {
        secretObjects += fmt::format(
                R"(
                    UPSERT OBJECT {password_secret_name} (TYPE SECRET) WITH value={password};
                )",
            "password_secret_name"_a = EncloseAndEscapeString("k2" + name, '`'),
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
                "password_secret_name"_a = EncloseAndEscapeString(signer ? "k1" + name : TString{}, '"'));
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
                "password_secret_name"_a = EncloseAndEscapeString(signer ? "k2" + name : TString{}, '"'));
    }
}

TString MakeCreateExternalDataSourceQuery(
    const FederatedQuery::ConnectionContent& connectionContent,
    const TSigner::TPtr& signer,
    const NConfig::TCommonConfig& common) {
    using namespace fmt::literals;

    TString properties;
    switch (connectionContent.setting().connection_case()) {
        case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
        case FederatedQuery::ConnectionSetting::kYdbDatabase:
        break;
        case FederatedQuery::ConnectionSetting::kClickhouseCluster:
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="ClickHouse",
                    MDB_CLUSTER_ID={mdb_cluster_id},
                    DATABASE_NAME={database_name},
                    PROTOCOL="{protocol}",
                    USE_TLS="true"
                )",
                "mdb_cluster_id"_a = EncloseAndEscapeString(connectionContent.setting().clickhouse_cluster().database_id(), '"'),
                "database_name"_a = EncloseAndEscapeString(connectionContent.setting().clickhouse_cluster().database_name(), '"'),
                "protocol"_a = common.GetUseNativeProtocolForClickHouse() ? "NATIVE" : "HTTP");
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
        case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
            const auto schema = connectionContent.setting().postgresql_cluster().schema();
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="PostgreSQL",
                    MDB_CLUSTER_ID={mdb_cluster_id},
                    DATABASE_NAME={database_name},
                    PROTOCOL="NATIVE",
                    USE_TLS="true"
                    {schema}
                )",
                "mdb_cluster_id"_a = EncloseAndEscapeString(connectionContent.setting().postgresql_cluster().database_id(), '"'),
                "database_name"_a = EncloseAndEscapeString(connectionContent.setting().postgresql_cluster().database_name(), '"'),
                "schema"_a =  schema ? ", SCHEMA=" + EncloseAndEscapeString(schema, '"') : TString{});
        break;
    }

    auto sourceName = connectionContent.name();
    return fmt::format(
        R"(
                CREATE EXTERNAL DATA SOURCE {external_source} WITH (
                    {properties}
                    {auth_params}
                );
            )",
        "external_source"_a = EncloseAndEscapeString(sourceName, '`'),
        "properties"_a = properties,
        "auth_params"_a =
            CreateAuthParamsQuery(connectionContent.setting(),
                                  connectionContent.name(),
                                  signer));
}

TMaybe<TString> DropSecretObjectQuery(const TString& name) {
    using namespace fmt::literals;
    return fmt::format(
            R"(
                DROP OBJECT {secret_name1} (TYPE SECRET);
                DROP OBJECT {secret_name2} (TYPE SECRET);
                DROP OBJECT {secret_name3} (TYPE SECRET); -- for backward compatibility
            )",
        "secret_name1"_a = EncloseAndEscapeString("k1" + name, '`'),
        "secret_name2"_a = EncloseAndEscapeString("k2" + name, '`'),
        "secret_name3"_a = EncloseAndEscapeString(name, '`'));
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
