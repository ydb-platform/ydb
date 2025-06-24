#include "query_utils.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <util/generic/maybe.h>

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/core/fq/libs/common/iceberg_processor.h>

namespace NFq {
namespace NPrivate {

namespace {

TString MakeSecretKeyName(const TString& prefix, const TString& folderId, const TString& name) {
    return TStringBuilder{} << prefix << "_" << folderId << "_" << name;
}

TString MakeCreateSecretObjectSql(const TString& name, const TString& value) {
    using namespace fmt::literals;
    return fmt::format(
        R"(
                UPSERT OBJECT {name} (TYPE SECRET) WITH value={value};
            )",
        "name"_a = EncloseAndEscapeString(name, '`'),
        "value"_a = EncloseSecret(EncloseAndEscapeString(value, '"')));
}

TString MakeCreateSecretAccessObjectSql(const TString& name, const TString& sid) {
    using namespace fmt::literals;
    TString accessName = TStringBuilder{} << name << ":" << sid;
    return fmt::format(
        R"(
            UPSERT OBJECT {name} (TYPE SECRET_ACCESS);
        )",
        "name"_a = EncloseAndEscapeString(accessName, '`'));
}

TString MakeCreateSecretAccessObjectsSql(const TString& name, const TVector<TString>& externalSourcesAccessSIDs) {
    TStringBuilder result;
    for (const auto& sid : externalSourcesAccessSIDs) {
        result << MakeCreateSecretAccessObjectSql(name, sid);
    }
    return result;
}

TString MakeDropSecretObjectSql(const TString& name) {
    using namespace fmt::literals;
    return fmt::format(
        R"(
                DROP OBJECT {name} (TYPE SECRET);
            )",
        "name"_a = EncloseAndEscapeString(name, '`'));
}

TString MakeDropAccessSecretObjectSql(const TString& name, const TString& sid) {
    using namespace fmt::literals;
    TString accessName = TStringBuilder{} << name << ":" << sid;
    return fmt::format(
        R"(
            DROP OBJECT {name} (TYPE SECRET_ACCESS);
        )",
        "name"_a = EncloseAndEscapeString(accessName, '`'));
}

TString MakeDropSecretAccessObjectsSql(const TString& name, const TVector<TString>& externalSourcesAccessSIDs) {
    TStringBuilder result;
    for (const auto& sid : externalSourcesAccessSIDs) {
        result << MakeDropAccessSecretObjectSql(name, sid);
    }
    return result;
}

}

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
    if (!subset.format().empty()) {
        withOptions.insert({"FORMAT", EncloseAndEscapeString(subset.format(), '"')});
    }
    if (!subset.compression().empty()) {
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
                                        const TString& folderId,
                                        const TVector<TString>& externalSourcesAccessSIDs) {
    TStringBuilder result;
    auto serviceAccountId = ExtractServiceAccountId(setting);
    if (serviceAccountId && signer) {
        const TString saSecretName = MakeSecretKeyName("f1", folderId, name);
        const TString signature = SignAccountId(serviceAccountId, signer);
        result << MakeCreateSecretObjectSql(saSecretName, signature);
        result << MakeCreateSecretAccessObjectsSql(saSecretName, externalSourcesAccessSIDs);
    }

    auto password = GetPassword(setting);
    if (password) {
        const TString passwordSecretName = MakeSecretKeyName("f2", folderId, name);
        result << MakeCreateSecretObjectSql(passwordSecretName, *password);
        result << MakeCreateSecretAccessObjectsSql(passwordSecretName, externalSourcesAccessSIDs);
    }

    return result ? result : TMaybe<TString>{};
}

TString CreateAuthParamsQuery(const FederatedQuery::ConnectionSetting& setting,
                              const TString& name,
                              const TSigner::TPtr& signer,
                              const TString& folderId) {
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
            "sa_secret_name"_a = EncloseAndEscapeString(signer ? MakeSecretKeyName("f1", folderId, name) : TString{}, '"'));
        case EYdbComputeAuth::BASIC:
            return fmt::format(
                    R"(,
                        AUTH_METHOD="{auth_method}",
                        LOGIN={login},
                        PASSWORD_SECRET_NAME={password_secret_name}
                    )",
                "auth_method"_a = ToString(authMethod),
                "login"_a = EncloseAndEscapeString(GetLogin(setting).GetOrElse({}), '"'),
                "password_secret_name"_a = EncloseAndEscapeString(MakeSecretKeyName("f2", folderId, name), '"'));
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
                "sa_secret_name"_a = EncloseAndEscapeString(signer ? MakeSecretKeyName("f1", folderId, name) : TString{}, '"'),
                "login"_a = EncloseAndEscapeString(GetLogin(setting).GetOrElse({}), '"'),
                "password_secret_name"_a = EncloseAndEscapeString(MakeSecretKeyName("f2", folderId, name), '"'));
    }
}

TString MakeCreateExternalDataSourceQuery(
    const FederatedQuery::ConnectionContent& connectionContent,
    const TSigner::TPtr& signer,
    const NConfig::TCommonConfig& common,
    bool replaceIfExists,
    const TString& folderId) {
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
        case FederatedQuery::ConnectionSetting::kMonitoring: {
            auto project = connectionContent.setting().monitoring().project();
            auto cluster = connectionContent.setting().monitoring().cluster();
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="Solomon",
                    LOCATION={http_location},
                    GRPC_LOCATION={grpc_location},
                    USE_TLS="{use_tls}",
                    PROJECT={project},
                    CLUSTER={cluster}
                )",
                "http_location"_a = EncloseAndEscapeString(common.GetMonitoringReadHttpEndpoint(), '"'),
                "grpc_location"_a = EncloseAndEscapeString(common.GetMonitoringReadGrpcEndpoint(), '"'),
                "use_tls"_a = "true",
                "project"_a = EncloseAndEscapeString(project, '"'),
                "cluster"_a = EncloseAndEscapeString(cluster, '"')
            );
            break;
        }
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
            break;
        }
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
            break;
        }
        case FederatedQuery::ConnectionSetting::kIceberg: {
            auto settings = connectionContent.setting().iceberg();
            properties = NFq::MakeIcebergCreateExternalDataSourceProperties(common, settings);
            break;
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
            break;
        }
        case FederatedQuery::ConnectionSetting::kLogging: {
            properties = fmt::format(
                R"(
                    SOURCE_TYPE="Logging",
                    FOLDER_ID={folder_id}
                )",
                "folder_id"_a = EncloseAndEscapeString(connectionContent.setting().logging().folder_id(), '"'));
            break;
        }
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
                                  signer,
                                  folderId));
}

TMaybe<TString> DropSecretObjectQuery(const TString& name, const TString& folderId, const TVector<TString>& externalSourcesAccessSIDs) {
    const TString secretName1 = MakeSecretKeyName("f1", folderId, name);
    const TString secretName2 = MakeSecretKeyName("f2", folderId, name);
    return TStringBuilder{}
            << MakeDropSecretAccessObjectsSql(secretName1, externalSourcesAccessSIDs)
            << MakeDropSecretObjectSql(secretName1)
            << MakeDropSecretAccessObjectsSql(secretName2, externalSourcesAccessSIDs)
            << MakeDropSecretObjectSql(secretName2);
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
