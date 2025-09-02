#include <fmt/format.h>
#include <util/generic/set.h>

#include <util/generic/serialized_enum.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/generic/yexception.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/core/external_sources/iceberg_fields.h>

#include "yql_generic_cluster_config.h"

namespace NYql {
    using namespace fmt::literals;

    void ParseLogin(
        const THashMap<TString, TString>& properties,
        NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("login");
        if (it == properties.cend()) {
            // It's OK not to have credentials for base auth
            return;
        }

        if (!it->second) {
            ythrow yexception() << "empty 'LOGIN' value";
        }

        clusterConfig.MutableCredentials()->Mutablebasic()->Setusername(it->second);
    }

    void ParsePassword(
        const THashMap<TString, TString>& properties,
        NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("password");
        if (it == properties.cend()) {
            // It's OK not to have credentials for base auth
            return;
        }

        clusterConfig.MutableCredentials()->Mutablebasic()->Setpassword(it->second);
    }

    void ParseLocation(
        const THashMap<TString, TString>& properties,
        NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("location");

        if (it == properties.cend()) {
            return;
        }

        if (!it->second) {
            // LOCATION is an optional field
            return;
        }

        size_t pos = it->second.find(':');
        if (pos == TString::npos) {
            ythrow yexception() << "invalid 'LOCATION' value: '" << it->second << "': no ':' separator";
        }

        TString host = it->second.substr(0, pos);

        if (!host) {
            ythrow yexception() << "invalid 'LOCATION' value: '" << it->second << "': empty host";
        }

        ui32 port;
        if (!TryFromString<ui32>(it->second.substr(pos + 1), port)) {
            ythrow yexception() << "invalid 'LOCATION' value: '" << it->second << "': invalid port";
        }
        if (port < 1 || port > 65535) {
            ythrow yexception() << "invalid 'LOCATION' value: '" << it->second << "': invalid port";
        }

        clusterConfig.MutableEndpoint()->Sethost(host);
        clusterConfig.MutableEndpoint()->Setport(port);
    }

    void ParseUseTLS(const THashMap<TString, TString>& properties,
                     NYql::TGenericClusterConfig& clusterConfig) {
        // Disable secure connections if this wasn't explicitly specified
        auto it = properties.find("use_tls");
        if (it == properties.cend()) {
            clusterConfig.SetUseSsl(false);
            return;
        }

        TString transformed = it->second;
        transformed.to_lower();

        if (transformed == "true") {
            clusterConfig.SetUseSsl(true);
            return;
        }
        if (transformed == "false") {
            clusterConfig.SetUseSsl(false);
            return;
        }

        ythrow yexception() << "invalid 'USE_TLS' value: '" << it->second << "'";
    }

    void ParseDatabaseName(const THashMap<TString, TString>& properties,
                           NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("database_name");
        if (it == properties.cend()) {
            // DATABASE_NAME is a mandatory field for the most of databases,
            // however, managed YDB does not require it, so we have to accept empty values here.
            return;
        }

        if (!it->second) {
            return;
        }

        clusterConfig.SetDatabaseName(it->second);
    }

    void ParseMdbClusterId(const THashMap<TString, TString>& properties,
                           NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("mdb_cluster_id");
        if (it == properties.cend()) {
            return;
        }

        if (!it->second) {
            // MDB_CLUSTER_ID is an optional field
            return;
        }

        if (!it->second) {
            ythrow yexception() << "invalid 'MDB_CLUSTER_ID' value: '" << it->second << "'";
        }

        clusterConfig.SetDatabaseId(it->second);
    }

    void ParseDatabaseId(const THashMap<TString, TString>& properties,
                         NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("database_id");
        if (it == properties.cend()) {
            return;
        }

        if (!it->second) {
            // DATABASE_ID is an optional field
            return;
        }

        if (!it->second) {
            ythrow yexception() << "invalid 'DATABASE_ID' value: '" << it->second << "'";
        }

        clusterConfig.SetDatabaseId(it->second);
    }

    void ParseSourceType(const THashMap<TString, TString>& properties,
                         NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("source_type");

        if (it == properties.cend()) {
            ythrow yexception() << "missing 'SOURCE_TYPE' value";
        }

        clusterConfig.SetKind(DatabaseTypeToDataSourceKind(FromString<NYql::EDatabaseType>(it->second)));
    }

    void ParseProtocol(const THashMap<TString, TString>& properties,
                       NYql::TGenericClusterConfig& clusterConfig) {
        // For some datasources the PROTOCOL is not required
        if (clusterConfig.GetKind() == EGenericDataSourceKind::LOGGING) {
            clusterConfig.SetProtocol(EGenericProtocol::PROTOCOL_UNSPECIFIED);
            return;
        }

        // We use HTTP protocol to access OpenSearch
        if (clusterConfig.GetKind() == EGenericDataSourceKind::OPENSEARCH) {
            clusterConfig.SetProtocol(EGenericProtocol::HTTP);
            return;
        }

        // For the most of transactional databases the PROTOCOL is always NATIVE 
        if (IsIn({
                EGenericDataSourceKind::GREENPLUM,
                EGenericDataSourceKind::YDB,
                EGenericDataSourceKind::MYSQL,
                EGenericDataSourceKind::MS_SQL_SERVER,
                EGenericDataSourceKind::ORACLE,
                EGenericDataSourceKind::ICEBERG,
                EGenericDataSourceKind::REDIS,
                EGenericDataSourceKind::MONGO_DB
                }, 
               clusterConfig.GetKind()
            )) {
            clusterConfig.SetProtocol(EGenericProtocol::NATIVE);
            return;
        }

        // For the rest, parse the property into typed enum value
        auto it = properties.find("protocol");
        if (it == properties.cend()) {
            ythrow yexception() << "missing 'PROTOCOL' value";
        }

        auto input = it->second;
        input.to_upper();

        EGenericProtocol protocol;
        if (!NYql::EGenericProtocol_Parse(input, &protocol)) {
            TStringBuilder b;
            b << "invalid 'PROTOCOL' value: '" << it->second << "', valid types are: ";
            for (auto i = EGenericProtocol_MIN + 1; i < EGenericProtocol_MAX; i++) {
                b << NYql::EGenericProtocol_Name(i);
                if (i != EGenericProtocol_MAX - 1) {
                    b << ", ";
                }
            }

            ythrow yexception() << b;
        }

        clusterConfig.SetProtocol(protocol);
    }

    void ParseServiceAccountId(const THashMap<TString, TString>& properties,
                               NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("serviceAccountId");
        if (it == properties.cend()) {
            return;
        }

        if (!it->second) {
            return;
        }

        clusterConfig.SetServiceAccountId(it->second);
    }

    void ParseServiceAccountIdSignature(const THashMap<TString, TString>& properties,
                                        NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("serviceAccountIdSignature");
        if (it == properties.cend()) {
            return;
        }

        if (!it->second) {
            return;
        }

        clusterConfig.SetServiceAccountIdSignature(it->second);
    }

    void ParseOptionalField(const THashMap<TString, TString>& properties,
                     NYql::TGenericClusterConfig& clusterConfig, const TString& fieldName) {
        auto it = properties.find(fieldName);
        if (it == properties.cend()) {
            return;
        }

        if (!it->second) {
            return;
        }

        clusterConfig.mutable_datasourceoptions()->insert({fieldName, TString(it->second)});
    }

    ///
    /// Extract token from properties and copy it to a cluster's config
    ///
    void ParseToken(const THashMap<TString, TString>& properties,
        NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("token");

        if (it == properties.cend()) {
            return;
        }

        if (!it->second) {
            return;
        }

        clusterConfig.SetToken(it->second);
    }

    ///
    /// Fill properties for an iceberg data source
    ///
    void ParseIcebergFields(const THashMap<TString, TString>& properties,
        NYql::TGenericClusterConfig& clusterConfig) {

        if (clusterConfig.GetKind() != NYql::EGenericDataSourceKind::ICEBERG) {
            return;
        }

        for (auto f : NKikimr::NExternalSource::NIceberg::FieldsToConnector) {
            auto it = properties.find(f);

            if (properties.end() != it) {
                clusterConfig.MutableDataSourceOptions()->insert({f, it->second});
            }
        }
    }

    using TProtoProperties = google::protobuf::Map<TProtoStringType, TProtoStringType>;

    TString GetPropertyWithDefault(const TProtoProperties& properties, const TString& key) {
        const auto iter = properties.find(key);
        if (iter == properties.cend()) {
            return TString{};
        }

        return iter->second;
    }

    TGenericClusterConfig GenericClusterConfigFromProperties(const TString& clusterName, const THashMap<TString, TString>& properties) {
        NYql::TGenericClusterConfig clusterConfig;
        clusterConfig.set_name(clusterName);
        ParseLogin(properties, clusterConfig);
        ParsePassword(properties, clusterConfig);
        ParseLocation(properties, clusterConfig);
        ParseUseTLS(properties, clusterConfig);
        ParseDatabaseName(properties, clusterConfig);
        ParseMdbClusterId(properties, clusterConfig);
        ParseDatabaseId(properties, clusterConfig);
        ParseSourceType(properties, clusterConfig);
        ParseProtocol(properties, clusterConfig);
        ParseServiceAccountId(properties, clusterConfig);
        ParseServiceAccountIdSignature(properties, clusterConfig);
        ParseToken(properties, clusterConfig);
        ParseIcebergFields(properties, clusterConfig);
        ParseOptionalField(properties, clusterConfig, "schema");
        ParseOptionalField(properties, clusterConfig, "folder_id");
        ParseOptionalField(properties, clusterConfig, "reading_mode");
        ParseOptionalField(properties, clusterConfig, "service_name");
        ParseOptionalField(properties, clusterConfig, "unexpected_type_display_mode");
        ParseOptionalField(properties, clusterConfig, "unsupported_type_display_mode");

        return clusterConfig;
    }

    void ValidationError(const NYql::TGenericClusterConfig& clusterConfig,
                         const TString& context,
                         const TString& msg) {
        ythrow yexception() << fmt::format(
            R"(
            {context}: invalid cluster config: {msg}.

            Full config dump:
            Name={name},
            Kind={kind},
            Location.Endpoint.host={host},
            Location.Endpoint.port={port},
            Location.DatabaseId={database_id},
            Credentials.basic.username={username},
            Credentials.basic.password=[{password} char(s)],
            ServiceAccountId={service_account_id},
            ServiceAccountIdSignature=[{service_account_id_signature} char(s)],
            Token=[{token} char(s)]
            UseSsl={use_ssl},
            DatabaseName={database_name},
            Protocol={protocol},
            Schema={schema},
            FolderId={folder_id}
        )",
            "context"_a = context,
            "msg"_a = msg,
            "name"_a = clusterConfig.GetName(),
            "kind"_a = NYql::EGenericDataSourceKind_Name(clusterConfig.GetKind()),
            "host"_a = clusterConfig.GetEndpoint().Gethost(),
            "port"_a = clusterConfig.GetEndpoint().Getport(),
            "database_id"_a = clusterConfig.GetDatabaseId(),
            "username"_a = clusterConfig.GetCredentials().Getbasic().Getusername(),
            "password"_a = ToString(clusterConfig.GetCredentials().Getbasic().Getpassword().size()),
            "service_account_id"_a = clusterConfig.GetServiceAccountId(),
            "service_account_id_signature"_a = ToString(clusterConfig.GetServiceAccountIdSignature().size()),
            "token"_a = ToString(clusterConfig.GetToken().size()),
            "use_ssl"_a = clusterConfig.GetUseSsl() ? "TRUE" : "FALSE",
            "database_name"_a = clusterConfig.GetDatabaseName(),
            "protocol"_a = NYql::EGenericProtocol_Name(clusterConfig.GetProtocol()),
            "schema"_a = GetPropertyWithDefault(clusterConfig.datasourceoptions(), "schema"),
            "folder_id"_a = GetPropertyWithDefault(clusterConfig.datasourceoptions(), "folder_id")
        );
    }

    static const TSet<NYql::EGenericDataSourceKind> managedDatabaseKinds{
        NYql::EGenericDataSourceKind::CLICKHOUSE,
        NYql::EGenericDataSourceKind::GREENPLUM,
        NYql::EGenericDataSourceKind::MYSQL,
        NYql::EGenericDataSourceKind::POSTGRESQL,
        NYql::EGenericDataSourceKind::YDB,
    };

    static const TSet<NYql::EGenericDataSourceKind> traditionalRelationalDatabaseKinds{
        NYql::EGenericDataSourceKind::CLICKHOUSE,
        NYql::EGenericDataSourceKind::GREENPLUM,
        NYql::EGenericDataSourceKind::MS_SQL_SERVER,
        NYql::EGenericDataSourceKind::MYSQL,
        NYql::EGenericDataSourceKind::ORACLE,
        NYql::EGenericDataSourceKind::POSTGRESQL,
    };

    bool DataSourceMustHaveDataBaseName(const NYql::EGenericDataSourceKind& sourceKind) {
        return traditionalRelationalDatabaseKinds.contains(sourceKind) && sourceKind != NYql::EGenericDataSourceKind::ORACLE;
    }

    void ValidateGenericClusterConfig(
        const NYql::TGenericClusterConfig& clusterConfig,
        const TString& context) {
        // Service account ID and service account ID signature are tightly coupled:
        // if one is set, another one must be set too.
        auto serviceAccountId = clusterConfig.GetServiceAccountId();
        auto serviceAccountIdSignature = clusterConfig.GetServiceAccountIdSignature();
        if (serviceAccountId && !serviceAccountIdSignature) {
            return ValidationError(
                clusterConfig,
                context,
                "'ServiceAccountId' field is set, but 'ServiceAccountIdSignature' field is not set; "
                "you must set either both 'ServiceAccountId' and 'ServiceAccountIdSignature' fields or none of them");
        }

        if (!serviceAccountId && serviceAccountIdSignature) {
            return ValidationError(
                clusterConfig,
                context,
                "'ServiceAccountIdSignature' field is set, but 'ServiceAccountId' field is not set; "
                "you must set either both 'ServiceAccountId' and 'ServiceAccountIdSignature' fields or none of them");
        }

        // Service account credentials and raw tokens are mutually exclusive:
        // no need to specify service account parameters if one already has a token.
        auto token = clusterConfig.GetToken();
        if ((serviceAccountId && serviceAccountIdSignature) && token) {
            return ValidationError(
                clusterConfig,
                context,
                "you must set either ('ServiceAccountId', 'ServiceAccountIdSignature') fields or 'Token' field or none of them");
        }

        // All managed databases:
        // * set endpoint when working with on-prem instances
        // * set database id when working with managed instances
        if (managedDatabaseKinds.contains(clusterConfig.GetKind())) {
            auto hasEndpoint = clusterConfig.HasEndpoint();
            auto hasDatabaseId = clusterConfig.HasDatabaseId();

            if (hasEndpoint && hasDatabaseId) {
                return ValidationError(
                    clusterConfig,
                    context,
                    "both 'Endpoint' and 'DatabaseId' fields are set; you must set only one of them");
            }

            if (!hasEndpoint and !hasDatabaseId) {
                return ValidationError(
                    clusterConfig,
                    context,
                    "none of 'Endpoint' and 'DatabaseId' fields are set; you must set one of them");
            }
        }

        // YDB:
        // * set database name when working with on-prem YDB instance;
        // * but set database ID when working with managed YDB.
        if (clusterConfig.GetKind() == NYql::EGenericDataSourceKind::YDB) {
            if (clusterConfig.HasDatabaseName() && clusterConfig.HasDatabaseId()) {
                return ValidationError(
                    clusterConfig,
                    context,
                    "For YDB clusters you must set either database name or database id, but you have set both of them");
            }

            if (!clusterConfig.HasDatabaseName() && !clusterConfig.HasDatabaseId()) {
                return ValidationError(
                    clusterConfig,
                    context,
                    "For YDB clusters you must set either database name or database id, but you have set none of them");
            }
        }

        // Oracle:
        // * always set service_name for oracle;
        if (clusterConfig.GetKind() == NYql::EGenericDataSourceKind::ORACLE) {
            if (!clusterConfig.GetDataSourceOptions().contains("service_name")) {
                return ValidationError(
                    clusterConfig,
                    context,
                    "For Oracle databases you must set service, but you have not set it");
            }
        }

        // All the databases with exception to managed YDB and Oracle:
        // * DATABASE_NAME is mandatory field
        if (DataSourceMustHaveDataBaseName(clusterConfig.GetKind())) {
            if (!clusterConfig.GetDatabaseName()) {
                return ValidationError(
                    clusterConfig,
                    context,
                    "You must provide database name explicitly");
            }
        }

        // check required fields
        if (!clusterConfig.GetName()) {
            return ValidationError(clusterConfig, context, "empty field 'Name'");
        }

        if (clusterConfig.GetKind() == NYql::EGenericDataSourceKind::DATA_SOURCE_KIND_UNSPECIFIED) {
            return ValidationError(clusterConfig, context, "empty field 'Kind'");
        }

        // TODO: validate Credentials.basic.password after ClickHouse recipe fix
    }
} // namespace NYql
