#include <fmt/format.h>

#include <util/generic/serialized_enum.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/generic/yexception.h>

#include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/external_data_source.h>

#include "yql_generic_cluster_config.h"

namespace NYql {
    using namespace NConnector;
    using namespace NConnector::NApi;
    using namespace fmt::literals;

    void ParseLogin(
        const THashMap<TString, TString>& properties,
        NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("login");
        if (it == properties.cend()) {
            ythrow yexception() << "missing 'LOGIN' value";
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
            ythrow yexception() << "missing 'PASSWORD' value";
        }

        if (!it->second) {
            ythrow yexception() << "empty 'PASSWORD' value";
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
            // TODO: make this property required during https://st.yandex-team.ru/YQ-2184
            // ythrow yexception() <<  "missing 'DATABASE_NAME' value";
            return;
        }

        if (!it->second) {
            // TODO: make this property required during https://st.yandex-team.ru/YQ-2184
            // ythrow yexception() << "invalid 'DATABASE_NAME' value: '" << it->second << "'";
            return;
        }

        clusterConfig.SetDatabaseName(it->second);

        return;
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

    static THashMap<EExternalDataSource, EDataSourceKind> DataSourceApiMapping = {
        {EExternalDataSource::ClickHouse, EDataSourceKind::CLICKHOUSE},
        {EExternalDataSource::PostgreSQL, EDataSourceKind::POSTGRESQL},
    };

    void ParseSourceType(const THashMap<TString, TString>& properties,
                         NYql::TGenericClusterConfig& clusterConfig) {
        auto it = properties.find("source_type");

        if (it == properties.cend()) {
            ythrow yexception() << "missing 'SOURCE_TYPE' value";
        }

        EExternalDataSource externalDataSource;
        if (!TryFromString<EExternalDataSource>(it->second, externalDataSource)) {
            ythrow yexception() << "invalid 'SOURCE_TYPE' value: '" << it->second
                                << "', valid types are: " << GetEnumAllNames<EExternalDataSource>();
        }

        if (!DataSourceApiMapping.contains(externalDataSource)) {
            ythrow yexception() << "cannot map 'SOURCE_TYPE' value: '" << it->second << "' into Connector API value";
        }

        clusterConfig.SetKind(DataSourceApiMapping.at(externalDataSource));
    }

    void ParseProtocol(const THashMap<TString, TString>& properties,
                       NYql::TGenericClusterConfig& clusterConfig) {
        using namespace NConnector::NApi;

        auto it = properties.find("protocol");
        if (it == properties.cend()) {
            ythrow yexception() << "missing 'PROTOCOL' value";
        }

        auto input = it->second;
        input.to_upper();

        EProtocol protocol;
        if (!EProtocol_Parse(input, &protocol)) {
            TStringBuilder b;
            b << "invalid 'PROTOCOL' value: '" << it->second << "', valid types are: ";
            for (auto i = EProtocol_MIN + 1; i < EProtocol_MAX; i++) {
                b << EProtocol_Name(i);
                if (i != EProtocol_MAX - 1) {
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

    bool KeyIsSet(const THashMap<TString, TString>& properties, const TString& key) {
        const auto iter = properties.find(key);
        if (iter == properties.cend()) {
            return false;
        }

        return !iter->second.Empty();
    }

    TGenericClusterConfig GenericClusterConfigFromProperties(const TString& clusterName, const THashMap<TString, TString>& properties) {
        // some cross-parameter validations
        auto location = KeyIsSet(properties, "location");
        auto mdbClusterId = KeyIsSet(properties, "mdb_cluster_id");

        if ((location && mdbClusterId) || (!location and !mdbClusterId)) {
            ythrow yexception() << "you must provide either 'LOCATION' or 'MDB_CLUSTER_ID' parameter";
        }

        auto serviceAccountId = KeyIsSet(properties, "serviceAccountId");
        auto serviceAccountIdSignature = KeyIsSet(properties, "serviceAccountIdSignature");
        if ((serviceAccountId && !serviceAccountIdSignature) || (!serviceAccountId && serviceAccountIdSignature)) {
            ythrow yexception() << "you must provide either both 'SERVICE_ACCOUNT_ID' and 'SERVICE_ACCOUNT_ID_SIGNATURE' parameters or none of them";
        }

        NYql::TGenericClusterConfig clusterConfig;
        clusterConfig.set_name(clusterName);
        ParseLogin(properties, clusterConfig);
        ParsePassword(properties, clusterConfig);
        ParseLocation(properties, clusterConfig);
        ParseUseTLS(properties, clusterConfig);
        ParseDatabaseName(properties, clusterConfig);
        ParseMdbClusterId(properties, clusterConfig);
        ParseSourceType(properties, clusterConfig);
        ParseProtocol(properties, clusterConfig);
        ParseServiceAccountId(properties, clusterConfig);
        ParseServiceAccountIdSignature(properties, clusterConfig);

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
            Protocol={protocol}
        )",
            "context"_a = context,
            "msg"_a = msg,
            "name"_a = clusterConfig.GetName(),
            "kind"_a = NConnector::NApi::EDataSourceKind_Name(clusterConfig.GetKind()),
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
            "protocol"_a = NConnector::NApi::EProtocol_Name(clusterConfig.GetProtocol()));
    }

    void ValidateGenericClusterConfig(
        const NYql::TGenericClusterConfig& clusterConfig,
        const TString& context) {
        // cross-parameter validations for optional fields
        auto hasEndpoint = clusterConfig.HasEndpoint();
        auto databaseId = clusterConfig.GetDatabaseId();

        if ((hasEndpoint && databaseId)) {
            return ValidationError(
                clusterConfig,
                context,
                "both 'Endpoint' and 'DatabaseId' fields are set; you must set only one of them");
        }

        if (!hasEndpoint and !databaseId) {
            return ValidationError(
                clusterConfig,
                context,
                "none of 'Endpoint' and 'DatabaseId' fields are set; you must set one of them");
        }

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

        auto token = clusterConfig.GetToken();
        if ((serviceAccountId && serviceAccountIdSignature) && token) {
            return ValidationError(
                clusterConfig,
                context,
                "you must set either ('ServiceAccountId', 'ServiceAccountIdSignature') fields or 'Token' field or none of them");
        }

        // check required fields
        if (!clusterConfig.GetName()) {
            return ValidationError(clusterConfig, context, "empty field 'Name'");
        }

        if (clusterConfig.GetKind() == EDataSourceKind::DATA_SOURCE_KIND_UNSPECIFIED) {
            return ValidationError(clusterConfig, context, "empty field 'Kind'");
        }

        if (!clusterConfig.GetCredentials().Getbasic().Getusername()) {
            return ValidationError(clusterConfig, context, "empty field 'Credentials.basic.username'");
        }

        // TODO: validate Credentials.basic.password after ClickHouse recipe fix
        // TODO: validate DatabaseName field when it is supported on frontend

        if (clusterConfig.GetProtocol() == EProtocol::PROTOCOL_UNSPECIFIED) {
            return ValidationError(clusterConfig, context, "empty field 'Protocol'");
        }
    }
}
