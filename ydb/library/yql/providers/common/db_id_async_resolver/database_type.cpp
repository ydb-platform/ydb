#include "db_async_resolver.h"

#include <util/generic/serialized_enum.h>
#include <util/string/cast.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

namespace NYql {

std::set<TString> GetAllExternalDataSourceTypes() {
    static const std::set<TString> allTypes = [] {
        std::set<TString> types;
        for (const auto type : GetEnumAllValues<EDatabaseType>()) {
            types.insert(ToString(type));
        }
        return types;
    }();
    return allTypes;
}

bool IsValidAvailableExternalDataSourceType(const TString& type) {
    static const auto allTypes = GetAllExternalDataSourceTypes();
    return type == "YdbTopics" || allTypes.contains(type);
}

std::set<EDatabaseType> GetAllExternalDataSourceDatabaseTypes() {
    static const std::set<EDatabaseType> allTypes = [] {
        const auto values = GetEnumAllValues<EDatabaseType>();
        return std::set<EDatabaseType>(values.begin(), values.end());
    }();
    return allTypes;
}

EDatabaseType DatabaseTypeFromDataSourceKind(NYql::EGenericDataSourceKind dataSourceKind) {
    switch (dataSourceKind) {
        case NYql::EGenericDataSourceKind::POSTGRESQL:
            return EDatabaseType::PostgreSQL;
        case NYql::EGenericDataSourceKind::CLICKHOUSE:
            return EDatabaseType::ClickHouse;
        case NYql::EGenericDataSourceKind::YDB:
            return EDatabaseType::Ydb;
        case NYql::EGenericDataSourceKind::MYSQL:
            return EDatabaseType::MySQL;
        case NYql::EGenericDataSourceKind::GREENPLUM:
            return EDatabaseType::Greenplum;
        case NYql::EGenericDataSourceKind::MS_SQL_SERVER:
            return EDatabaseType::MsSQLServer;
        case NYql::EGenericDataSourceKind::ORACLE:
            return EDatabaseType::Oracle;
        case NYql::EGenericDataSourceKind::LOGGING:
            return EDatabaseType::Logging;
        case NYql::EGenericDataSourceKind::ICEBERG:
            return EDatabaseType::Iceberg;
        case NYql::EGenericDataSourceKind::REDIS:
            return EDatabaseType::Redis;
        case NYql::EGenericDataSourceKind::PROMETHEUS:
            return EDatabaseType::Prometheus;
        case NYql::EGenericDataSourceKind::MONGO_DB:
            return EDatabaseType::MongoDB;
        case NYql::EGenericDataSourceKind::OPENSEARCH:
            return EDatabaseType::OpenSearch; 
        default:
            ythrow yexception() << "Unknown data source kind: " << NYql::EGenericDataSourceKind_Name(dataSourceKind);
    }
}

NYql::EGenericDataSourceKind DatabaseTypeToDataSourceKind(EDatabaseType databaseType) {
    switch (databaseType) {
        case EDatabaseType::PostgreSQL:
            return NYql::EGenericDataSourceKind::POSTGRESQL;
        case EDatabaseType::ClickHouse:
            return NYql::EGenericDataSourceKind::CLICKHOUSE;
        case EDatabaseType::Ydb:
            return NYql::EGenericDataSourceKind::YDB;
        case EDatabaseType::MySQL:
            return NYql::EGenericDataSourceKind::MYSQL;
        case EDatabaseType::Greenplum:
            return NYql::EGenericDataSourceKind::GREENPLUM;
        case EDatabaseType::MsSQLServer:
            return NYql::EGenericDataSourceKind::MS_SQL_SERVER;
        case EDatabaseType::Oracle:
            return NYql::EGenericDataSourceKind::ORACLE;
        case EDatabaseType::Logging:
            return NYql::EGenericDataSourceKind::LOGGING;
        case EDatabaseType::Iceberg:
            return NYql::EGenericDataSourceKind::ICEBERG;
        case EDatabaseType::Redis:
            return NYql::EGenericDataSourceKind::REDIS;
        case EDatabaseType::Prometheus:
            return NYql::EGenericDataSourceKind::PROMETHEUS;
        case EDatabaseType::MongoDB:
            return NYql::EGenericDataSourceKind::MONGO_DB;
        case EDatabaseType::OpenSearch:
            return NYql::EGenericDataSourceKind::OPENSEARCH;    
        default:
            ythrow yexception() << "Unknown database type: " << ToString(databaseType);
    }
}

TString DatabaseTypeLowercase(EDatabaseType databaseType) {
    auto dump = ToString(databaseType);
    dump.to_lower();
    return dump;
}

// TODO: remove this function after /kikimr/yq/tests/control_plane_storage is moved to /ydb.
TString DatabaseTypeToMdbUrlPath(EDatabaseType databaseType) {
    return DatabaseTypeLowercase(databaseType);
}

std::optional<EDatabaseType> DatabaseTypeFromString(const TString& type) {
    EDatabaseType databaseType;
    if (!TryFromString<EDatabaseType>(type, databaseType)) {
        return std::nullopt;
    }
    return databaseType;
}

} // NYql
