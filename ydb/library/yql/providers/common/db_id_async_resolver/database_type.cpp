#include "db_async_resolver.h"

#include <util/generic/map.h>

#include <util/string/cast.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

namespace NYql {

std::set<TString> GetAllExternalDataSourceTypes() {
    static std::set<TString> allTypes = {
        ToString(NYql::EDatabaseType::ObjectStorage),
        ToString(NYql::EDatabaseType::ClickHouse),
        ToString(NYql::EDatabaseType::PostgreSQL),
        ToString(NYql::EDatabaseType::MySQL),
        ToString(NYql::EDatabaseType::Ydb),
        ToString(NYql::EDatabaseType::YT),
        ToString(NYql::EDatabaseType::Greenplum),
        ToString(NYql::EDatabaseType::MsSQLServer),
        ToString(NYql::EDatabaseType::Oracle),
        ToString(NYql::EDatabaseType::Logging),
        ToString(NYql::EDatabaseType::Solomon),
        ToString(NYql::EDatabaseType::MoniumMetrics),
        ToString(NYql::EDatabaseType::Iceberg),
        ToString(NYql::EDatabaseType::Redis),
        ToString(NYql::EDatabaseType::Prometheus),
        ToString(NYql::EDatabaseType::OpenSearch),
        ToString(NYql::EDatabaseType::DataStreams),
        ToString(NYql::EDatabaseType::YdbTopics),
    };
    return allTypes;
}

std::set<EDatabaseType> GetAllExternalDataSourceDatabaseTypes() {
    static std::set<EDatabaseType> allTypes = {
        EDatabaseType::ObjectStorage,
        EDatabaseType::ClickHouse,
        EDatabaseType::PostgreSQL,
        EDatabaseType::MySQL,
        EDatabaseType::Ydb,
        EDatabaseType::YT,
        EDatabaseType::Greenplum,
        EDatabaseType::MsSQLServer,
        EDatabaseType::Oracle,
        EDatabaseType::Logging,
        EDatabaseType::Solomon,
        EDatabaseType::MoniumMetrics,
        EDatabaseType::Iceberg,
        EDatabaseType::Redis,
        EDatabaseType::Prometheus,
        EDatabaseType::OpenSearch,
        EDatabaseType::DataStreams,
        EDatabaseType::YdbTopics,
    };
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
    static const TMap<TString, EDatabaseType> typeMap = {
        {ToString(EDatabaseType::Ydb), EDatabaseType::Ydb},
        {ToString(EDatabaseType::ClickHouse), EDatabaseType::ClickHouse},
        {ToString(EDatabaseType::DataStreams), EDatabaseType::DataStreams},
        {ToString(EDatabaseType::ObjectStorage), EDatabaseType::ObjectStorage},
        {ToString(EDatabaseType::PostgreSQL), EDatabaseType::PostgreSQL},
        {ToString(EDatabaseType::YT), EDatabaseType::YT},
        {ToString(EDatabaseType::MySQL), EDatabaseType::MySQL},
        {ToString(EDatabaseType::Greenplum), EDatabaseType::Greenplum},
        {ToString(EDatabaseType::MsSQLServer), EDatabaseType::MsSQLServer},
        {ToString(EDatabaseType::Oracle), EDatabaseType::Oracle},
        {ToString(EDatabaseType::Logging), EDatabaseType::Logging},
        {ToString(EDatabaseType::Solomon), EDatabaseType::Solomon},
        {ToString(EDatabaseType::MoniumMetrics), EDatabaseType::MoniumMetrics},
        {ToString(EDatabaseType::Iceberg), EDatabaseType::Iceberg},
        {ToString(EDatabaseType::Redis), EDatabaseType::Redis},
        {ToString(EDatabaseType::Prometheus), EDatabaseType::Prometheus},
        {ToString(EDatabaseType::MongoDB), EDatabaseType::MongoDB},
        {ToString(EDatabaseType::OpenSearch), EDatabaseType::OpenSearch},
        {ToString(EDatabaseType::YdbTopics), EDatabaseType::YdbTopics},
    };
    auto it = typeMap.find(type);
    if (it == typeMap.end()) {
        return std::nullopt;
    }
    return it->second;
}

} // NYql
