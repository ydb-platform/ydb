#include "db_async_resolver.h"

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
        ToString(NYql::EDatabaseType::Solomon)
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
        default:
            ythrow yexception() << "Unknown data source kind: " << NYql::EGenericDataSourceKind_Name(dataSourceKind);
    }
}

NYql::EGenericDataSourceKind DatabaseTypeToDataSourceKind(EDatabaseType databaseType) {
    switch (databaseType) {
        case EDatabaseType::PostgreSQL:
            return  NYql::EGenericDataSourceKind::POSTGRESQL;
        case EDatabaseType::ClickHouse:
            return  NYql::EGenericDataSourceKind::CLICKHOUSE;
        case EDatabaseType::Ydb:
            return  NYql::EGenericDataSourceKind::YDB;
        case EDatabaseType::MySQL:
            return NYql::EGenericDataSourceKind::MYSQL;
        case EDatabaseType::Greenplum:
            return  NYql::EGenericDataSourceKind::GREENPLUM;
        case EDatabaseType::MsSQLServer:
            return NYql::EGenericDataSourceKind::MS_SQL_SERVER;
        case EDatabaseType::Oracle:
            return NYql::EGenericDataSourceKind::ORACLE;
        case EDatabaseType::Logging:
            return NYql::EGenericDataSourceKind::LOGGING;
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

} // NYql
