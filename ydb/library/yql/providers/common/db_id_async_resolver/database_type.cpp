#include "db_async_resolver.h"

#include <util/string/cast.h>

#include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>

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
        ToString(NYql::EDatabaseType::MsSQLServer)
    };
    return allTypes;
}

EDatabaseType DatabaseTypeFromDataSourceKind(NConnector::NApi::EDataSourceKind dataSourceKind) {
    switch (dataSourceKind) {
        case NConnector::NApi::EDataSourceKind::POSTGRESQL:
            return EDatabaseType::PostgreSQL;
        case NConnector::NApi::EDataSourceKind::CLICKHOUSE:
            return EDatabaseType::ClickHouse;
        case NConnector::NApi::EDataSourceKind::YDB:
            return EDatabaseType::Ydb;
        case NConnector::NApi::EDataSourceKind::MYSQL:
            return EDatabaseType::MySQL;
        case NConnector::NApi::EDataSourceKind::GREENPLUM:
            return EDatabaseType::Greenplum;
        case NConnector::NApi::EDataSourceKind::MS_SQL_SERVER:
          return EDatabaseType::MsSQLServer;
        default:
            ythrow yexception() << "Unknown data source kind: " << NConnector::NApi::EDataSourceKind_Name(dataSourceKind);
    }
}

NConnector::NApi::EDataSourceKind DatabaseTypeToDataSourceKind(EDatabaseType databaseType) {
    switch (databaseType) {
        case EDatabaseType::PostgreSQL:
            return NConnector::NApi::EDataSourceKind::POSTGRESQL;
        case EDatabaseType::ClickHouse:
            return NConnector::NApi::EDataSourceKind::CLICKHOUSE;
        case EDatabaseType::Ydb:
            return NConnector::NApi::EDataSourceKind::YDB;
        case EDatabaseType::MySQL:
            return NConnector::NApi::EDataSourceKind::MYSQL;
        case EDatabaseType::Greenplum:
            return NConnector::NApi::EDataSourceKind::GREENPLUM;
        case EDatabaseType::MsSQLServer:
            return NConnector::NApi::EDataSourceKind::MS_SQL_SERVER;
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
