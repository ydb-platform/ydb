#pragma once

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYql {

enum class EDatabaseType {
    Ydb,
    ClickHouse,
    DataStreams,
    ObjectStorage,
    PostgreSQL,
    YT,
    MySQL,
    Greenplum,
    MsSQLServer
};

std::set<TString> GetAllExternalDataSourceTypes();

EDatabaseType DatabaseTypeFromDataSourceKind(NYql::EGenericDataSourceKind dataSourceKind);

NYql::EGenericDataSourceKind DatabaseTypeToDataSourceKind(EDatabaseType databaseType);

TString DatabaseTypeLowercase(EDatabaseType databaseType);

// TODO: remove this function after /kikimr/yq/tests/control_plane_storage is moved to /ydb.
TString DatabaseTypeToMdbUrlPath(EDatabaseType databaseType);

} // NYql
