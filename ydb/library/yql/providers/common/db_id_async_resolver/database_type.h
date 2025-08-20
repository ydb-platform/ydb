#pragma once

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

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
    MsSQLServer,
    Oracle,
    Logging,
    Solomon,
    Iceberg,
    Redis,
    Prometheus,
    MongoDB,
    OpenSearch,
    YdbTopics,
};

std::set<TString> GetAllExternalDataSourceTypes();

EDatabaseType DatabaseTypeFromDataSourceKind(NYql::EGenericDataSourceKind dataSourceKind);

NYql::EGenericDataSourceKind DatabaseTypeToDataSourceKind(EDatabaseType databaseType);

TString DatabaseTypeLowercase(EDatabaseType databaseType);

// TODO: remove this function after /kikimr/yq/tests/control_plane_storage is moved to /ydb.
TString DatabaseTypeToMdbUrlPath(EDatabaseType databaseType);

} // NYql
