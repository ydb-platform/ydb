#pragma once

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <optional>
#include <set>

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
    MoniumMetrics /* "Monium.Metrics" */,
    Iceberg,
    Redis,
    Prometheus,
    MongoDB,
    OpenSearch,
};

std::set<TString> GetAllExternalDataSourceTypes();
bool IsValidAvailableExternalDataSourceType(const TString& type);

std::set<EDatabaseType> GetAllExternalDataSourceDatabaseTypes();

EDatabaseType DatabaseTypeFromDataSourceKind(NYql::EGenericDataSourceKind dataSourceKind);

std::optional<EDatabaseType> DatabaseTypeFromString(const TString& type);

inline TString ToStringDatabaseType(const std::optional<EDatabaseType>& type) {
    return type ? ToString(*type) : TString{};
}

inline TString ToStringDatabaseType(const std::optional<EDatabaseType>& type, const TString& fallback) {
    return type ? ToString(*type) : fallback;
}

NYql::EGenericDataSourceKind DatabaseTypeToDataSourceKind(EDatabaseType databaseType);

TString DatabaseTypeLowercase(EDatabaseType databaseType);

// TODO: remove this function after /kikimr/yq/tests/control_plane_storage is moved to /ydb.
TString DatabaseTypeToMdbUrlPath(EDatabaseType databaseType);

} // NYql
