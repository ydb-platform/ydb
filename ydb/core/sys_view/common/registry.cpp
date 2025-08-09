#include "registry.h"

#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

namespace NKikimr {
namespace NSysView {

namespace {

TVector<Schema::PgColumn> GetPgStaticTableColumns(const TString& schema, const TString& tableName) {
    TVector<Schema::PgColumn> res;
    auto columns = NYql::NPg::GetStaticColumns().FindPtr(NYql::NPg::TTableInfoKey{schema, tableName});
    res.reserve(columns->size());
    for (size_t i = 0; i < columns->size(); i++) {
        const auto& column = columns->at(i);
        res.emplace_back(i, column.UdtType, column.Name);
    }
    return res;
}

}

Schema::PgColumn::PgColumn(NIceDb::TColumnId columnId, TStringBuf columnTypeName, TStringBuf columnName)
    : _ColumnId(columnId)
    , _ColumnTypeInfo(NPg::TypeDescFromPgTypeId(NYql::NPg::LookupType(TString(columnTypeName)).TypeId))
    , _ColumnName(columnName)
{}

const TVector<Schema::PgColumn>& Schema::PgTablesSchemaProvider::GetColumns(TStringBuf tableName) const {
    TString key(tableName);
    Y_ENSURE(columnsStorage.contains(key));
    return columnsStorage.at(key);
}

Schema::PgTablesSchemaProvider::PgTablesSchemaProvider() {
    columnsStorage[TString(PgTablesName)] = GetPgStaticTableColumns("pg_catalog", "pg_tables");
    columnsStorage[TString(InformationSchemaTablesName)] = GetPgStaticTableColumns("information_schema", "tables");
    columnsStorage[TString(PgClassName)] = GetPgStaticTableColumns("pg_catalog", "pg_class");
}

const TVector<SysViewsRegistryRecord> SysViewsRegistry::SysViews = {
    {"partition_stats", ESysViewType::EPartitionStats, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::PartitionStats>},
    {"nodes", ESysViewType::ENodes, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::Nodes>},

    {"top_queries_by_duration_one_minute", ESysViewType::ETopQueriesByDurationOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_duration_one_hour", ESysViewType::ETopQueriesByDurationOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_read_bytes_one_minute", ESysViewType::ETopQueriesByReadBytesOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_read_bytes_one_hour", ESysViewType::ETopQueriesByReadBytesOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_cpu_time_one_minute", ESysViewType::ETopQueriesByCpuTimeOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_cpu_time_one_hour", ESysViewType::ETopQueriesByCpuTimeOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_request_units_one_minute", ESysViewType::ETopQueriesByRequestUnitsOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_request_units_one_hour", ESysViewType::ETopQueriesByRequestUnitsOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},

    {"query_sessions", ESysViewType::EQuerySessions, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QuerySessions>},
    {"query_metrics_one_minute", ESysViewType::EQueryMetricsOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryMetrics>},
    // don't have approved schema yet
    // {"compile_cache_queries", ESysViewType::ECompileCacheQueries, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::CompileCacheQueries>},

    {"ds_pdisks", ESysViewType::EPDisks, {ESource::Domain},  &FillSchema<Schema::PDisks>},
    {"ds_vslots", ESysViewType::EVSlots, {ESource::Domain},  &FillSchema<Schema::VSlots>},
    {"ds_groups", ESysViewType::EGroups, {ESource::Domain},  &FillSchema<Schema::Groups>},
    {"ds_storage_pools", ESysViewType::EStoragePools, {ESource::Domain},  &FillSchema<Schema::StoragePools>},
    {"ds_storage_stats", ESysViewType::EStorageStats, {ESource::Domain},  &FillSchema<Schema::StorageStats>},
    {"hive_tablets", ESysViewType::ETablets, {ESource::Domain},  &FillSchema<Schema::Tablets>},

    {"top_partitions_one_minute", ESysViewType::ETopPartitionsByCpuOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::TopPartitions>},
    {"top_partitions_one_hour", ESysViewType::ETopPartitionsByCpuOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::TopPartitions>},
    {"top_partitions_by_tli_one_minute", ESysViewType::ETopPartitionsByTliOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::TopPartitionsTli>},
    {"top_partitions_by_tli_one_hour", ESysViewType::ETopPartitionsByTliOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::TopPartitionsTli>},

    {"resource_pool_classifiers", ESysViewType::EResourcePoolClassifiers, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::ResourcePoolClassifiers>},
    {"resource_pools", ESysViewType::EResourcePools, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::ResourcePools>},

    {"auth_users", ESysViewType::EAuthUsers, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthUsers>},
    {"auth_groups", ESysViewType::EAuthGroups, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthGroups>},
    {"auth_group_members", ESysViewType::EAuthGroupMembers, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthGroupMembers>},
    {"auth_owners", ESysViewType::EAuthOwners, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthOwners>},
    {"auth_permissions", ESysViewType::EAuthPermissions, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthPermissions>},
    {"auth_effective_permissions", ESysViewType::EAuthEffectivePermissions, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthPermissions>},
};

const TVector<SysViewsRegistryRecord> SysViewsRegistry::RewrittenSysViews = {
    {"show_create", ESysViewType::EShowCreate, {},  &FillSchema<Schema::ShowCreate>},
};

SysViewsRegistry::SysViewsRegistry() {
    for (const auto& registryRecord : SysViews) {
        SysViewTypesMap.emplace(registryRecord.Name, registryRecord.Type);
    }

    for (const auto& registryRecord : RewrittenSysViews) {
        SysViewTypesMap.emplace(registryRecord.Name, registryRecord.Type);
    }

    SysViewTypesMap.emplace(PgTablesName, ESysViewType::EPgTables);
    SysViewTypesMap.emplace(InformationSchemaTablesName, ESysViewType::EInformationSchemaTables);
    SysViewTypesMap.emplace(PgClassName, ESysViewType::EPgClass);
}

const SysViewsRegistry Registry;

} // NSysView
} // NKikimr
