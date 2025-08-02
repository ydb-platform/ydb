#include "schema.h"

#include <ydb/core/base/appdata.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

namespace {
    using NKikimrSysView::ESysViewType;
}

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

bool MaybeSystemViewPath(const TVector<TString>& path) {
    auto length = path.size();
    // minimal system view path should be /Root/.sys/view
    // only one level after ".sys" is allowed at the moment
    if (length < 3 || path[length - 2] != SysPathName) {
        return false;
    }
    return true;
}

bool MaybeSystemViewFolderPath(const TVector<TString>& path) {
    if (path.size() < 2 || path.back() != SysPathName) {
        return false;
    }
    return true;
}

template <typename Table>
struct TSchemaFiller {

    using TSchema = ISystemViewResolver::TSchema;

    template <typename...>
    struct TFiller;

    template <typename Column>
    struct TFiller<Column> {
        static void Fill(TSchema& schema) {
            schema.Columns[Column::ColumnId] = TSysTables::TTableColumnInfo(
                Table::template TableColumns<Column>::GetColumnName(),
                Column::ColumnId, NScheme::TTypeInfo(Column::ColumnType), "", -1);
        }
    };

    template <typename Column, typename... Columns>
    struct TFiller<Column, Columns...> {
        static void Fill(TSchema& schema) {
            TFiller<Column>::Fill(schema);
            TFiller<Columns...>::Fill(schema);
        }
    };

    template <typename... Columns>
    using TColumnsType = typename Table::template TableColumns<Columns...>;

    template <typename... Columns>
    static void FillColumns(TSchema& schema, TColumnsType<Columns...>) {
        TFiller<Columns...>::Fill(schema);
    }

    template <typename...>
    struct TKeyFiller;

    template <typename Key>
    struct TKeyFiller<Key> {
        static void Fill(TSchema& schema, i32 index) {
            auto& column = schema.Columns[Key::ColumnId];
            column.KeyOrder = index;
            schema.KeyColumnTypes.push_back(column.PType);
        }
    };

    template <typename Key, typename... Keys>
    struct TKeyFiller<Key, Keys...> {
        static void Fill(TSchema& schema, i32 index) {
            TKeyFiller<Key>::Fill(schema, index);
            TKeyFiller<Keys...>::Fill(schema, index + 1);
        }
    };

    template <typename... Keys>
    using TKeysType = typename Table::template TableKey<Keys...>;

    template <typename... Keys>
    static void FillKeys(TSchema& schema, TKeysType<Keys...>) {
        TKeyFiller<Keys...>::Fill(schema, 0);
    }

    static void Fill(TSchema& schema) {
        FillColumns(schema, typename Table::TColumns());
        FillKeys(schema, typename Table::TKey());
    }
};

class TSystemViewResolver : public ISystemViewResolver {
public:
    TSystemViewResolver() {
        RegisterSystemViews();
    }

    bool IsSystemViewPath(const TVector<TString>& path, TSystemViewPath& sysViewPath) const override final {
        if (MaybeSystemViewFolderPath(path)) {
            TVector<TString> realPath(path.begin(), path.end() - 1);
            sysViewPath.Parent = std::move(realPath);
            sysViewPath.ViewName = SysPathName;
            return true;

        } else if (MaybeSystemViewPath(path)) {
            auto maybeSystemViewName = path.back();
            if (!DomainSystemViews.contains(maybeSystemViewName) &&
                !SubDomainSystemViews.contains(maybeSystemViewName) &&
                !OlapStoreSystemViews.contains(maybeSystemViewName) &&
                !ColumnTableSystemViews.contains(maybeSystemViewName))
            {
                return false;
            }
            TVector<TString> realPath(path.begin(), path.end() - 2);
            sysViewPath.Parent = std::move(realPath);
            sysViewPath.ViewName = path.back();
            return true;
        }
        return false;
    }

    TMaybe<TSchema> GetSystemViewSchema(const TStringBuf viewName, ETarget target) const override final {
        const TSchema* view = nullptr;
        switch (target) {
        case ETarget::Domain:
            view = DomainSystemViews.FindPtr(viewName);
            break;
        case ETarget::SubDomain:
            view = SubDomainSystemViews.FindPtr(viewName);
            break;
        case ETarget::OlapStore:
            view = OlapStoreSystemViews.FindPtr(viewName);
            break;
        case ETarget::ColumnTable:
            view = ColumnTableSystemViews.FindPtr(viewName);
            break;
        }
        return view ? TMaybe<TSchema>(*view) : Nothing();
    }

    TMaybe<TSchema> GetSystemViewSchema(ESysViewType viewType) const override final {
        const TSchema* view = SystemViews.FindPtr(viewType);
        return view ? TMaybe<TSchema>(*view) : Nothing();
    }

    TVector<TString> GetSystemViewNames(ETarget target) const override {
        TVector<TString> result;
        switch (target) {
        case ETarget::Domain:
            result.reserve(DomainSystemViews.size());
            for (const auto& [name, _] : DomainSystemViews) {
                result.push_back(name);
            }
            break;
        case ETarget::SubDomain:
            result.reserve(SubDomainSystemViews.size());
            for (const auto& [name, _] : SubDomainSystemViews) {
                result.push_back(name);
            }
            break;
        case ETarget::OlapStore:
            result.reserve(OlapStoreSystemViews.size());
            for (const auto& [name, _] : OlapStoreSystemViews) {
                result.push_back(name);
            }
            break;
        case ETarget::ColumnTable:
            result.reserve(ColumnTableSystemViews.size());
            for (const auto& [name, _] : ColumnTableSystemViews) {
                result.push_back(name);
            }
            break;
        }
        return result;
    }

    const THashMap<TString, ESysViewType>& GetSystemViewsTypes(ETarget target) const override {
        switch (target) {
        case ETarget::Domain:
            return DomainSystemViewTypes;
        case ETarget::SubDomain:
            return SubDomainSystemViewTypes;
        case ETarget::OlapStore:
            return OlapStoreSystemViewTypes;
        case ETarget::ColumnTable:
            return ColumnTableSystemViewTypes;
        }
    }

    bool IsSystemView(const TStringBuf viewName) const override final {
        return DomainSystemViews.contains(viewName) ||
            SubDomainSystemViews.contains(viewName) ||
            OlapStoreSystemViews.contains(viewName) ||
            ColumnTableSystemViews.contains(viewName);
    }

private:
    void RegisterPgTablesSystemViews() {
        auto registerView = [&](TStringBuf tableName, ESysViewType type, const TVector<Schema::PgColumn>& columns) {
            auto& dsv  = DomainSystemViews[tableName];
            DomainSystemViewTypes[tableName] = type;
            auto& sdsv = SubDomainSystemViews[tableName];
            SubDomainSystemViewTypes[tableName] = type;
            auto& sv = SystemViews[type];
            for (const auto& column : columns) {
                dsv.Columns[column._ColumnId + 1] = TSysTables::TTableColumnInfo(
                    column._ColumnName, column._ColumnId + 1, column._ColumnTypeInfo, "", -1
                );
                sdsv.Columns[column._ColumnId + 1] = TSysTables::TTableColumnInfo(
                    column._ColumnName, column._ColumnId + 1, column._ColumnTypeInfo, "", -1
                );
                sv.Columns[column._ColumnId + 1] = TSysTables::TTableColumnInfo(
                    column._ColumnName, column._ColumnId + 1, column._ColumnTypeInfo, "", -1
                );
            }
        };
        registerView(
            PgTablesName,
            ESysViewType::EPgTables,
            Singleton<Schema::PgTablesSchemaProvider>()->GetColumns(PgTablesName)
        );
        registerView(
            InformationSchemaTablesName,
            ESysViewType::EInformationSchemaTables,
            Singleton<Schema::PgTablesSchemaProvider>()->GetColumns(InformationSchemaTablesName)
        );
        registerView(
            PgClassName,
            ESysViewType::EPgClass,
            Singleton<Schema::PgTablesSchemaProvider>()->GetColumns(PgClassName)
        );
    }

    template <typename Table>
    void RegisterSystemView(const TStringBuf& name, ESysViewType type) {
        TSchemaFiller<Table>::Fill(DomainSystemViews[name]);
        DomainSystemViewTypes[name] = type;
        TSchemaFiller<Table>::Fill(SubDomainSystemViews[name]);
        SubDomainSystemViewTypes[name] = type;
        TSchemaFiller<Table>::Fill(SystemViews[type]);
    }

    template <typename Table>
    void RegisterDomainSystemView(const TStringBuf& name, ESysViewType type) {
        TSchemaFiller<Table>::Fill(DomainSystemViews[name]);
        DomainSystemViewTypes[name] = type;
        TSchemaFiller<Table>::Fill(SystemViews[type]);
    }

    template <typename Table>
    void RegisterOlapStoreSystemView(const TStringBuf& name) {
        TSchemaFiller<Table>::Fill(OlapStoreSystemViews[name]);
    }

    template <typename Table>
    void RegisterColumnTableSystemView(const TStringBuf& name) {
        TSchemaFiller<Table>::Fill(ColumnTableSystemViews[name]);
    }

    void RegisterSystemViews() {
        RegisterSystemView<Schema::PartitionStats>(PartitionStatsName, ESysViewType::EPartitionStats);

        RegisterSystemView<Schema::Nodes>(NodesName, ESysViewType::ENodes);

        RegisterSystemView<Schema::QueryStats>(TopQueriesByDuration1MinuteName, ESysViewType::ETopQueriesByDurationOneMinute);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByDuration1HourName, ESysViewType::ETopQueriesByDurationOneHour);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByReadBytes1MinuteName, ESysViewType::ETopQueriesByReadBytesOneMinute);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByReadBytes1HourName, ESysViewType::ETopQueriesByReadBytesOneHour);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByCpuTime1MinuteName, ESysViewType::ETopQueriesByCpuTimeOneMinute);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByCpuTime1HourName, ESysViewType::ETopQueriesByCpuTimeOneHour);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByRequestUnits1MinuteName, ESysViewType::ETopQueriesByRequestUnitsOneMinute);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByRequestUnits1HourName, ESysViewType::ETopQueriesByRequestUnitsOneHour);
        RegisterSystemView<Schema::QuerySessions>(QuerySessions, ESysViewType::EQuerySessions);
        RegisterSystemView<Schema::CompileCacheQueries>(CompileCacheQueries, ESysViewType::ECompileCacheQueries);

        RegisterDomainSystemView<Schema::PDisks>(PDisksName, ESysViewType::EPDisks);
        RegisterDomainSystemView<Schema::VSlots>(VSlotsName, ESysViewType::EVSlots);
        RegisterDomainSystemView<Schema::Groups>(GroupsName, ESysViewType::EGroups);
        RegisterDomainSystemView<Schema::StoragePools>(StoragePoolsName, ESysViewType::EStoragePools);
        RegisterDomainSystemView<Schema::StorageStats>(StorageStatsName, ESysViewType::EStorageStats);

        RegisterDomainSystemView<Schema::Tablets>(TabletsName, ESysViewType::ETablets);

        RegisterSystemView<Schema::QueryMetrics>(QueryMetricsName, ESysViewType::EQueryMetricsOneMinute);

        RegisterOlapStoreSystemView<Schema::PrimaryIndexStats>(StorePrimaryIndexStatsName);
        RegisterOlapStoreSystemView<Schema::PrimaryIndexSchemaStats>(StorePrimaryIndexSchemaStatsName);
        RegisterOlapStoreSystemView<Schema::PrimaryIndexPortionStats>(StorePrimaryIndexPortionStatsName);
        RegisterOlapStoreSystemView<Schema::PrimaryIndexGranuleStats>(StorePrimaryIndexGranuleStatsName);
        RegisterOlapStoreSystemView<Schema::PrimaryIndexOptimizerStats>(StorePrimaryIndexOptimizerStatsName);
        RegisterColumnTableSystemView<Schema::PrimaryIndexStats>(TablePrimaryIndexStatsName);
        RegisterColumnTableSystemView<Schema::PrimaryIndexSchemaStats>(TablePrimaryIndexSchemaStatsName);
        RegisterColumnTableSystemView<Schema::PrimaryIndexPortionStats>(TablePrimaryIndexPortionStatsName);
        RegisterColumnTableSystemView<Schema::PrimaryIndexGranuleStats>(TablePrimaryIndexGranuleStatsName);
        RegisterColumnTableSystemView<Schema::PrimaryIndexOptimizerStats>(TablePrimaryIndexOptimizerStatsName);

        RegisterSystemView<Schema::TopPartitions>(TopPartitionsByCpu1MinuteName, ESysViewType::ETopPartitionsByCpuOneMinute);
        RegisterSystemView<Schema::TopPartitions>(TopPartitionsByCpu1HourName, ESysViewType::ETopPartitionsByCpuOneHour);
        RegisterSystemView<Schema::TopPartitionsTli>(TopPartitionsByTli1MinuteName, ESysViewType::ETopPartitionsByTliOneMinute);
        RegisterSystemView<Schema::TopPartitionsTli>(TopPartitionsByTli1HourName, ESysViewType::ETopPartitionsByTliOneHour);

        RegisterPgTablesSystemViews();

        RegisterSystemView<Schema::ResourcePoolClassifiers>(ResourcePoolClassifiersName, ESysViewType::EResourcePoolClassifiers);
        RegisterSystemView<Schema::ResourcePools>(ResourcePoolsName, ESysViewType::EResourcePools);

        {
            using namespace NAuth;
            RegisterSystemView<Schema::AuthUsers>(UsersName, ESysViewType::EAuthUsers);
            RegisterSystemView<Schema::AuthGroups>(NAuth::GroupsName, ESysViewType::EAuthGroups);
            RegisterSystemView<Schema::AuthGroupMembers>(GroupMembersName, ESysViewType::EAuthGroupMembers);
            RegisterSystemView<Schema::AuthOwners>(OwnersName, ESysViewType::EAuthOwners);
            RegisterSystemView<Schema::AuthPermissions>(PermissionsName, ESysViewType::EAuthPermissions);
            RegisterSystemView<Schema::AuthPermissions>(EffectivePermissionsName, ESysViewType::EAuthEffectivePermissions);
        }
    }

private:
    THashMap<TString, TSchema> DomainSystemViews;
    THashMap<TString, ESysViewType> DomainSystemViewTypes;
    THashMap<TString, TSchema> SubDomainSystemViews;
    THashMap<TString, ESysViewType> SubDomainSystemViewTypes;
    THashMap<TString, TSchema> OlapStoreSystemViews;
    THashMap<TString, ESysViewType> OlapStoreSystemViewTypes;
    THashMap<TString, TSchema> ColumnTableSystemViews;
    THashMap<TString, ESysViewType> ColumnTableSystemViewTypes;
    THashMap<ESysViewType, TSchema> SystemViews;
};

class TSystemViewRewrittenResolver : public ISystemViewResolver {
public:

    TSystemViewRewrittenResolver() {
        TSchemaFiller<Schema::ShowCreate>::Fill(SystemViews[ShowCreateName]);
    }

    bool IsSystemViewPath(const TVector<TString>& path, TSystemViewPath& sysViewPath) const override final {
        if (MaybeSystemViewPath(path)) {
            auto maybeSystemViewName = path.back();
            if (!SystemViews.contains(maybeSystemViewName)) {
                return false;
            }
            TVector<TString> realPath(path.begin(), path.end() - 2);
            sysViewPath.Parent = std::move(realPath);
            sysViewPath.ViewName = path.back();
            return true;
        }
        return false;
    }

    TMaybe<TSchema> GetSystemViewSchema(const TStringBuf viewName, ETarget target) const override final {
        Y_UNUSED(target);
        const TSchema* view = SystemViews.FindPtr(viewName);
        return view ? TMaybe<TSchema>(*view) : Nothing();
    }

    TMaybe<TSchema> GetSystemViewSchema(ESysViewType sysViewType) const override final {
        Y_UNUSED(sysViewType);
        return Nothing();
    }

    TVector<TString> GetSystemViewNames(ETarget target) const override {
        Y_UNUSED(target);
        return {};
    }

    const THashMap<TString, ESysViewType>& GetSystemViewsTypes(ETarget target) const override {
        Y_UNUSED(target);
        return SystemViewTypes;
    }

    bool IsSystemView(const TStringBuf viewName) const override final {
        return SystemViews.contains(viewName);
    }

private:
    THashMap<TString, TSchema> SystemViews;
    THashMap<TString, ESysViewType> SystemViewTypes;
};

THolder<ISystemViewResolver> CreateSystemViewResolver() {
    return MakeHolder<TSystemViewResolver>();
}

THolder<ISystemViewResolver> CreateSystemViewRewrittenResolver() {
    return MakeHolder<TSystemViewRewrittenResolver>();
}

} // NSysView
} // NKikimr
