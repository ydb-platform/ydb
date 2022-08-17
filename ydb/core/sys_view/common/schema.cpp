#include "schema.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NSysView {

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

private:
    template <typename Table>
    struct TSchemaFiller {

        template <typename...>
        struct TFiller;

        template <typename Column>
        struct TFiller<Column> {
            static void Fill(TSchema& schema) {
                schema.Columns[Column::ColumnId] = TSysTables::TTableColumnInfo(
                    Table::template TableColumns<Column>::GetColumnName(),
                    Column::ColumnId, Column::ColumnType, -1);
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

    template <typename Table>
    void RegisterSystemView(const TStringBuf& name) {
        TSchemaFiller<Table>::Fill(DomainSystemViews[name]);
        TSchemaFiller<Table>::Fill(SubDomainSystemViews[name]);
    }

    template <typename Table>
    void RegisterDomainSystemView(const TStringBuf& name) {
        TSchemaFiller<Table>::Fill(DomainSystemViews[name]);
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
        RegisterSystemView<Schema::PartitionStats>(PartitionStatsName);

        // 'nodes' table is currently switched off
        // RegisterSystemView<Schema::Nodes>(NodesName);

        RegisterSystemView<Schema::QueryStats>(TopQueriesByDuration1MinuteName);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByDuration1HourName);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByReadBytes1MinuteName);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByReadBytes1HourName);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByCpuTime1MinuteName);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByCpuTime1HourName);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByRequestUnits1MinuteName);
        RegisterSystemView<Schema::QueryStats>(TopQueriesByRequestUnits1HourName);

        RegisterDomainSystemView<Schema::PDisks>(PDisksName);
        RegisterDomainSystemView<Schema::VSlots>(VSlotsName);
        RegisterDomainSystemView<Schema::Groups>(GroupsName);
        RegisterDomainSystemView<Schema::StoragePools>(StoragePoolsName);
        RegisterDomainSystemView<Schema::StorageStats>(StorageStatsName);

        RegisterDomainSystemView<Schema::Tablets>(TabletsName);

        RegisterSystemView<Schema::QueryMetrics>(QueryMetricsName);

        RegisterOlapStoreSystemView<Schema::PrimaryIndexStats>(StorePrimaryIndexStatsName);
        RegisterColumnTableSystemView<Schema::PrimaryIndexStats>(TablePrimaryIndexStatsName);

        RegisterSystemView<Schema::TopPartitions>(TopPartitions1MinuteName);
        RegisterSystemView<Schema::TopPartitions>(TopPartitions1HourName);
    }

private:
    THashMap<TString, TSchema> DomainSystemViews;
    THashMap<TString, TSchema> SubDomainSystemViews;
    THashMap<TString, TSchema> OlapStoreSystemViews;
    THashMap<TString, TSchema> ColumnTableSystemViews;
};

ISystemViewResolver* CreateSystemViewResolver() {
    return new TSystemViewResolver();
}

} // NSysView
} // NKikimr
