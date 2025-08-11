#include "resolver.h"

#include <ydb/core/sys_view/common/path.h>
#include <ydb/core/sys_view/common/registry.h>

using NKikimrSysView::ESysViewType;

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

    TMaybe<TSchema> GetSystemViewSchema(const TStringBuf viewName, ESource sourceObjectType) const override final {
        const TSchema* view = nullptr;
        switch (sourceObjectType) {
        case ESource::Domain:
            view = DomainSystemViews.FindPtr(viewName);
            break;
        case ESource::SubDomain:
            view = SubDomainSystemViews.FindPtr(viewName);
            break;
        case ESource::OlapStore:
            view = OlapStoreSystemViews.FindPtr(viewName);
            break;
        case ESource::ColumnTable:
            view = ColumnTableSystemViews.FindPtr(viewName);
            break;
        }
        return view ? TMaybe<TSchema>(*view) : Nothing();
    }

    TMaybe<TSchema> GetSystemViewSchema(ESysViewType viewType) const override final {
        const TSchema* view = SystemViews.FindPtr(viewType);
        return view ? TMaybe<TSchema>(*view) : Nothing();
    }

    TVector<TString> GetSystemViewNames(ESource sourceObjectType) const override {
        TVector<TString> result;
        switch (sourceObjectType) {
        case ESource::Domain:
            result.reserve(DomainSystemViews.size());
            for (const auto& [name, _] : DomainSystemViews) {
                result.push_back(name);
            }
            break;
        case ESource::SubDomain:
            result.reserve(SubDomainSystemViews.size());
            for (const auto& [name, _] : SubDomainSystemViews) {
                result.push_back(name);
            }
            break;
        case ESource::OlapStore:
            result.reserve(OlapStoreSystemViews.size());
            for (const auto& [name, _] : OlapStoreSystemViews) {
                result.push_back(name);
            }
            break;
        case ESource::ColumnTable:
            result.reserve(ColumnTableSystemViews.size());
            for (const auto& [name, _] : ColumnTableSystemViews) {
                result.push_back(name);
            }
            break;
        }
        return result;
    }

    const THashMap<TString, ESysViewType>& GetSystemViewsTypes(ESource sourceObjectType) const override {
        switch (sourceObjectType) {
        case ESource::Domain:
            return DomainSystemViewTypes;
        case ESource::SubDomain:
            return SubDomainSystemViewTypes;
        case ESource::OlapStore:
            return OlapStoreSystemViewTypes;
        case ESource::ColumnTable:
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
    void RegisterOlapStoreSystemView(const TStringBuf& name) {
        TSchemaFiller<Table>::Fill(OlapStoreSystemViews[name]);
    }

    template <typename Table>
    void RegisterColumnTableSystemView(const TStringBuf& name) {
        TSchemaFiller<Table>::Fill(ColumnTableSystemViews[name]);
    }

    void RegisterSystemViews() {
        for (const auto& registryRecord : SysViewsRegistry::SysViews) {
            TSchema schema;
            registryRecord.FillSchemaFunc(schema);
            SystemViews[registryRecord.Type] = schema;

            for (const auto sourceObjectType : registryRecord.SourceObjectTypes) {
                switch (sourceObjectType) {
                case ESource::Domain: {
                    DomainSystemViews[registryRecord.Name] = schema;
                    DomainSystemViewTypes[registryRecord.Name] = registryRecord.Type;
                    break;
                };
                case ESource::SubDomain: {
                    SubDomainSystemViews[registryRecord.Name] = schema;
                    SubDomainSystemViewTypes[registryRecord.Name] = registryRecord.Type;
                    break;
                };
                case ESource::OlapStore: {
                    OlapStoreSystemViews[registryRecord.Name] = schema;
                    break;
                };
                case ESource::ColumnTable: {
                    ColumnTableSystemViews[registryRecord.Name] = schema;
                    break;
                };
                }
            }
        }

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

        RegisterPgTablesSystemViews();
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
        for (const auto& registryRecord : SysViewsRegistry::RewrittenSysViews) {
            TSchema schema;
            registryRecord.FillSchemaFunc(schema);
            SystemViews[registryRecord.Type] = std::move(schema);
            SystemViewTypes[registryRecord.Name] = registryRecord.Type;
        }
    }

    bool IsSystemViewPath(const TVector<TString>& path, TSystemViewPath& sysViewPath) const override final {
        if (MaybeSystemViewPath(path)) {
            auto maybeSystemViewName = path.back();
            if (!SystemViewTypes.contains(maybeSystemViewName)) {
                return false;
            }
            TVector<TString> realPath(path.begin(), path.end() - 2);
            sysViewPath.Parent = std::move(realPath);
            sysViewPath.ViewName = path.back();
            return true;
        }
        return false;
    }

    TMaybe<TSchema> GetSystemViewSchema(const TStringBuf viewName, ESource sourceObjectType) const override final {
        Y_UNUSED(viewName);
        Y_UNUSED(sourceObjectType);
        return Nothing();
    }

    TMaybe<TSchema> GetSystemViewSchema(ESysViewType viewType) const override final {
        const TSchema* view = SystemViews.FindPtr(viewType);
        return view ? TMaybe<TSchema>(*view) : Nothing();
    }

    TVector<TString> GetSystemViewNames(ESource sourceObjectType) const override {
        Y_UNUSED(sourceObjectType);
        return {};
    }

    const THashMap<TString, ESysViewType>& GetSystemViewsTypes(ESource sourceObjectType) const override {
        Y_UNUSED(sourceObjectType);
        return SystemViewTypes;
    }

    bool IsSystemView(const TStringBuf viewName) const override final {
        return SystemViewTypes.contains(viewName);
    }

private:
    THashMap<ESysViewType, TSchema> SystemViews;
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
