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
            if (!DomainSystemViewsTypes.contains(maybeSystemViewName) &&
                !SubDomainSystemViewsTypes.contains(maybeSystemViewName) &&
                !OlapStoreSystemViewsTypes.contains(maybeSystemViewName) &&
                !ColumnTableSystemViewsTypes.contains(maybeSystemViewName))
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

    TMaybe<TSchema> GetSystemViewSchema(ESysViewType viewType) const override final {
        const TSchema* view = SystemViews.FindPtr(viewType);
        return view ? TMaybe<TSchema>(*view) : Nothing();
    }

    TMaybe<NKikimrSysView::ESysViewType> GetSystemViewType(const TStringBuf viewName, TMaybe<ESource> sourceObjectType) const override final {
        if (!sourceObjectType) {
            return Nothing();
        }

        TMaybe<THashMap<TStringBuf, ESysViewType>> sysViewsTypes;
        switch (*sourceObjectType) {
        case ESource::Domain:
            sysViewsTypes = DomainSystemViewsTypes;
            break;
        case ESource::SubDomain:
            sysViewsTypes = SubDomainSystemViewsTypes;
            break;
        case ESource::OlapStore:
            sysViewsTypes = OlapStoreSystemViewsTypes;
            break;
        case ESource::ColumnTable:
            sysViewsTypes = ColumnTableSystemViewsTypes;
            break;
        }

        if (const auto it = sysViewsTypes->find(viewName); it != sysViewsTypes->end()) {
            return it->second;
        } else {
            return Nothing();
        }
    }

    const THashMap<TStringBuf, ESysViewType>& GetSystemViewsTypes(ESource sourceObjectType) const override final {
        switch (sourceObjectType) {
        case ESource::Domain:
            return DomainSystemViewsTypes;
        case ESource::SubDomain:
            return SubDomainSystemViewsTypes;
        case ESource::OlapStore:
            return OlapStoreSystemViewsTypes;
        case ESource::ColumnTable:
            return ColumnTableSystemViewsTypes;
        }
    }

private:
    void RegisterPgTablesSystemViews() {
        auto registerView = [&](TStringBuf tableName, ESysViewType type, const TVector<Schema::PgColumn>& columns) {
            DomainSystemViewsTypes[tableName] = type;
            SubDomainSystemViewsTypes[tableName] = type;
            auto& sv = SystemViews[type];
            for (const auto& column : columns) {
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

    void RegisterSystemViews() {
        for (const auto& registryRecord : SysViewsRegistry::SysViews) {
            registryRecord.FillSchemaFunc(SystemViews[registryRecord.Type]);

            for (const auto sourceObjectType : registryRecord.SourceObjectTypes) {
                switch (sourceObjectType) {
                case ESource::Domain: {
                    DomainSystemViewsTypes[registryRecord.Name] = registryRecord.Type;
                    break;
                };
                case ESource::SubDomain: {
                    SubDomainSystemViewsTypes[registryRecord.Name] = registryRecord.Type;
                    break;
                };
                case ESource::OlapStore: {
                    OlapStoreSystemViewsTypes[registryRecord.Name] = registryRecord.Type;
                    break;
                };
                case ESource::ColumnTable: {
                    ColumnTableSystemViewsTypes[registryRecord.Name] = registryRecord.Type;
                    break;
                };
                }
            }
        }

        RegisterPgTablesSystemViews();
    }

private:
    THashMap<TStringBuf, ESysViewType> DomainSystemViewsTypes;
    THashMap<TStringBuf, ESysViewType> SubDomainSystemViewsTypes;
    THashMap<TStringBuf, ESysViewType> OlapStoreSystemViewsTypes;
    THashMap<TStringBuf, ESysViewType> ColumnTableSystemViewsTypes;
    THashMap<ESysViewType, TSchema> SystemViews;
};

class TSystemViewRewrittenResolver : public ISystemViewResolver {
public:

    TSystemViewRewrittenResolver() {
        for (const auto& registryRecord : SysViewsRegistry::RewrittenSysViews) {
            Y_ENSURE(registryRecord.SourceObjectTypes.empty());
            registryRecord.FillSchemaFunc(SystemViews[registryRecord.Type]);
            SystemViewsTypes[registryRecord.Name] = registryRecord.Type;
        }
    }

    bool IsSystemViewPath(const TVector<TString>& path, TSystemViewPath& sysViewPath) const override final {
        if (MaybeSystemViewPath(path)) {
            auto maybeSystemViewName = path.back();
            if (!SystemViewsTypes.contains(maybeSystemViewName)) {
                return false;
            }
            TVector<TString> realPath(path.begin(), path.end() - 2);
            sysViewPath.Parent = std::move(realPath);
            sysViewPath.ViewName = path.back();
            return true;
        }
        return false;
    }

    TMaybe<TSchema> GetSystemViewSchema(ESysViewType viewType) const override final {
        const TSchema* view = SystemViews.FindPtr(viewType);
        return view ? TMaybe<TSchema>(*view) : Nothing();
    }

    TMaybe<NKikimrSysView::ESysViewType> GetSystemViewType(const TStringBuf viewName, TMaybe<ESource> sourceObjectType) const override final{
        if (sourceObjectType) {
            return Nothing();
        }

        if (const auto it = SystemViewsTypes.find(viewName); it != SystemViewsTypes.end()) {
            return it->second;
        } else {
            return Nothing();
        }
    }

    const THashMap<TStringBuf, ESysViewType>& GetSystemViewsTypes(ESource) const override final {
        return SystemViewsTypes;
    }

private:
    THashMap<TStringBuf, ESysViewType> SystemViewsTypes;
    THashMap<ESysViewType, TSchema> SystemViews;
};

const ISystemViewResolver& GetSystemViewResolver() {
    return *Singleton<TSystemViewResolver>();
}

const ISystemViewResolver& GetSystemViewRewrittenResolver() {
    return *Singleton<TSystemViewRewrittenResolver>();
}

} // NSysView
} // NKikimr
