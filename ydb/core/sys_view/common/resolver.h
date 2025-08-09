#pragma once

#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/locks/sys_tables.h>

namespace NKikimr {
namespace NSysView {

class ISystemViewResolver {
public:
    virtual ~ISystemViewResolver() = default;

    enum class ESource : ui8 {
        Domain,
        SubDomain,
        OlapStore,
        ColumnTable
    };

    struct TSystemViewPath {
        TVector<TString> Parent;
        TString ViewName;
    };

    struct TSchema {
        THashMap<NTable::TTag, TSysTables::TTableColumnInfo> Columns;
        TVector<NScheme::TTypeInfo> KeyColumnTypes;
    };

    virtual bool IsSystemViewPath(const TVector<TString>& path, TSystemViewPath& sysViewPath) const = 0;

    virtual TMaybe<TSchema> GetSystemViewSchema(const TStringBuf viewName, ESource sourceObjectType) const = 0;

    virtual TMaybe<TSchema> GetSystemViewSchema(NKikimrSysView::ESysViewType viewType) const = 0;

    virtual bool IsSystemView(const TStringBuf viewName) const = 0;

    virtual TVector<TString> GetSystemViewNames(ESource target) const = 0;

    virtual const THashMap<TString, NKikimrSysView::ESysViewType>& GetSystemViewsTypes(ESource target) const = 0;
};

bool MaybeSystemViewPath(const TVector<TString>& path);
bool MaybeSystemViewFolderPath(const TVector<TString>& path);

THolder<ISystemViewResolver> CreateSystemViewResolver();

THolder<ISystemViewResolver> CreateSystemViewRewrittenResolver();

} // NSysView
} // NKikimr
