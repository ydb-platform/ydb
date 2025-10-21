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

    virtual TMaybe<TSchema> GetSystemViewSchema(NKikimrSysView::ESysViewType viewType) const = 0;

    virtual TMaybe<NKikimrSysView::ESysViewType> GetSystemViewType(const TStringBuf viewName, TMaybe<ESource> sourceObjectType = Nothing()) const = 0;

    virtual const THashMap<TStringBuf, NKikimrSysView::ESysViewType>& GetSystemViewsTypes(ESource source = ESource::Domain) const = 0;
};

bool MaybeSystemViewPath(const TVector<TString>& path);
bool MaybeSystemViewFolderPath(const TVector<TString>& path);

const ISystemViewResolver& GetSystemViewResolver();

const ISystemViewResolver& GetSystemViewRewrittenResolver();

} // NSysView
} // NKikimr
