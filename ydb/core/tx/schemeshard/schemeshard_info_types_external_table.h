#pragma once

#include "schemeshard_info_types_table.h"

namespace NKikimr {
namespace NSchemeShard {

struct TExternalTableInfo: TSimpleRefCount<TExternalTableInfo> {
    using TPtr = TIntrusivePtr<TExternalTableInfo>;

    TString SourceType;
    TString DataSourcePath;
    TString Location;
    ui64 AlterVersion = 0;
    THashMap<ui32, TTableInfo::TColumn> Columns;
    TString Content;
};

}
}
