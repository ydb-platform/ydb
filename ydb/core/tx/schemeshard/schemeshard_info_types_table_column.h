#pragma once

#include "schemeshard_types.h"

#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/flat_table_column.h>

namespace NKikimr::NSchemeShard {

struct TTableColumn : public NTable::TColumn {
    ui64 CreateVersion;
    ui64 DeleteVersion;
    ETableColumnDefaultKind DefaultKind = ETableColumnDefaultKind::None;
    TString DefaultValue;
    bool IsBuildInProgress = false;
    bool SetNotNullInProgress = false;

    TTableColumn(const TString& name, ui32 id, NScheme::TTypeInfo type, const TString& typeMod, bool notNull)
        : NTable::TScheme::TColumn(name, id, type, typeMod, notNull)
        , CreateVersion(0)
        , DeleteVersion(Max<ui64>())
    {}

    TTableColumn()
        : NTable::TScheme::TColumn()
        , CreateVersion(0)
        , DeleteVersion(Max<ui64>())
    {}

    bool IsKey() const { return KeyOrder != Max<ui32>(); }
    bool IsDropped() const { return DeleteVersion != Max<ui64>(); }
};

} // namespace NKikimr::NSchemeShard
