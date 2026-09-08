#pragma once

#include "schemeshard_info_types_core.h"

namespace NKikimr::NSchemeShard {

inline bool IsVirtualGeneratedColumn(const TTableColumn& column) {
    if (column.DefaultKind != ETableColumnDefaultKind::FromExpression) {
        return false;
    }

    NKikimrSchemeOp::TDefaultExpressionColumnDescription generatedDesc;
    return generatedDesc.ParseFromString(column.DefaultValue) && !generatedDesc.GetStored();
}

} // namespace NKikimr::NSchemeShard
