#pragma once

#include "schemeshard__operation_part.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard::NCdc {

void DoRotateStream(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TRotateCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath);

} // namespace NKikimr::NSchemesShard::NCdc
