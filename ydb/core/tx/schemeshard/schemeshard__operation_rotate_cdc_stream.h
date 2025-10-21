#pragma once

#include "schemeshard__operation_part.h"

namespace NKikimr::NSchemeShard::NCdc {

void DoRotateStream(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TRotateCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath);

} // namespace NKikimr::NSchemesShard::NCdc
