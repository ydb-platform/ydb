#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard::NCdc {

void DoAlterStream(
    const NKikimrSchemeOp::TAlterCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath,
    TVector<ISubOperation::TPtr>& result);

} // namespace NKikimr::NSchemesShard::NCdc
