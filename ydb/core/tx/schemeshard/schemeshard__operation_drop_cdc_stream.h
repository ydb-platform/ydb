#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard {

void DoDropStream(
    const NKikimrSchemeOp::TDropCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath,
    const TPath& streamPath,
    const TTxId lockTxId,
    TOperationContext& context,
    TVector<ISubOperation::TPtr>& result);

} // namespace NKikimr::NSchemesShard
