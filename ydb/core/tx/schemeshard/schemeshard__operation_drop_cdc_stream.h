#pragma once

#include "schemeshard__operation_common.h"
#include "schemeshard__operation_create_cdc_stream.h" // for TStreamPaths
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard::NCdc {

std::variant<TStreamPaths, ISubOperation::TPtr> DoDropStreamPathChecks(
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TString& tableName,
    const TString& streamName);

ISubOperation::TPtr DoDropStreamChecks(
    const TOperationId& opId,
    const TPath& tablePath,
    const TTxId lockTxId,
    TOperationContext& context);

void DoDropStream(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TDropCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath,
    const TPath& streamPath,
    const TTxId lockTxId,
    TOperationContext& context);

} // namespace NKikimr::NSchemesShard::NCdc
