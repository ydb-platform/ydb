#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard::NCdc {

struct TStreamPaths {
    TPath TablePath;
    TPath StreamPath;
};

std::variant<TStreamPaths, ISubOperation::TPtr> DoNewStreamPathChecks(
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TString& tableName,
    const TString& streamName,
    bool acceptExisted);

void DoCreateStream(
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath,
    const bool acceptExisted,
    const bool initialScan,
    const TString& indexName,
    TVector<ISubOperation::TPtr>& result);

void DoCreatePqPart(
    const TOperationId& opId,
    const TPath& streamPath,
    const TString& streamName,
    const TIntrusivePtr<TTableInfo> table,
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TVector<TString>& boundaries,
    const bool acceptExisted,
    TVector<ISubOperation::TPtr>& result);

} // namespace NKikimr::NSchemesShard::NCdc
