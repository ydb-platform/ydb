#pragma once

#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard::NCdc {

struct TStreamPaths {
    TPath TablePath;
    TPath StreamPath;
};

std::variant<TStreamPaths, ISubOperation::TPtr> DoNewStreamPathChecks(
    const TOperationContext& context,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TString& tableName,
    const TString& streamName,
    bool acceptExisted,
    bool restore = false);

void DoCreateStreamImpl(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TOperationId& opId,
    const TPath& tablePath,
    const bool acceptExisted,
    const bool initialScan);

void DoCreateStream(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TOperationId& opId,
    const TPath& workingDirPath,
    const TPath& tablePath,
    const bool acceptExisted,
    const bool initialScan);

struct TCdcPqPartParams {
    ui32 TotalGroupCount = 0;
    ui32 PartitionPerTablet = 2;
    bool ReplicationAutoPartitioning = false;
    ui32 MinPartitionCount = 0;
    ui32 MaxPartitionCount = 0;
};

// Decides the changefeed topic shape from the source table size.
// Replication autopartitioning caps max at maxShardsInPath and min at that limit / 4.
// An explicit TopicPartitions stays the initial count and the strategy minimum, capped by that max.
TCdcPqPartParams MakeCdcPqPartParams(const NKikimrSchemeOp::TCreateCdcStream& op, ui64 tablePartitionCount, ui64 maxShardsInPath);

void DoCreatePqPart(
    TVector<ISubOperation::TPtr>& result,
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TOperationId& opId,
    const TPath& streamPath,
    const TString& streamName,
    TTableInfo::TCPtr table,
    const TVector<TString>& boundaries,
    const bool acceptExisted);

} // namespace NKikimr::NSchemesShard::NCdc
