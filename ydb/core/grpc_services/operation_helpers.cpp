#include "operation_helpers.h"
#include "rpc_calls.h"

#include "rpc_export_base.h"
#include "rpc_import_base.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>

#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/protos/forced_compaction.pb.h>
<<<<<<< HEAD
#include <ydb/core/protos/analyze_operation.pb.h>
#include <ydb/core/util/ulid.h>
=======
#include <ydb/core/protos/set_column_constraint.pb.h>
>>>>>>> 22f73d6fa08 (add in grpc)

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NGRpcService {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)

TActorId CreatePipeClient(ui64 id, const TActorContext& ctx) {
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {.RetryLimitCount = 3};
    return ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, id, clientConfig));
}

Ydb::TOperationId ToOperationId(const NKikimrIndexBuilder::TIndexBuild& build) {
    Ydb::TOperationId operationId;
    operationId.SetKind(Ydb::TOperationId::BUILD_INDEX);
    NOperationId::AddOptionalValue(operationId, "id", ToString(build.GetId()));

    return operationId;
}

Ydb::TOperationId ToOperationId(const NKikimrForcedCompaction::TForcedCompaction& compaction) {
    Ydb::TOperationId operationId;
    operationId.SetKind(Ydb::TOperationId::COMPACTION);
    NOperationId::AddOptionalValue(operationId, "id", ToString(compaction.GetId()));

    return operationId;
}

void ToOperation(const NKikimrIndexBuilder::TIndexBuild& build, Ydb::Operations::Operation* operation) {
    operation->set_id(NOperationId::ProtoToString(ToOperationId(build)));
    operation->mutable_issues()->CopyFrom(build.GetIssues());
    if (build.HasStartTime()) {
        *operation->mutable_create_time() = build.GetStartTime();
    }
    if (build.HasEndTime()) {
        *operation->mutable_end_time() = build.GetEndTime();
    }
    if (build.HasUserSID()) {
        operation->set_created_by(build.GetUserSID());
    }

    switch (build.GetState()) {
        case Ydb::Table::IndexBuildState::STATE_DONE:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
        break;
        case Ydb::Table::IndexBuildState::STATE_CANCELLED:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::CANCELLED);
        break;
        case Ydb::Table::IndexBuildState::STATE_REJECTED:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::ABORTED);
        break;
        default:
            operation->set_ready(false);
    }

    Ydb::Table::IndexBuildMetadata metadata;
    metadata.set_state(build.GetState());
    metadata.set_progress(build.GetProgress());
    auto desc = metadata.mutable_description();
    desc->set_path(build.GetSettings().source_path());
    desc->mutable_index()->CopyFrom(build.GetSettings().index());

    auto data = operation->mutable_metadata();
    data->PackFrom(metadata);
}

void ToOperation(const NKikimrForcedCompaction::TForcedCompaction& compaction, Ydb::Operations::Operation* operation) {
    operation->set_id(NOperationId::ProtoToString(ToOperationId(compaction)));
    if (compaction.HasStartTime()) {
        *operation->mutable_create_time() = compaction.GetStartTime();
    }
    if (compaction.HasEndTime()) {
        *operation->mutable_end_time() = compaction.GetEndTime();
    }
    if (compaction.HasUserSID()) {
        operation->set_created_by(compaction.GetUserSID());
    }

    switch (compaction.GetState()) {
        case Ydb::Table::CompactState::STATE_DONE: {
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            break;
        }
        case Ydb::Table::CompactState::STATE_CANCELLED: {
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::CANCELLED);
            break;
        }
        default: {
            operation->set_ready(false);
            break;
        }
    }

    Ydb::Table::CompactMetadata metadata;
    metadata.set_path(compaction.GetSettings().source_path());
    metadata.set_cascade(compaction.GetSettings().cascade());
    metadata.set_max_shards_in_flight(compaction.GetSettings().max_shards_in_flight());
    metadata.set_progress(compaction.GetProgress());
    metadata.set_state(compaction.GetState());
    metadata.set_shards_total(compaction.GetShardsTotal());
    metadata.set_shards_done(compaction.GetShardsDone());

    auto data = operation->mutable_metadata();
    data->PackFrom(metadata);
}

Ydb::TOperationId ToOperationId(const NKikimrAnalyzeOp::TAnalyzeOperation& op) {
    Ydb::TOperationId operationId;
    operationId.SetKind(Ydb::TOperationId::ANALYZE);
    const auto& binId = op.GetOperationId();
    if (binId.size() == 16) {
        TULID ulid = TULID::FromBinary(binId);
        NOperationId::AddOptionalValue(operationId, "id", ulid.ToString());
    } else {
        // Emit an empty "id" value and let the higher layer surface NOT_FOUND
        NOperationId::AddOptionalValue(operationId, "id", TString());
    }
    return operationId;
}

void ToOperation(const NKikimrAnalyzeOp::TAnalyzeOperation& op, Ydb::Operations::Operation* operation) {
    operation->set_id(NOperationId::ProtoToString(ToOperationId(op)));

    if (op.HasCreateTime()) {
        *operation->mutable_create_time() = op.GetCreateTime();
    }
    if (op.HasEndTime()) {
        *operation->mutable_end_time() = op.GetEndTime();
    }
    if (op.IssuesSize() > 0) {
        operation->mutable_issues()->CopyFrom(op.GetIssues());
    }

    switch (op.GetState()) {
        case Ydb::Table::AnalyzeState::STATE_DONE:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            break;
        case Ydb::Table::AnalyzeState::STATE_CANCELLED:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::CANCELLED);
            break;
        case Ydb::Table::AnalyzeState::STATE_FAILED:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::GENERIC_ERROR);
            break;
        default:
            operation->set_ready(false);
            break;
    }

    Ydb::Table::AnalyzeMetadata metadata;
    metadata.set_state(op.GetState());
    metadata.set_progress(op.GetProgress());
    for (const auto& path : op.GetPaths()) {
        metadata.add_paths(path);
    }
    for (const auto& path : op.GetInProgressPaths()) {
        metadata.add_in_progress_paths(path);
    }
    for (const auto& path : op.GetDonePaths()) {
        metadata.add_done_paths(path);
    }

    operation->mutable_metadata()->PackFrom(metadata);
}

bool TryGetUlidId(const NOperationId::TOperationId& operationId, TString& binaryId) {
    const auto& ids = operationId.GetValue("id");
    if (ids.size() != 1) {
        return false;
    }
    TULID ulid;
    if (!ulid.ParseString(*ids[0])) {
        return false;
    }
    binaryId = ulid.ToBinary();
    return true;
}

bool TryGetId(const NOperationId::TOperationId& operationId, ui64& id) {
    const auto& ids = operationId.GetValue("id");

    if (ids.size() != 1) {
        return false;
    }

    if (!TryFromString(*ids[0], id)) {
        return false;
    }

    return id;
}


} // namespace NGRpcService
} // namespace NKikimr
