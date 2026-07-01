#pragma once

#include "defs.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

namespace NKikimrIndexBuilder {
    class TIndexBuild;
}

namespace NKikimrForcedCompaction {
    class TForcedCompaction;
}

namespace NKikimrAnalyzeOp {
    class TAnalyzeOperation;
}

namespace Ydb {
namespace Operations {
    class Operation;
}
}

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;

IEventBase* CreateNavigateForPath(const TString& path);
TActorId CreatePipeClient(ui64 id, const TActorContext& ctx);
Ydb::TOperationId ToOperationId(const NKikimrIndexBuilder::TIndexBuild& build);
Ydb::TOperationId ToOperationId(const NKikimrForcedCompaction::TForcedCompaction& compaction);
Ydb::TOperationId ToOperationId(const NKikimrAnalyzeOp::TAnalyzeOperation& op);
void ToOperation(const NKikimrIndexBuilder::TIndexBuild& build, Ydb::Operations::Operation* operation);
void ToOperation(const NKikimrForcedCompaction::TForcedCompaction& build, Ydb::Operations::Operation* operation);
void ToOperation(const NKikimrAnalyzeOp::TAnalyzeOperation& op, Ydb::Operations::Operation* operation);
bool TryGetId(const NOperationId::TOperationId& operationId, ui64& id);
bool TryGetUlidId(const NOperationId::TOperationId& operationId, TString& binaryId);


} // namespace NGRpcService
} // namespace NKikimr
