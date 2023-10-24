#pragma once

#include "defs.h"
#include <ydb/public/lib/operation_id/operation_id.h>

namespace NKikimrIndexBuilder {
    class TIndexBuild;
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
void ToOperation(const NKikimrIndexBuilder::TIndexBuild& build, Ydb::Operations::Operation* operation);
bool TryGetId(const NOperationId::TOperationId& operationId, ui64& id);


} // namespace NGRpcService
} // namespace NKikimr
