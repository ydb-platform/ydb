#include "kqp.h"

#include <ydb/library/actors/core/actorsystem.h>

#include <util/datetime/base.h>

namespace NKikimr::NKqp {

TString ScriptExecutionRunnerActorIdString(const NActors::TActorId& actorId) {
    return TStringBuilder() << "[" << actorId.NodeId() << ":" << actorId.LocalId() << ":" << actorId.Hint() << ":" << actorId.PoolID() << "]";
}

bool ScriptExecutionRunnerActorIdFromString(const TString& executionId, TActorId& actorId) {
    if (executionId.Size() < 5 || executionId[0] != '[' || executionId[executionId.Size() - 1] != ']')
        return false;

    size_t semicolons[3];
    semicolons[0] = executionId.find(':', 1);
    if (semicolons[0] == TStringBuf::npos)
        return false;
    semicolons[1] = executionId.find(':', semicolons[0] + 1);
    if (semicolons[1] == TStringBuf::npos)
        return false;
    semicolons[2] = executionId.find(':', semicolons[1] + 1);
    if (semicolons[2] == TStringBuf::npos)
        return false;

    ui32 nodeId = 0;
    ui64 localId = 0;
    ui32 hint = 0;
    ui32 poolId = 0;
    bool success = TryFromString(executionId.c_str() + 1, semicolons[0] - 1, nodeId)
        && TryFromString(executionId.c_str() + semicolons[0] + 1, semicolons[1] - semicolons[0] - 1, localId)
        && TryFromString(executionId.c_str() + semicolons[1] + 1, semicolons[2] - semicolons[1] - 1, hint)
        && TryFromString(executionId.c_str() + semicolons[2] + 1, executionId.Size() - semicolons[2] - 2, poolId);

    if (!success) {
        return false;
    }
    actorId = NActors::TActorId{nodeId, poolId, localId, hint};
    return true;
}

} // namespace NKikimr::NKqp
