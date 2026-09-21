#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NIamDelegation {

// Node-local service performing SetupDelegation / RevokeDelegation calls.
inline NActors::TActorId MakeIamDelegationServiceId(ui32 nodeId = 0) {
    return NActors::TActorId(nodeId, "iam_dlg_srv");
}

// Node-local service issuing tokens for delegated service accounts.
inline NActors::TActorId MakeIamDelegatedTokenServiceId(ui32 nodeId = 0) {
    return NActors::TActorId(nodeId, "iam_dlg_tok");
}

} // namespace NKikimr::NIamDelegation
