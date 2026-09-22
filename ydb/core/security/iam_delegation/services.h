#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NIamDelegation {

// The services behind these ids trust their callers: nothing in them checks the sender or the subject of
// a request. TEvGetToken mints a token of any (cloud, service account) pair with the authority of the
// node's system service account, and TEvSetupDelegation / TEvRevokeDelegation act on behalf of the
// SubjectId they carry. A caller must therefore authorize the subject for the secret and the service
// account / cloud pair before asking; that is the job of the secret machinery these services are made
// for, and nothing else on the node should use these ids.

// Node-local service performing SetupDelegation / RevokeDelegation calls.
inline NActors::TActorId MakeIamDelegationServiceId(ui32 nodeId = 0) {
    return NActors::TActorId(nodeId, "iam_dlg_srv");
}

// Node-local service holding the token of YDB's own system service account (see system_token_service.h).
inline NActors::TActorId MakeIamSystemTokenServiceId(ui32 nodeId = 0) {
    return NActors::TActorId(nodeId, "iam_sys_tok");
}

// Node-local service issuing tokens for delegated service accounts.
inline NActors::TActorId MakeIamDelegatedTokenServiceId(ui32 nodeId = 0) {
    return NActors::TActorId(nodeId, "iam_dlg_tok");
}

} // namespace NKikimr::NIamDelegation
