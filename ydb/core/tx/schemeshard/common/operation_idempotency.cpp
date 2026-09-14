#include <ydb/core/tx/schemeshard/common/operation_idempotency.h>

#include <ydb/public/api/protos/ydb_operation.pb.h>

namespace NKikimr::NSchemeShard {

TString GetUid(const Ydb::Operations::OperationParams& operationParams) {
    if (const auto* uid = FindOperationByUid(operationParams.labels(), "uid")) {
        return *uid;
    }
    return {};
}

EUidReplayMatch CompareOperationUid(const TOperationUidIdentity& stored, const TOperationUidIdentity& requested) {
    if (requested.UserSID && stored.UserSID != requested.UserSID) {
        return EUidReplayMatch::OwnerMismatch;
    }
    if (requested.DomainPathId && stored.DomainPathId != requested.DomainPathId) {
        return EUidReplayMatch::DomainMismatch;
    }
    if (requested.RequestBody && stored.RequestBody != requested.RequestBody) {
        return EUidReplayMatch::RequestMismatch;
    }
    return EUidReplayMatch::Match;
}

} // namespace NKikimr::NSchemeShard
