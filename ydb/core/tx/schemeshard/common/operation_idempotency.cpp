#include <ydb/core/tx/schemeshard/common/operation_idempotency.h>

#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <util/system/yassert.h>

namespace NKikimr::NSchemeShard {

TString GetUid(const Ydb::Operations::OperationParams& operationParams) {
    if (const auto* uid = FindOperationByUid(operationParams.labels(), "uid")) {
        return *uid;
    }
    return {};
}

TString GetUid(Ydb::TOperationId::EKind kind, const Ydb::Operations::OperationParams& operationParams) {
    Y_ABORT_UNLESS(SupportsOperationUid(kind), "UID support is not registered for operation kind %d", static_cast<int>(kind));
    return GetUid(operationParams);
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
