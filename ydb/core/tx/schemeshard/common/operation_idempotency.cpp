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

TOperationUidAdmission TOperationUidAdmission::Prepare(const TOperationUidKey& key,
    EDuplicatePolicy policy, const TLookup& lookup, const TCheck& check)
{
    Y_ABORT_UNLESS(SupportsOperationUid(key.first));
    TOperationUidAdmission admission;
    if (key.second.empty()) {
        return admission;
    }
    const auto stored = lookup(key);
    if (!stored) {
        return admission;
    }
    admission.OperationId = stored->OperationId;
    if (policy == EDuplicatePolicy::Reject) {
        admission.Decision = EDecision::AlreadyExists;
        return admission;
    }
    Y_ABORT_UNLESS(check);
    switch (check(*stored)) {
        case EUidReplayMatch::Match:
            admission.Decision = EDecision::Replay;
            break;
        case EUidReplayMatch::OwnerMismatch:
            admission.Decision = EDecision::OwnerMismatch;
            break;
        case EUidReplayMatch::DomainMismatch:
            admission.Decision = EDecision::DomainMismatch;
            break;
        case EUidReplayMatch::RequestMismatch:
            admission.Decision = EDecision::RequestMismatch;
            break;
    }
    return admission;
}

bool TOperationUidAdmission::Commit(bool admitted, const std::function<void()>& persist) {
    if (!admitted || Decision != EDecision::Proceed) {
        return false;
    }
    Y_ABORT_UNLESS(!Committed);
    persist();
    Committed = true;
    return true;
}

} // namespace NKikimr::NSchemeShard
