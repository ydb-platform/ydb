#include <ydb/core/tx/schemeshard/common/operation_idempotency.h>

#include <ydb/public/api/protos/ydb_operation.pb.h>

#include <util/system/yassert.h>

namespace NKikimr::NSchemeShard {

TString GetUid(const Ydb::Operations::OperationParams& operationParams) {
    const auto it = operationParams.labels().find("uid");
    if (it != operationParams.labels().end()) {
        return it->second;
    }
    return {};
}

TString GetUid(EOperationUidKind kind, const Ydb::Operations::OperationParams& operationParams) {
    Y_ABORT_UNLESS(SupportsOperationUid(kind), "UID support is not registered for operation kind %d", static_cast<int>(kind));
    return GetUid(operationParams);
}

TOperationUidAdmission::EDecision CompareOperationUid(const TOperationUidIdentity& stored, const TOperationUidIdentity& requested) {
    using EDecision = TOperationUidAdmission::EDecision;
    if (requested.UserSID && stored.UserSID != requested.UserSID) {
        return EDecision::OwnerMismatch;
    }
    if (requested.RequestBody && stored.RequestBody != requested.RequestBody) {
        return EDecision::RequestMismatch;
    }
    return EDecision::Replay;
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
    admission.Decision = check(*stored);
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
