#pragma once

#include <ydb/core/security/iam_delegation/tablet/protos/iam_delegation.pb.h>
#include <ydb/core/util/async_backoff.h>
#include <ydb/library/actors/async/async.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NKikimr::NIamDelegation {

// Actor-owned waiter/deadline cache. Only the service tablet grants admission
// and changes failure backoff; coroutine frames own each request/permit/deadline.
class TIamRetryGate {
public:
    explicit TIamRetryGate(ui64 tabletId) : TabletId(tabletId) {}

    NActors::async<ui64> Wait(NKikimrIamDelegation::TServiceKey key,
        NActors::TMonotonic deadline, TDuration requestTimeout);
    NActors::async<void> Report(NKikimrIamDelegation::TServiceKey key, ui64 version,
        NKikimrIamDelegation::EServiceHealth health, NActors::TMonotonic deadline);

private:
    TAsyncBackoff& Waiter(const NKikimrIamDelegation::TServiceKey& key);
    void Observe(const NKikimrIamDelegation::TServiceKey& key,
        const NKikimrIamDelegation::TServiceGateResult& result);
    NActors::async<NKikimrIamDelegation::TResponse> Call(
        NKikimrIamDelegation::TRequest request, NActors::TMonotonic deadline);

    const ui64 TabletId;
    THashMap<TString, THolder<TAsyncBackoff>> Waiters;
};

} // namespace NKikimr::NIamDelegation
