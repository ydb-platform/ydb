#include "retry_gate.h"
#include "iam_actor_base.h"

#include <ydb/core/security/iam_delegation/tablet/client/client.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/async/timeout.h>
#include <util/generic/scope.h>

namespace NKikimr::NIamDelegation {
namespace P = NKikimrIamDelegation;
using namespace NActors;

TAsyncBackoff& TIamRetryGate::Waiter(const P::TServiceKey& key) {
    const auto encoded = key.SerializeAsString();
    if (const auto it = Waiters.find(encoded); it != Waiters.end()) {
        return *it->second;
    }
    if (Waiters.size() >= 1024) {
        throw TIamCallError(Ydb::StatusIds::OVERLOADED) << "Too many IAM service retry scopes";
    }
    auto waiter = MakeHolder<TAsyncBackoff>(TDuration::Seconds(1), TDuration::Minutes(1),
        TAsyncBackoff::EOwnership::External);
    auto* result = waiter.Get();
    Waiters.emplace(encoded, std::move(waiter));
    return *result;
}

void TIamRetryGate::Observe(const P::TServiceKey& key, const P::TServiceGateResult& result) {
    if (!result.HasState() || !result.GetState().GetVersion() || !result.HasServerNowUs()
        || result.GetState().GetIteration() > 8 || result.GetDisposition() == P::SERVICE_GATE_UNSPECIFIED) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "Invalid IAM service retry state";
    }
    const auto due = result.GetState().GetNextAllowedAtUs();
    const auto now = result.GetServerNowUs();
    const auto remaining = due > now ? due - now : 0;
    if (remaining > TDuration::Minutes(5).MicroSeconds()) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "Invalid IAM service retry deadline";
    }
    // Translate the server's remaining interval, not its wall clock, into this
    // actor's monotonic clock. Network latency makes this conservative.
    const auto wakeup = remaining
        ? TActivationContext::Monotonic() + TDuration::MicroSeconds(remaining) : TMonotonic::Zero();
    Waiter(key).Observe(result.GetState().GetVersion(), wakeup);
}

async<P::TResponse> TIamRetryGate::Call(P::TRequest request, TMonotonic deadline) {
    if (!TabletId) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "IAM service retry tablet is not configured";
    }
    if (TActivationContext::Monotonic() >= deadline) {
        throw TIamCallError(Ydb::StatusIds::TIMEOUT) << "IAM service retry deadline exceeded";
    }
    const auto client = TActivationContext::Register(CreateIamDelegationTabletRequestActor(TabletId));
    auto* actorSystem = TActivationContext::ActorSystem();
    // Capture the system, not actor TLS: this cleanup also runs on hard teardown.
    Y_DEFER { actorSystem->Send(client, new TEvents::TEvPoison); };
    auto event = MakeHolder<TEvIamDelegationTablet::TEvRequest>();
    event->Record = std::move(request);
    auto response = co_await WithDeadline(deadline,
        [client, event = std::move(event)]() mutable -> async<TEvIamDelegationTablet::TEvResponse::TPtr> {
            co_return co_await ActorRequest<TEvIamDelegationTablet::TEvResponse>(client, event.Release());
        });
    if (!response) {
        throw TIamCallError(Ydb::StatusIds::TIMEOUT) << "IAM service retry tablet timed out";
    }
    auto result = std::move((*response)->Get()->Record);
    if (result.GetStatus() != P::SUCCESS || !result.HasServiceGate()) {
        // An older backend/unknown command must fail closed, never silently
        // authorize an IAM call or fall back to independent operation backoff.
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "IAM service retry tablet is unavailable or unsupported";
    }
    co_return result;
}

async<ui64> TIamRetryGate::Wait(P::TServiceKey key, TMonotonic deadline, TDuration requestTimeout) {
    auto& waiter = Waiter(key);
    for (;;) {
        if (TActivationContext::Monotonic() >= deadline) {
            throw TIamCallError(Ydb::StatusIds::TIMEOUT) << "IAM service retry deadline exceeded";
        }
        const auto version = co_await WithDeadline(deadline, [&]() -> async<ui64> {
            co_return co_await waiter.Wait(requestTimeout);
        });
        if (!version || TActivationContext::Monotonic() >= deadline) {
            throw TIamCallError(Ydb::StatusIds::TIMEOUT) << "IAM service retry deadline exceeded";
        }
        P::TRequest request;
        auto* check = request.MutableCheckService();
        *check->MutableKey() = key;
        check->SetExpectedVersion(*version);
        check->SetRequestTimeoutUs(requestTimeout.MicroSeconds());
        const auto reply = co_await Call(std::move(request), deadline);
        const auto& result = reply.GetServiceGate();
        Observe(key, result);
        if (result.GetDisposition() == P::SERVICE_ADMITTED) {
            co_return result.GetState().GetVersion();
        }
    }
}

async<void> TIamRetryGate::Report(P::TServiceKey key, ui64 version, P::EServiceHealth health, TMonotonic deadline) {
    if (TActivationContext::Monotonic() >= deadline) {
        co_return;
    }
    P::TRequest request;
    auto* report = request.MutableReportService();
    *report->MutableKey() = key;
    report->SetVersion(version);
    report->SetHealth(health);
    try {
        const auto result = co_await Call(std::move(request), deadline);
        Observe(key, result.GetServiceGate());
    } catch (const TIamCallError&) {
        // A lost health report must not discard an authoritative IAM operation
        // ID/result. Its recovery admission times out in the durable record.
        YDB_LOG_WARN_COMP(NKikimrServices::IAM_DELEGATION, "IAM service health report was not confirmed");
    }
}
} // namespace NKikimr::NIamDelegation
