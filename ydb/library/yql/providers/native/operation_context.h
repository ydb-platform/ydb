#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <library/cpp/threading/cancellation/cancellation_token.h>

#include <functional>
#include <memory>

namespace NActors {
class TActorSystem;
class IActor;
}

namespace NYql::NNative {

struct TOperationContext {
    TInstant Deadline = TInstant::Max();
    NThreading::TCancellationToken Cancellation = NThreading::TCancellationToken::Default();
    // Every SDK request/callback and pending result must retain this lease until
    // its buffers have been destroyed. Destruction releases quota asynchronously.
    std::shared_ptr<void> MemoryLease;
};

class IAsyncMemoryQuota {
public:
    virtual ~IAsyncMemoryQuota() = default;
    virtual NThreading::TFuture<std::shared_ptr<void>> Acquire(
        ui64 bytes, TInstant deadline, NThreading::TCancellationToken cancellation) = 0;
    // Cancel admission. Already issued leases remain valid until their last owner releases them.
    virtual void Shutdown() = 0;
};

// The registrar can place the governor in the compute actor's mailbox when its
// quota manager is not thread safe. Without a registrar it gets its own mailbox.
std::shared_ptr<IAsyncMemoryQuota> CreateAsyncMemoryQuota(
    NActors::TActorSystem* actorSystem,
    NDq::IMemoryQuotaManager::TPtr quota,
    ui64 perOperationLimit,
    std::function<NActors::TActorId(NActors::IActor*)> registerActor = {});

} // namespace NYql::NNative
