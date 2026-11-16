#include "compute.h"

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <atomic>

namespace NKikimr::NMiniKQL::NAsync {

struct TExecutionServices::TImpl {
    struct TWakeupGate {
        const TWakeup Enqueue;
        std::atomic<bool> Pending = false;
        std::atomic<bool> Closed = false;
        void Notify(ui64 generation) {
            if (!Closed.load() && !Pending.exchange(true)) {
                Enqueue(generation);
            }
        }
    };
    std::shared_ptr<TWakeupGate> Gate;
    TExecutionContext Context;
    THashSet<TString> Providers;
    THashSet<ui64> Live;
    THashMap<ui64, TResult> Ready;
    const size_t StepBudget;
    size_t Steps = 0;
    ui64 Turn = 0;
    bool Closed = false;

    TImpl(TWakeup enqueue, size_t budget, TLimits limits)
        : Gate(std::make_shared<TWakeupGate>(std::move(enqueue)))
        , Context([gate = Gate](ui64 generation) { gate->Notify(generation); }, limits)
        , StepBudget(budget)
    {
        Y_ENSURE(budget && Gate->Enqueue, "Async execution requires a budget and an enqueue callback");
    }

    void Drain() {
        for (auto& result : Context.TakeReady()) {
            if (Live.contains(result.Id.Sequence)) {
                Ready.emplace(result.Id.Sequence, std::move(result));
            }
        }
    }
};

TExecutionServices::TExecutionServices(TWakeup enqueue, size_t stepBudget, TLimits limits)
    : Impl_(std::make_unique<TImpl>(std::move(enqueue), stepBudget, limits))
{
}
TExecutionServices::~TExecutionServices() {
    Shutdown();
}
void TExecutionServices::RegisterProvider(TString name, std::shared_ptr<IProvider> provider) {
    Impl_->Context.RegisterProvider(name, std::move(provider));
    Impl_->Providers.insert(std::move(name));
}
bool TExecutionServices::HasProvider(TStringBuf name) const {
    return Impl_->Providers.contains(name);
}
void TExecutionServices::BeginTurn() {
    Y_ENSURE(!Impl_->Closed, "Async execution is closed");
    // Clear BEFORE processing. A completion racing Drain, or a budget rerun
    // requested during this turn, must be able to post the next event.
    Impl_->Gate->Pending.store(false);
    Impl_->Steps = Impl_->StepBudget;
    ++Impl_->Turn;
    Impl_->Drain();
}
bool TExecutionServices::TryStep() {
    Y_ENSURE(!Impl_->Closed && Impl_->Turn, "Async execution requires BeginTurn");
    if (!Impl_->Steps) {
        return false;
    }
    --Impl_->Steps;
    return true;
}
void TExecutionServices::RequestRerun() {
    Impl_->Gate->Notify(Impl_->Context.GetGeneration());
}
ui64 TExecutionServices::GetTurn() const {
    return Impl_->Turn;
}
TSubmission TExecutionServices::Submit(TStringBuf provider, TRequest request) {
    auto submission = Impl_->Context.Submit(provider, std::move(request));
    if (submission.Status == EAdmission::Accepted) {
        Impl_->Live.insert(submission.Id.Sequence);
    }
    return submission;
}
bool TExecutionServices::IsReady(TOperationId id) const {
    return id.Generation == Impl_->Context.GetGeneration() && Impl_->Ready.contains(id.Sequence);
}
std::optional<TResult> TExecutionServices::TakeResult(TOperationId id) {
    if (!IsReady(id)) {
        return {};
    }
    auto result = std::move(Impl_->Ready.at(id.Sequence));
    Impl_->Ready.erase(id.Sequence);
    Impl_->Live.erase(id.Sequence);
    return result;
}
void TExecutionServices::CancelAndForget(TOperationId id) {
    if (id.Generation != Impl_->Context.GetGeneration()) {
        return;
    }
    Impl_->Context.Cancel(id);
    Impl_->Live.erase(id.Sequence);
    Impl_->Ready.erase(id.Sequence);
    Impl_->Drain();
}
void TExecutionServices::Shutdown() {
    if (Impl_->Closed) {
        return;
    }
    Impl_->Closed = true;
    Impl_->Gate->Closed.store(true);
    Impl_->Context.Shutdown();
    Impl_->Live.clear();
    Impl_->Ready.clear();
    Impl_->Drain();
}
TStats TExecutionServices::GetStats() const {
    return Impl_->Context.GetStats();
}

} // namespace NKikimr::NMiniKQL::NAsync
