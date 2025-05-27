#include "sampling_throttling_control.h"

#include "sampling_throttling_control_internals.h"

namespace NKikimr::NJaegerTracing {

TSamplingThrottlingControl::TSamplingThrottlingControl(std::unique_ptr<TSamplingThrottlingImpl> initialImpl)
    : Impl(std::move(initialImpl))
{}

TSamplingThrottlingControl::~TSamplingThrottlingControl() {
    UpdateImpl(nullptr);
}

NWilson::TTraceId TSamplingThrottlingControl::HandleTracing(const TRequestDiscriminator& discriminator,
        const TMaybe<TString>& traceparent) {
    if (ImplUpdate.load(std::memory_order_relaxed)) {
        auto newImpl = std::unique_ptr<TSamplingThrottlingImpl>(ImplUpdate.exchange(nullptr, std::memory_order_acquire));
        Y_ABORT_UNLESS(newImpl);
        Impl = std::move(newImpl);
    }
    return Impl->HandleTracing(discriminator, traceparent);
}

void TSamplingThrottlingControl::UpdateImpl(std::unique_ptr<TSamplingThrottlingImpl> newImpl) {
    std::unique_ptr<TSamplingThrottlingImpl> guard(ImplUpdate.exchange(newImpl.release(), std::memory_order_acq_rel));
}

} // namespace NKikimr::NJaegerTracing
