#pragma once

#include "request_discriminator.h"

#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr::NJaegerTracing {

class TSamplingThrottlingControl: public TThrRefBase {
    friend class TSamplingThrottlingConfigurator;
    
public:
    void HandleTracing(NWilson::TTraceId& traceId, const TRequestDiscriminator& discriminator);

    ~TSamplingThrottlingControl();
    
private:
    struct TSamplingThrottlingImpl;

    // Should only be obtained from TSamplingThrottlingConfigurator
    TSamplingThrottlingControl(std::unique_ptr<TSamplingThrottlingImpl> initialImpl);

    void UpdateImpl(std::unique_ptr<TSamplingThrottlingImpl> newParams);

    // Exclusively owned by the only thread, that may call HandleTracing
    std::unique_ptr<TSamplingThrottlingImpl> Impl;

    // Shared between the thread calling HandleTracing and the thread calling UpdateParams
    std::atomic<TSamplingThrottlingImpl*> ImplUpdate{nullptr};
};

} // namespace NKikimr::NJaegerTracing
