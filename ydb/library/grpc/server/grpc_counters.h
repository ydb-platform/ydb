#pragma once

#include <library/cpp/monlib/dynamic_counters/percentile/percentile.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>

namespace NYdbGrpc {

struct ICounterBlock : public TThrRefBase {
    virtual void CountNotOkRequest() = 0;
    virtual void CountNotOkResponse() = 0;
    virtual void CountNotAuthenticated() = 0;
    virtual void CountResourceExhausted() = 0;
    virtual void CountRequestBytes(ui32 requestSize) = 0;
    virtual void CountResponseBytes(ui32 responseSize) = 0;
    virtual void StartProcessing(ui32 requestSize, TInstant deadline) = 0;
    virtual void FinishProcessing(ui32 requestSize, ui32 responseSize, bool ok, ui32 status, TDuration requestDuration) = 0;
    virtual void CountRequestsWithoutDatabase() {}
    virtual void CountRequestsWithoutToken() {}
    virtual void CountRequestWithoutTls() {}

    virtual TIntrusivePtr<ICounterBlock> Clone() { return this; }
    virtual void UseDatabase(const TString& database) { Y_UNUSED(database); }
};

using ICounterBlockPtr = TIntrusivePtr<ICounterBlock>;

class TCounterBlock final : public ICounterBlock {
    NMonitoring::TDynamicCounters::TCounterPtr TotalCounter;
    NMonitoring::TDynamicCounters::TCounterPtr InflyCounter;
    NMonitoring::TDynamicCounters::TCounterPtr NotOkRequestCounter;
    NMonitoring::TDynamicCounters::TCounterPtr NotOkResponseCounter;
    NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    NMonitoring::TDynamicCounters::TCounterPtr InflyRequestBytes;
    NMonitoring::TDynamicCounters::TCounterPtr ResponseBytes;
    NMonitoring::TDynamicCounters::TCounterPtr NotAuthenticated;
    NMonitoring::TDynamicCounters::TCounterPtr ResourceExhausted;
    bool Percentile = false;
    NMonitoring::TPercentileTracker<4, 512, 15> RequestHistMs;
    std::array<NMonitoring::TDynamicCounters::TCounterPtr, 2>  GRpcStatusCounters;

public:
    TCounterBlock(NMonitoring::TDynamicCounters::TCounterPtr totalCounter,
            NMonitoring::TDynamicCounters::TCounterPtr inflyCounter,
            NMonitoring::TDynamicCounters::TCounterPtr notOkRequestCounter,
            NMonitoring::TDynamicCounters::TCounterPtr notOkResponseCounter,
            NMonitoring::TDynamicCounters::TCounterPtr requestBytes,
            NMonitoring::TDynamicCounters::TCounterPtr inflyRequestBytes,
            NMonitoring::TDynamicCounters::TCounterPtr responseBytes,
            NMonitoring::TDynamicCounters::TCounterPtr notAuthenticated,
            NMonitoring::TDynamicCounters::TCounterPtr resourceExhausted,
            TIntrusivePtr<NMonitoring::TDynamicCounters> group)
        : TotalCounter(std::move(totalCounter))
        , InflyCounter(std::move(inflyCounter))
        , NotOkRequestCounter(std::move(notOkRequestCounter))
        , NotOkResponseCounter(std::move(notOkResponseCounter))
        , RequestBytes(std::move(requestBytes))
        , InflyRequestBytes(std::move(inflyRequestBytes))
        , ResponseBytes(std::move(responseBytes))
        , NotAuthenticated(std::move(notAuthenticated))
        , ResourceExhausted(std::move(resourceExhausted))
    {
        if (group) {
            RequestHistMs.Initialize(group, "event", "request", "ms", {0.5f, 0.9f, 0.99f, 0.999f, 1.0f});
            Percentile = true;
        }
    }

    void CountNotOkRequest() override {
        NotOkRequestCounter->Inc();
    }

    void CountNotOkResponse() override {
        NotOkResponseCounter->Inc();
    }

    void CountNotAuthenticated() override {
        NotAuthenticated->Inc();
    }

    void CountResourceExhausted() override {
        ResourceExhausted->Inc();
    }

    void CountRequestBytes(ui32 requestSize) override {
        *RequestBytes += requestSize;
    }

    void CountResponseBytes(ui32 responseSize) override {
        *ResponseBytes += responseSize;
    }

    void StartProcessing(ui32 requestSize, TInstant /*deadline*/) override {
        TotalCounter->Inc();
        InflyCounter->Inc();
        *RequestBytes += requestSize;
        *InflyRequestBytes += requestSize;
    }

    void FinishProcessing(ui32 requestSize, ui32 responseSize, bool ok, ui32 status,
        TDuration requestDuration) override
    {
        Y_UNUSED(status);

        InflyCounter->Dec();
        *InflyRequestBytes -= requestSize;
        *ResponseBytes += responseSize;
        if (!ok) {
            NotOkResponseCounter->Inc();
        }
        if (Percentile) {
            RequestHistMs.Increment(requestDuration.MilliSeconds());
        }
    }

    ICounterBlockPtr Clone() override {
        return this;
    }

    void Update() {
        if (Percentile) {
            RequestHistMs.Update();
        }
    }
};

using TCounterBlockPtr = TIntrusivePtr<TCounterBlock>;

/**
 * Creates new instance of ICounterBlock implementation which does nothing.
 *
 * @return new instance
 */
ICounterBlockPtr FakeCounterBlock();

} // namespace NYdbGrpc
