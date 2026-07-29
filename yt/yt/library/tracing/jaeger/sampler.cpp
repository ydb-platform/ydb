#include "sampler.h"

#include "config.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTracing {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

bool TSampler::TUserState::TrySampleByMinCount(ui64 minCount, TCpuDuration period)
{
    if (minCount == 0) {
        return false;
    }

    auto lastReset = LastReset.load();
    auto now = GetCpuInstant();
    if (now - lastReset > period) {
        if (LastReset.compare_exchange_strong(lastReset, now)) {
            Sampled.store(0);
        }
    }

    return Sampled.fetch_add(1) < minCount;
}

TSampler::TSampler()
    : TSampler(New<TSamplerConfig>())
{ }

TSampler::TSampler(
    TSamplerConfigPtr config,
    const TProfiler& profiler)
    : Config_(std::move(config))
    , Profiler_(profiler.WithHot())
    , TracesSampled_(Profiler_.Counter("/traces_sampled"))
{ }

void TSampler::SampleTraceContext(const std::string& user, const TTraceContextPtr& traceContext)
{
    auto config = Config_.Acquire();

    auto [userState, inserted] = Users_.FindOrInsert(user, [&] {
        auto state = New<TUserState>();

        auto profiler = Profiler_.WithSparse().WithTag("user", user);
        state->TracesSampledByUser = profiler.Counter("/traces_sampled_by_user");
        state->TracesSampledByProbability = profiler.Counter("/traces_sampled_by_probability");

        return state;
    });

    std::optional<std::string> endpoint;
    auto itEndpoint = config->UserEndpoint.find(user);
    if (itEndpoint != config->UserEndpoint.end()) {
        traceContext->SetTargetEndpoint(itEndpoint->second);
    }

    if (traceContext->IsSampled()) {
        userState->Get()->TracesSampledByUser.Increment();

        if (config->ClearSampledFlag.find(user) != config->ClearSampledFlag.end()) {
            traceContext->SetSampled(false);
        } else {
            TracesSampled_.Increment();
            return;
        }
    }

    if (config->GlobalSampleRate != 0.0) {
        auto p = RandomNumber<double>();
        if (p < config->GlobalSampleRate) {
            userState->Get()->TracesSampledByProbability.Increment();
            TracesSampled_.Increment();
            traceContext->SetSampled(true);
            return;
        }
    }

    auto it = config->UserSampleRate.find(user);
    if (it != config->UserSampleRate.end()) {
        auto p = RandomNumber<double>();
        if (p < it->second) {
            userState->Get()->TracesSampledByProbability.Increment();
            TracesSampled_.Increment();
            traceContext->SetSampled(true);
            return;
        }
    }

    if (userState->Get()->TrySampleByMinCount(config->MinPerUserSamples, DurationToCpuDuration(config->MinPerUserSamplesPeriod))) {
        TracesSampled_.Increment();
        traceContext->SetSampled(true);
        return;
    }
}

void TSampler::UpdateConfig(TSamplerConfigPtr config)
{
    Config_.Store(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
