#include "sampler.h"

namespace NYT::NTracing {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

void TSamplerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("global_sample_rate", &TThis::GlobalSampleRate)
        .Default(0.0);
    registrar.Parameter("user_sample_rate", &TThis::UserSampleRate)
        .Default();
    registrar.Parameter("user_endpoints", &TThis::UserEndpoint)
        .Default();
    registrar.Parameter("clear_sampled_flag", &TThis::ClearSampledFlag)
        .Default();

    registrar.Parameter("min_per_user_samples", &TThis::MinPerUserSamples)
        .Default(0);
    registrar.Parameter("min_per_user_samples_period", &TThis::MinPerUserSamplesPeriod)
        .Default(TDuration::Minutes(1));
}

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

        // TODO(babenko): switch to std::string
        auto profiler = Profiler_.WithSparse().WithTag("user", TString(user));
        state->TracesSampledByUser = profiler.Counter("/traces_sampled_by_user");
        state->TracesSampledByProbability = profiler.Counter("/traces_sampled_by_probability");

        return state;
    });

    std::optional<TString> endpoint;
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
