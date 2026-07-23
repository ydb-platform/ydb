#include "config.h"

namespace NYT::NTracing {

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

void TJaegerTracerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("collector_channel", &TThis::CollectorChannel)
        .Alias("collector_channel_config")
        .Optional();
    registrar.Parameter("max_request_size", &TThis::MaxRequestSize)
        .Default();
    registrar.Parameter("max_memory", &TThis::MaxMemory)
        .Default();
    registrar.Parameter("subsampling_rate", &TThis::SubsamplingRate)
        .Default();
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJaegerTracerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("collector_channel_config", &TThis::CollectorChannelConfig)
        .Optional();

    // 10K nodes x 128 KB / 15s == 85mb/s
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("stop_timeout", &TThis::StopTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("queue_stall_timeout", &TThis::QueueStallTimeout)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("max_request_size", &TThis::MaxRequestSize)
        .Default(128_KB)
        .LessThanOrEqual(4_MB);
    registrar.Parameter("max_batch_size", &TThis::MaxBatchSize)
        .Default(128);
    registrar.Parameter("max_memory", &TThis::MaxMemory)
        .Default(1_GB);
    registrar.Parameter("subsampling_rate", &TThis::SubsamplingRate)
        .Default();
    registrar.Parameter("reconnect_period", &TThis::ReconnectPeriod)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("endpoint_channel_timeout", &TThis::EndpointChannelTimeout)
        .Default(TDuration::Hours(2));

    registrar.Parameter("service_name", &TThis::ServiceName)
        .Default();
    registrar.Parameter("process_tags", &TThis::ProcessTags)
        .Default();
    registrar.Parameter("enable_pid_tag", &TThis::EnablePidTag)
        .Default(false);

    registrar.Parameter("tvm_service", &TThis::TvmService)
        .Optional();

    registrar.Parameter("test_drop_spans", &TThis::TestDropSpans)
        .Default(false);
}

TJaegerTracerConfigPtr TJaegerTracerConfig::ApplyDynamic(const TJaegerTracerDynamicConfigPtr& dynamicConfig) const
{
    auto config = New<TJaegerTracerConfig>();
    config->CollectorChannelConfig = CollectorChannelConfig;
    if (dynamicConfig->CollectorChannel) {
        config->CollectorChannelConfig = dynamicConfig->CollectorChannel;
    }

    config->FlushPeriod = dynamicConfig->FlushPeriod.value_or(FlushPeriod);
    config->QueueStallTimeout = QueueStallTimeout;
    config->MaxRequestSize = dynamicConfig->MaxRequestSize.value_or(MaxRequestSize);
    config->MaxBatchSize = MaxBatchSize;
    config->MaxMemory = dynamicConfig->MaxMemory.value_or(MaxMemory);
    config->SubsamplingRate = SubsamplingRate;
    if (dynamicConfig->SubsamplingRate) {
        config->SubsamplingRate = dynamicConfig->SubsamplingRate;
    }

    config->ServiceName = ServiceName;
    config->ProcessTags = ProcessTags;
    config->EnablePidTag = EnablePidTag;
    config->TvmService = TvmService;
    config->TestDropSpans = TestDropSpans;

    config->Postprocess();
    return config;
}

bool TJaegerTracerConfig::IsEnabled() const
{
    return ServiceName && CollectorChannelConfig;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
