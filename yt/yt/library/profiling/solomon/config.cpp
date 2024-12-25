#include "config.h"

#include "private.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TShardConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("filter", &TThis::Filter)
        .Default();

    registrar.Parameter("grid_step", &TThis::GridStep)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TSolomonExporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("grid_step", &TThis::GridStep)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("linger_timeout", &TThis::LingerTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("window_size", &TThis::WindowSize)
        .Default(12);

    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(1);
    registrar.Parameter("encoding_thread_pool_size", &TThis::EncodingThreadPoolSize)
        .Default(1);
    registrar.Parameter("thread_pool_polling_period", &TThis::ThreadPoolPollingPeriod)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("encoding_thread_pool_polling_period", &TThis::EncodingThreadPoolPollingPeriod)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("convert_counters_to_rate_for_solomon", &TThis::ConvertCountersToRateForSolomon)
        .Alias("convert_counters_to_rate")
        .Default(true);
    registrar.Parameter("rename_converted_counters", &TThis::RenameConvertedCounters)
        .Default(true);
    registrar.Parameter("convert_counters_to_delta_gauge", &TThis::ConvertCountersToDeltaGauge)
        .Default(false);

    registrar.Parameter("enable_histogram_compat", &TThis::EnableHistogramCompat)
        .Default(false);

    registrar.Parameter("export_summary", &TThis::ExportSummary)
        .Default(false);
    registrar.Parameter("export_summary_as_max", &TThis::ExportSummaryAsMax)
        .Default(true);
    registrar.Parameter("export_summary_as_avg", &TThis::ExportSummaryAsAvg)
        .Default(false);

    registrar.Parameter("mark_aggregates", &TThis::MarkAggregates)
        .Default(true);

    registrar.Parameter("strip_sensors_name_prefix", &TThis::StripSensorsNamePrefix)
        .Default(false);

    registrar.Parameter("enable_self_profiling", &TThis::EnableSelfProfiling)
        .Default(true);

    registrar.Parameter("report_build_info", &TThis::ReportBuildInfo)
        .Default(true);

    registrar.Parameter("report_kernel_version", &TThis::ReportKernelVersion)
        .Default(true);

    registrar.Parameter("report_restart", &TThis::ReportRestart)
        .Default(true);

    registrar.Parameter("read_delay", &TThis::ReadDelay)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("host", &TThis::Host)
        .Default();

    registrar.Parameter("instance_tags", &TThis::InstanceTags)
        .Default();

    registrar.Parameter("shards", &TThis::Shards)
        .Default();

    registrar.Parameter("response_cache_ttl", &TThis::ResponseCacheTtl)
        .Default(TDuration::Minutes(2));

    registrar.Parameter("update_sensor_service_tree_period", &TThis::UpdateSensorServiceTreePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("producer_collection_batch_size", &TThis::ProducerCollectionBatchSize)
        .Default(DefaultProducerCollectionBatchSize)
        .GreaterThan(0);

    registrar.Parameter("label_sanitization_policy", &TThis::LabelSanitizationPolicy)
        .Default(ELabelSanitizationPolicy::None);

    registrar.Postprocessor([] (TThis* config) {
        if (config->LingerTimeout.GetValue() % config->GridStep.GetValue() != 0) {
            THROW_ERROR_EXCEPTION("\"linger_timeout\" must be multiple of \"grid_step\"");
        }
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->ConvertCountersToRateForSolomon && config->ConvertCountersToDeltaGauge) {
            THROW_ERROR_EXCEPTION("\"convert_counters_to_rate_for_solomon\" and \"convert_counters_to_delta_gauge\" both set to true");
        }
    });

    registrar.Postprocessor([] (TThis* config) {
        for (const auto& [name, shard] : config->Shards) {
            if (!shard->GridStep) {
                continue;
            }

            if (shard->GridStep < config->GridStep) {
                THROW_ERROR_EXCEPTION("shard \"grid_step\" must be greater than global \"grid_step\"");
            }

            if (shard->GridStep->GetValue() % config->GridStep.GetValue() != 0) {
                THROW_ERROR_EXCEPTION("shard \"grid_step\" must be multiple of global \"grid_step\"");
            }

            if (config->LingerTimeout.GetValue() % shard->GridStep->GetValue() != 0) {
                THROW_ERROR_EXCEPTION("\"linger_timeout\" must be multiple shard \"grid_step\"");
            }
        }
    });
}

TShardConfigPtr TSolomonExporterConfig::MatchShard(const std::string& sensorName)
{
    TShardConfigPtr matchedShard;
    int matchSize = -1;

    for (const auto& [name, config] : Shards) {
        for (auto prefix : config->Filter) {
            if (!sensorName.starts_with(prefix)) {
                continue;
            }

            if (static_cast<int>(prefix.size()) > matchSize) {
                matchSize = prefix.size();
                matchedShard = config;
            }
        }
    }

    return matchedShard;
}

ESummaryPolicy TSolomonExporterConfig::GetSummaryPolicy() const
{
    auto policy = ESummaryPolicy::Default;
    if (ExportSummary) {
        policy |= ESummaryPolicy::All;
    }
    if (ExportSummaryAsMax) {
        policy |= ESummaryPolicy::Max;
    }
    if (ExportSummaryAsAvg) {
        policy |= ESummaryPolicy::Avg;
    }

    return policy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
