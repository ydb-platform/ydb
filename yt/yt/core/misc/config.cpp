#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TLogDigestConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("relative_precision", &TThis::RelativePrecision)
        .Default(0.01)
        .GreaterThan(0);

    registrar.Parameter("lower_bound", &TThis::LowerBound)
        .GreaterThan(0);

    registrar.Parameter("upper_bound", &TThis::UpperBound)
        .GreaterThan(0);

    registrar.Parameter("default_value", &TThis::DefaultValue)
        .Default();

    registrar.Postprocessor([] (TLogDigestConfig* config) {
        // If there are more than 1000 buckets, the implementation of TLogDigest
        // becomes inefficient since it stores information about at least that many buckets.
        const int MaxBucketCount = 1000;
        double bucketCount = log(config->UpperBound / config->LowerBound) / log(1 + config->RelativePrecision);
        if (bucketCount > MaxBucketCount) {
            THROW_ERROR_EXCEPTION("Bucket count is too large")
                << TErrorAttribute("bucket_count", bucketCount)
                << TErrorAttribute("max_bucket_count", MaxBucketCount);
        }
        if (config->DefaultValue && (*config->DefaultValue < config->LowerBound || *config->DefaultValue > config->UpperBound)) {
            THROW_ERROR_EXCEPTION("Default value should be between lower bound and upper bound")
                << TErrorAttribute("default_value", *config->DefaultValue)
                << TErrorAttribute("lower_bound", config->LowerBound)
                << TErrorAttribute("upper_bound", config->UpperBound);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void THistogramDigestConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("absolute_precision", &TThis::AbsolutePrecision)
        .Default(0.01)
        .GreaterThan(0);

    registrar.Parameter("lower_bound", &TThis::LowerBound)
        .Default(0.0);

    registrar.Parameter("upper_bound", &TThis::UpperBound)
        .Default(1.0);

    registrar.Parameter("default_value", &TThis::DefaultValue)
        .Default();

    registrar.Postprocessor([] (THistogramDigestConfig* config) {
        if (config->UpperBound < config->LowerBound) {
            THROW_ERROR_EXCEPTION("Upper bound should be greater than or equal to lower bound")
                << TErrorAttribute("lower_bound", config->LowerBound)
                << TErrorAttribute("upper_bound", config->UpperBound);
        }

        // If there are more buckets, the implementation of THistogramDigest
        // becomes inefficient since it stores information about at least that many buckets.
        const int MaxBucketCount = 10000;
        double bucketCount = (config->UpperBound - config->LowerBound) / config->AbsolutePrecision;
        if (bucketCount > MaxBucketCount) {
            THROW_ERROR_EXCEPTION("Bucket count is too large")
                << TErrorAttribute("bucket_count", bucketCount)
                << TErrorAttribute("max_bucket_count", MaxBucketCount);
        }

        if (config->DefaultValue && (*config->DefaultValue < config->LowerBound || *config->DefaultValue > config->UpperBound)) {
            THROW_ERROR_EXCEPTION("Default value should be between lower bound and upper bound")
                << TErrorAttribute("default_value", *config->DefaultValue)
                << TErrorAttribute("lower_bound", config->LowerBound)
                << TErrorAttribute("upper_bound", config->UpperBound);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void THistoricUsageConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("aggregation_mode", &TThis::AggregationMode)
        .Default(EHistoricUsageAggregationMode::None);

    registrar.Parameter("ema_alpha", &TThis::EmaAlpha)
        // TODO(eshcherbin): Adjust.
        .Default(1.0 / (24.0 * 60.0 * 60.0))
        .GreaterThanOrEqual(0.0);
}

////////////////////////////////////////////////////////////////////////////////

void TAdaptiveHedgingManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_backup_request_ratio", &TThis::MaxBackupRequestRatio)
        .GreaterThan(0.)
        .Optional();

    registrar.Parameter("tick_period", &TThis::TickPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(1));

    registrar.Parameter("hedging_delay_tune_factor", &TThis::HedgingDelayTuneFactor)
        .GreaterThanOrEqual(1.)
        .Default(1.05);
    registrar.Parameter("min_hedging_delay", &TThis::MinHedgingDelay)
        .Default(TDuration::Zero());
    registrar.Parameter("max_hedging_delay", &TThis::MaxHedgingDelay)
        .Default(TDuration::Seconds(10));

    registrar.Postprocessor([] (TAdaptiveHedgingManagerConfig* config) {
        if (config->MinHedgingDelay > config->MaxHedgingDelay) {
            THROW_ERROR_EXCEPTION("\"min_hedging_delay\" cannot be greater than \"max_hedging_delay\"")
                << TErrorAttribute("min_hedging_delay", config->MinHedgingDelay)
                << TErrorAttribute("max_hedging_delay", config->MaxHedgingDelay);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
