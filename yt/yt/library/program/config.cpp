#include "config.h"

#include <yt/yt/core/concurrency/fiber_scheduler_thread.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void THeapProfilerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sampling_rate", &TThis::SamplingRate)
        .Default();
    registrar.Parameter("snapshot_update_period", &TThis::SnapshotUpdatePeriod)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

void WarnForUnrecognizedOptionsImpl(
    const NLogging::TLogger& logger,
    const IMapNodePtr& unrecognized)
{
    const auto& Logger = logger;
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        YT_LOG_WARNING("Bootstrap config contains unrecognized options (Unrecognized: %v)",
            ConvertToYsonString(unrecognized, NYson::EYsonFormat::Text));
    }
}

void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config)
{
    WarnForUnrecognizedOptionsImpl(logger, config->GetRecursiveUnrecognized());
}

void AbortOnUnrecognizedOptionsImpl(
    const NLogging::TLogger& logger,
    const IMapNodePtr& unrecognized)
{
    const auto& Logger = logger;
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        YT_LOG_ERROR("Bootstrap config contains unrecognized options, terminating (Unrecognized: %v)",
            ConvertToYsonString(unrecognized, NYson::EYsonFormat::Text));
        YT_ABORT();
    }
}

void AbortOnUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config)
{
    AbortOnUnrecognizedOptionsImpl(logger, config->GetRecursiveUnrecognized());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

