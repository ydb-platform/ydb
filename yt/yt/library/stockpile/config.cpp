#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TStockpileConfig::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("buffer_size", &TThis::BufferSize)
        .Default(DefaultBufferSize)
        .GreaterThan(0);
    registrar.BaseClassParameter("thread_count", &TThis::ThreadCount)
        .Default(DefaultThreadCount);
    registrar.BaseClassParameter("strategy", &TThis::Strategy)
        .Default(DefaultStrategy);
    registrar.BaseClassParameter("period", &TThis::Period)
        .Default(DefaultPeriod);
}

TStockpileConfigPtr TStockpileConfig::ApplyDynamic(const TStockpileDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));

    if (dynamicConfig->BufferSize) {
        mergedConfig->BufferSize = *dynamicConfig->BufferSize;
    }
    if (dynamicConfig->ThreadCount) {
        mergedConfig->ThreadCount = *dynamicConfig->ThreadCount;
    }
    if (dynamicConfig->Strategy) {
        mergedConfig->Strategy = *dynamicConfig->Strategy;
    }
    if (dynamicConfig->Period) {
        mergedConfig->Period = *dynamicConfig->Period;
    }

    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TStockpileDynamicConfig::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("buffer_size", &TThis::BufferSize)
        .Optional()
        .GreaterThan(0);
    registrar.BaseClassParameter("thread_count", &TThis::ThreadCount)
        .Optional()
        .GreaterThanOrEqual(0);
    registrar.BaseClassParameter("strategy", &TThis::Strategy)
        .Optional();
    registrar.BaseClassParameter("period", &TThis::Period)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
