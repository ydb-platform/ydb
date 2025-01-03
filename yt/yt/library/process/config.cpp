#include "config.h"

namespace NYT::NPipes {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TIODispatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_polling_period", &TThis::ThreadPoolPollingPeriod)
        .Default(TDuration::MilliSeconds(10));
}

TIODispatcherConfigPtr TIODispatcherConfig::ApplyDynamic(
    const TIODispatcherDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(mergedConfig->ThreadPoolPollingPeriod, dynamicConfig->ThreadPoolPollingPeriod);
    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TIODispatcherDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_polling_period", &TThis::ThreadPoolPollingPeriod)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
