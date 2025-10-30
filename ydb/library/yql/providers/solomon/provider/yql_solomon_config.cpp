#include "yql_solomon_config.h"

namespace NYql {

using namespace NCommon;

TSolomonConfiguration::TSolomonConfiguration()
{
    REGISTER_SETTING(*this, _EnableReading);
    REGISTER_SETTING(*this, _EnableRuntimeListing);
    REGISTER_SETTING(*this, _EnableSolomonClientPostApi);
    REGISTER_SETTING(*this, _TruePointsFindRange);
    REGISTER_SETTING(*this, _MaxListingPageSize);
    REGISTER_SETTING(*this, MetricsQueueBatchCountLimit);
    REGISTER_SETTING(*this, ComputeActorBatchSize);
    REGISTER_SETTING(*this, MaxApiInflight);
}

TSolomonSettings::TConstPtr TSolomonConfiguration::Snapshot() const {
    return std::make_shared<const TSolomonSettings>(*this);
}

} // NYql
