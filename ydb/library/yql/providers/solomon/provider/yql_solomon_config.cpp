#include "yql_solomon_config.h"

namespace NYql {

using namespace NCommon;

TSolomonConfiguration::TSolomonConfiguration()
{
    REGISTER_SETTING(*this, _EnableReading);
    REGISTER_SETTING(*this, MetricsQueuePageSize);
    REGISTER_SETTING(*this, MetricsQueuePrefetchSize);
    REGISTER_SETTING(*this, MetricsQueueBatchCountLimit);
    REGISTER_SETTING(*this, SolomonClientDefaultReplica);
    REGISTER_SETTING(*this, MaxInflightDataRequests);
    REGISTER_SETTING(*this, ComputeActorBatchSize);
}

TSolomonSettings::TConstPtr TSolomonConfiguration::Snapshot() const {
    return std::make_shared<const TSolomonSettings>(*this);
}

} // NYql
