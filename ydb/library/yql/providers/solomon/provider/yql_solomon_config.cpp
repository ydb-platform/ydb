#include "yql_solomon_config.h"

#include <ydb/library/yql/providers/solomon/common/constants.h>

namespace NYql {

using namespace NCommon;

TSolomonConfiguration::TSolomonConfiguration()
{
    REGISTER_SETTING(*this, _EnableReading);
    REGISTER_SETTING(*this, _EnableRuntimeListing);
    REGISTER_SETTING(*this, _EnableSolomonClientPostApi);
    REGISTER_SETTING(*this, _TruePointsFindRange);
    REGISTER_SETTING(*this, _MaxListingPageSize);
    REGISTER_SETTING(*this, Auth)
        .ValueSetter([this](const TString&, const TString& value) {
            Auth = value;
            for (auto& token: Tokens) {
                token.second = ComposeStructuredTokenJsonForServiceAccount("", "", value);
            }
        });
    REGISTER_SETTING(*this, MetricsQueueBatchCountLimit);
    REGISTER_SETTING(*this, MetricsQueuePrefetchSize);
    REGISTER_SETTING(*this, ComputeActorBatchSize);
    REGISTER_SETTING(*this, MaxApiInflight);
    REGISTER_SETTING(*this, MaxDataInflightBytes);
    REGISTER_SETTING(*this, MaxPointsPerOneRequest);
    REGISTER_SETTING(*this, PoisonTimeoutSec);
    REGISTER_SETTING(*this, RoundRobinStageTimeoutMs);
    REGISTER_SETTING(*this, LabelsListingLimit);
    REGISTER_SETTING(*this, WriteMaxRequestsInflight).Lower(1);
    REGISTER_SETTING(*this, WriteMaxPointsPerRequest).Lower(1).Upper(NSo::NConstants::MaxWritePointsPerRequest);
}

TSolomonSettings::TConstPtr TSolomonConfiguration::Snapshot() const {
    return std::make_shared<const TSolomonSettings>(*this);
}

} // NYql
