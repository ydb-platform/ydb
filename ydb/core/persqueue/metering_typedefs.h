#pragma once
#include <library/cpp/json/writer/json_value.h>

//ToDo: Remove after switching to new metering service

namespace NKikimr::NMetering {
struct TMetricValue {
    ui64 Quantity;

    /** Start time or moment when event occurred. usage -> start of billing json */
    TInstant StartTime;

    /** 'tags' in billing json. Concatenated with MetricMeta MetricTags (conflicting keys ore overridden)*/
    THashMap <TString, NJson::TJsonValue> MetricTags;

    TInstant EndTime = TInstant::Zero();
};
}
