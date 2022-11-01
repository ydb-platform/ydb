#pragma once

#include <util/generic/strbuf.h>

namespace NYql {

constexpr TStringBuf PartitionsCountProp = "PartitionsCount";
constexpr TStringBuf ConsumerSetting = "Consumer";
constexpr TStringBuf EndpointSetting = "Endpoint";
constexpr TStringBuf UseSslSetting = "UseSsl";
constexpr TStringBuf AddBearerToTokenSetting = "AddBearerToToken";
constexpr TStringBuf WatermarksEnableSetting = "WatermarksEnable";
constexpr TStringBuf WatermarksGranularityUsSetting = "WatermarksGranularityUs";
constexpr TStringBuf WatermarksLateArrivalDelayUsSetting = "WatermarksLateArrivalDelayUs";
constexpr TStringBuf WatermarksIdlePartitionsSetting = "WatermarksIdlePartitions";

} // namespace NYql
