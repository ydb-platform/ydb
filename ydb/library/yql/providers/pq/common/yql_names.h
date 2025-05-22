#pragma once

#include <util/generic/strbuf.h>

namespace NYql {

constexpr TStringBuf PartitionsCountProp = "PartitionsCount";
constexpr TStringBuf FederatedClustersProp = "FederatedClusters";
constexpr TStringBuf ConsumerSetting = "Consumer";
constexpr TStringBuf EndpointSetting = "Endpoint";
constexpr TStringBuf SharedReading = "SharedReading";
constexpr TStringBuf Format = "Format";
constexpr TStringBuf UseSslSetting = "UseSsl";
constexpr TStringBuf AddBearerToTokenSetting = "AddBearerToToken";
constexpr TStringBuf WatermarksEnableSetting = "WatermarksEnable";
constexpr TStringBuf WatermarksGranularityUsSetting = "WatermarksGranularityUs";
constexpr TStringBuf WatermarksLateArrivalDelayUsSetting = "WatermarksLateArrivalDelayUs";
constexpr TStringBuf WatermarksIdlePartitionsSetting = "WatermarksIdlePartitions";
constexpr TStringBuf ReconnectPeriod = "ReconnectPeriod";
constexpr TStringBuf ReadGroup = "ReadGroup";

} // namespace NYql
