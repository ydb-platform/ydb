#pragma once

#include <util/generic/strbuf.h>
#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql {

constexpr TStringBuf PqSource = NDq::PqSource;
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
constexpr TStringBuf WatermarksIdleTimeoutUsSetting = "WatermarksIdleTimeoutUs";
constexpr TStringBuf WatermarksIdlePartitionsSetting = "WatermarksIdlePartitions";
constexpr TStringBuf ReconnectPeriod = "ReconnectPeriod";
constexpr TStringBuf ReadGroup = "ReadGroup";
constexpr TStringBuf SkipJsonErrors = "SkipJsonErrors";
constexpr TStringBuf StreamingTopicRead = "StreamingTopicRead";
constexpr TStringBuf PartitionsBalancingIdleTimeoutUsSetting = "PartitionsBalancingIdleTimeoutUs";
constexpr TStringBuf UserSchemaColumnsSetting = "UserSchemaColumns";

// Write settings

namespace NDeliveryGuaranteeSetting {

static constexpr TStringBuf Name = "deliveryguarantee";
static constexpr TStringBuf PrettyName = "DELIVERY_GUARANTEE";
static constexpr TStringBuf ExactlyOnceValue = "exactly_once";
static constexpr TStringBuf AtLeastOnceValue = "at_least_once";

} // namespace NDeliveryGuaranteeSetting

} // namespace NYql
