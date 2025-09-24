#pragma once

#include "actors.h"

namespace NKafka {

static const TString SUPPORTED_ASSIGN_STRATEGY = "roundrobin";
static const TString SUPPORTED_JOIN_GROUP_PROTOCOL = "consumer";

EBalancingMode GetBalancingMode(const TJoinGroupRequestData& request);
std::optional<TConsumerProtocolSubscription> GetSubscriptions(const TJoinGroupRequestData& request);

}
