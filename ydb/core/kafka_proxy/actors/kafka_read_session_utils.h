#pragma once

#include "actors.h"

namespace NKafka {

static const TString SUPPORTED_JOIN_GROUP_PROTOCOL = "consumer";

static const TString ASSIGN_STRATEGY_ROUNDROBIN = "roundrobin";
static const TString ASSIGN_STRATEGY_SERVER = "server";

EBalancingMode GetBalancingMode(const TJoinGroupRequestData& request);
std::optional<TConsumerProtocolSubscription> GetSubscriptions(const TJoinGroupRequestData& request);

}
