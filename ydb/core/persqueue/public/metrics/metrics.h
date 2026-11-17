#pragma once

#include <ydb/core/protos/pqconfig.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NPQ {

// Same as above. PQConfig is taken from the static AppData().
TString MetricsConsumerName(const NKikimrPQ::TPQTabletConfig::TConsumer& consumer);
TString MetricsConsumerName(const TString& consumerName);

// Name for the "consumer" metric label. PQConfig decides whether the name is converted.
TString MetricsConsumerName(const NKikimrPQ::TPQTabletConfig::TConsumer& consumer, const NKikimrPQ::TPQConfig& pqConfig);
TString MetricsConsumerName(const TString& consumerName, const NKikimrPQ::TPQConfig& pqConfig);

} // namespace NKikimr::NPQ
