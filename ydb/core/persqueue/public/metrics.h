#pragma once

#include <ydb/core/protos/pqconfig.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NPQ {

// QueueUrl already stores ConvertOldConsumerName(config name). Convert that
// path back to the config name and apply the old-name conversion once, same
// as topic.committed_lag_messages. A second conversion treats a path without
// '@' as a shared consumer and prefixes "shared/".
TString MetricsConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig);
TString MetricsConsumerName(const TString& consumer);

} // namespace NKikimr::NPQ
