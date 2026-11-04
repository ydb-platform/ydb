#include "metrics.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NPQ {

TString MetricsConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig) {
    const TString canonical = NPersQueue::ConvertNewConsumerName(consumer, pqConfig);
    return NPersQueue::ConvertOldConsumerName(canonical, pqConfig);
}

TString MetricsConsumerName(const TString& consumer) {
    return MetricsConsumerName(consumer, AppData()->PQConfig);
}

} // namespace NKikimr::NPQ
