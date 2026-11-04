#include "metrics.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NPQ {

TString MetricsConsumerName(const TString& consumerName, const NKikimrPQ::TPQConfig& pqConfig) {
    // CLIENTID_WITHOUT_CONSUMER. A read without a consumer has an empty metric label.
    if (consumerName == "$without_consumer") {
        return {};
    }
    return NPersQueue::ConvertOldConsumerName(consumerName, pqConfig);
}

TString MetricsConsumerName(const NKikimrPQ::TPQTabletConfig::TConsumer& consumer, const NKikimrPQ::TPQConfig& pqConfig) {
    return MetricsConsumerName(consumer.GetName(), pqConfig);
}

TString MetricsConsumerName(const TString& consumerName) {
    return MetricsConsumerName(consumerName, AppData()->PQConfig);
}

TString MetricsConsumerName(const NKikimrPQ::TPQTabletConfig::TConsumer& consumer) {
    return MetricsConsumerName(consumer.GetName());
}

} // namespace NKikimr::NPQ
