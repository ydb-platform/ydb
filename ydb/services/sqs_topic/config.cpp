#include "config.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NSqsTopic::V1 {

    TMaybe<NKikimrPQ::TPQTabletConfig::TConsumer> GetConsumerConfig(
        const NKikimrPQ::TPQTabletConfig& pqConfig,
        const TStringBuf consumerName,
        const NActors::TActorContext& ctx)
    {
        auto resolvedConsumerName = NPersQueue::ConvertNewConsumerName(TString{consumerName}, ctx);
        auto normalizedConsumerName = NPersQueue::ConvertOldConsumerName(resolvedConsumerName, ctx);
        for (const auto& consumer : pqConfig.GetConsumers()) {
            if (NPersQueue::ConvertOldConsumerName(consumer.GetName(), ctx) == normalizedConsumerName) {
                return consumer;
            }
        }
        return Nothing();
    }

    TString ResolveConsumerNameFromQueueUrl(const TStringBuf consumerFromUrl, const NActors::TActorContext& ctx) {
        return NPersQueue::ConvertNewConsumerName(TString{consumerFromUrl}, ctx);
    }

} // namespace NKikimr::NSqsTopic::V1
