#include "config.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NSqsTopic::V1 {

    TMaybe<NKikimrPQ::TPQTabletConfig::TConsumer> GetConsumerConfig(
        const NKikimrPQ::TPQTabletConfig& pqConfig,
        const TStringBuf consumerName,
        const NActors::TActorContext& ctx)
    {
        TString resolvedConsumerName(consumerName);
        if (!NKikimr::AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            resolvedConsumerName = NPersQueue::ConvertNewConsumerName(resolvedConsumerName, ctx);
        }
        auto normalizedConsumerName = NPersQueue::ConvertOldConsumerName(resolvedConsumerName, ctx);
        for (const auto& consumer : pqConfig.GetConsumers()) {
            if (NPersQueue::ConvertOldConsumerName(consumer.GetName(), ctx) == normalizedConsumerName) {
                return consumer;
            }
        }
        return Nothing();
    }

} // namespace NKikimr::NSqsTopic::V1
