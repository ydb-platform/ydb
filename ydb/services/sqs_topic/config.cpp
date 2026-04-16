#include "config.h"

namespace NKikimr::NSqsTopic::V1 {

    TMaybe<NKikimrPQ::TPQTabletConfig::TConsumer> GetConsumerConfig(const NKikimrPQ::TPQTabletConfig& pqConfig, const TStringBuf consumerName) {
        for (const auto& consumer : pqConfig.GetConsumers()) {
            if (consumer.GetName() == consumerName) {
                return consumer;
            }
        }
        return Nothing();
    }
} // namespace NKikimr::NSqsTopic::V1
