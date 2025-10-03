#include "kafka_read_session_utils.h"
#include <ydb/core/base/appdata.h>

namespace NKafka {

EBalancingMode GetBalancingMode(const TJoinGroupRequestData& request) {
    if (NKikimr::AppData()->FeatureFlags.GetEnableKafkaNativeBalancing()) {
        return request.ProtocolType == SUPPORTED_JOIN_GROUP_PROTOCOL && AnyOf(request.Protocols, [](const TJoinGroupRequestData::TJoinGroupRequestProtocol& p) {
            return p.Name == ASSIGN_STRATEGY_SERVER;
        }) ? EBalancingMode::Server : EBalancingMode::Native;
    } else {
        return EBalancingMode::Server;
    }
}

std::optional<TConsumerProtocolSubscription> GetSubscriptions(const TJoinGroupRequestData& request) {
    if (request.ProtocolType != SUPPORTED_JOIN_GROUP_PROTOCOL) {
        return std::nullopt;
    }

    auto* p = FindIf(request.Protocols, [](const TJoinGroupRequestData::TJoinGroupRequestProtocol& p) {
        return p.Name == ASSIGN_STRATEGY_ROUNDROBIN || p.Name == ASSIGN_STRATEGY_SERVER;
    });

    if (!p) {
        return std::nullopt;
    }

    auto& metadata = p->Metadata;

    TKafkaVersion version = *(TKafkaVersion*)(metadata.value().data() + sizeof(TKafkaVersion));

    TBuffer buffer(metadata.value().data() + sizeof(TKafkaVersion), metadata.value().size_bytes() - sizeof(TKafkaVersion));
    TKafkaReadable readable(buffer);

    TConsumerProtocolSubscription result;
    result.Read(readable, version);

    return result;
}

}