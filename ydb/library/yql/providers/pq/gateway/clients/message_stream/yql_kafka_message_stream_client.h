#pragma once

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>

namespace NActors {
class TActorSystem;
}   // namespace NActors

namespace NFq {

struct TKafkaMessageStreamClientSettings {
    TString BootstrapBrokers;
    TString Token;
    TString SaslMechanism = "PLAIN";
    bool UseTls = false;
    NActors::TActorSystem* ActorSystem = nullptr;
    ui32 ActorPoolId = 0;
};

// Managed subscriptions only. Durable progress belongs to the Kafka consumer group.
std::shared_ptr<IMessageStreamClient> CreateKafkaMessageStreamClient(TString topic, TKafkaMessageStreamClientSettings settings);

}   // namespace NFq
