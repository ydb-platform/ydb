#include "topic_configuration_replicator.h"
#include "topic_configuration_replicator_factory.h"

namespace NKikimr::NPQ {

TTopicConfigurationReplicator::TTopicConfigurationReplicator(ui64 tabletId, const TActorId& tabletActor,
    const NKikimrPQ::TMirrorPartitionConfig& config)
    : TBaseTabletActor(tabletId, tabletActor, NKikimrServices::PERSQUEUE)
    , Config(config)
{}

void TTopicConfigurationReplicator::Bootstrap(const TActorContext&) {
    Become(&TThis::StateWork);
}

STFUNC(TTopicConfigurationReplicator::StateWork) {
    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    }
}

TStructuredMessage TTopicConfigurationReplicator::BuildLogPrefix() const {
    return YDB_LOG_CREATE_MESSAGE({"tablet_id", TabletId}, {"source_topic", Config.GetTopic()});
}

NActors::IActor* CreateTopicConfigurationReplicator(ui64 tabletId, const NActors::TActorId& tabletActor,
    const NKikimrPQ::TMirrorPartitionConfig& config)
{
    return new TTopicConfigurationReplicator(tabletId, tabletActor, config);
}

} // namespace NKikimr::NPQ
