#include "mlp_consumer.h"

namespace NKikimr::NPQ::NMLP {

TConsumerActor::TConsumerActor(ui64 tabletId, const TActorId& tabletActorId, const TActorId& partitionActorId)
    : TBaseActor(tabletId, tabletActorId, NKikimrServices::EServiceKikimr::PQ_MLP_CONSUMER)
    , PartitionActorId(partitionActorId) {
}

NActors::IActor* CreateConsumerActor(ui64 tabletId, const NActors::TActorId& tabletActorId, const NActors::TActorId& partitionActorId) {
    return new TConsumerActor(tabletId, tabletActorId, partitionActorId);
}

}
