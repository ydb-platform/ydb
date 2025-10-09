#include "mlp_consumer.h"

namespace NKikimr::NPQ::NMLP {


NActors::IActor* CreateConsumerActor(const NActors::TActorId& tabletActorId, const NActors::TActorId& partitionActorId) {
    return new TConsumerActor(tabletActorId, partitionActorId);
}

}
