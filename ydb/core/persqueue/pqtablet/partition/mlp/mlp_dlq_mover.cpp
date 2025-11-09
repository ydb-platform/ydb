#include "mlp_dlq_mover.h"

namespace NKikimr::NPQ::NMLP {

NActors::IActor* CreateDLQMover(const TString& database,
                                const ui64 tabletId,
                                const ui32 partitionId,
                                const TString& consumerName,
                                const TString& destinationTopic,
                                std::deque<ui64>&& offsets) {
    Y_UNUSED(database);
    Y_UNUSED(tabletId);
    Y_UNUSED(partitionId);
    Y_UNUSED(consumerName);
    Y_UNUSED(destinationTopic);
    Y_UNUSED(offsets);

    return nullptr;
}

}
