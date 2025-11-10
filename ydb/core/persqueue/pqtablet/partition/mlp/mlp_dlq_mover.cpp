#include "mlp_dlq_mover.h"

namespace NKikimr::NPQ::NMLP {

NActors::IActor* CreateDLQMover(TDLQMoverSettings&& settings) {
    Y_UNUSED(settings);

    return nullptr;
}

}
