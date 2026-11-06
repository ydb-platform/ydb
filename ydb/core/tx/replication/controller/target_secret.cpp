#include "target_secret.h"

namespace NKikimr::NReplication::NController {

void TTargetSecret::ProgressDestination(const TActorContext& ctx) {
    // TSecretCopier creates the destination during initial copy. Only its
    // removal belongs to the common destination lifecycle.
    if (GetReplication()->GetState() == TReplication::EState::Removing
        && GetDstState() == TReplication::EDstState::Removing)
    {
        TTargetBase::Progress(ctx);
    }
}

}
