#include "agent.h"
#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    IActor *CreateBlobDepotAgent(ui32 virtualGroupId) {
        return new TBlobDepotAgent(virtualGroupId);
    }

} // NKikimr::NBlobDepot
