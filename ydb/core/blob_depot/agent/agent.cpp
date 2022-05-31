#include "agent.h"
#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    IActor *CreateBlobDepotAgent(ui32 virtualGroupId, ui64 tabletId) {
        return new TBlobDepotAgent(virtualGroupId, tabletId);
    }

} // NKikimr::NBlobDepot
