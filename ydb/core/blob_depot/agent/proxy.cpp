#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::SendToProxy(ui32 groupId, std::unique_ptr<IEventBase> event, TRequestSender *sender,
            TRequestContext::TPtr context) {
        const ui64 id = NextRequestId++;
        SendToBSProxy(SelfId(), groupId, event.release(), id);
        RegisterRequest(id, sender, std::move(context), {}, false);
    }

} // NKikimr::NBlobDepot
