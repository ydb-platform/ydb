#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::SendToProxy(ui32 groupId, std::unique_ptr<IEventBase> event, TRequestSender *sender,
            TRequestContext::TPtr context) {
        const ui64 id = NextOtherRequestId++;
        if (groupId == DecommitGroupId) {
            Send(ProxyId, event.release(), 0, id);
        } else {
            SendToBSProxy(SelfId(), groupId, event.release(), id);
        }
        RegisterRequest(id, sender, std::move(context), {}, false);
    }

} // NKikimr::NBlobDepot
