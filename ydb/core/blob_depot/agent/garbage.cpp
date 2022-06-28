#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvCollectGarbage msg, TRequestSender *sender, TRequestContext::TPtr context) {
        auto ev = std::make_unique<TEvBlobDepot::TEvCollectGarbage>();
        msg.Swap(&ev->Record);
        Issue(std::move(ev), sender, std::move(context));
    }

} // NKikimr::NBlobDepot
