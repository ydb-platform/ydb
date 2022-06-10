#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TExecutingQuery *TBlobDepotAgent::CreateExecutingQuery<TEvBlobStorage::EvPatch>(std::unique_ptr<IEventHandle> ev) {
        return (void)ev, nullptr;
    }

} // NKikimr::NBlobDepot
