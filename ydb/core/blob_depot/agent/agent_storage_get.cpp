#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TExecutingQuery *TBlobDepotAgent::CreateExecutingQuery<TEvBlobStorage::EvGet>(std::unique_ptr<IEventHandle> ev) {
        return (void)ev, nullptr;
    }

} // NKikimr::NBlobDepot
