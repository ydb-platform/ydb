#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TExecutingQuery *TBlobDepotAgent::CreateExecutingQuery<TEvBlobStorage::EvRange>(std::unique_ptr<IEventHandle> ev) {
        return (void)ev, nullptr;
    }

} // NKikimr::NBlobDepot
