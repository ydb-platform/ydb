#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    TStorageStatusFlags TBlobDepotAgent::GetStorageStatusFlags() const {
        return NKikimrBlobStorage::StatusIsValid; // FIXME: implement
    }

} // NKikimr::NBlobDepot
