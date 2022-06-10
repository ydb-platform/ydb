#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvCommitBlobSeq::TPtr ev) {
        (void)ev;
    }

} // NKikimr::NBlobDepot
