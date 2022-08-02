#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGroupAssimilator : public TActorBootstrapped<TGroupAssimilator> {
        const ui32 GroupId;
        const TActorId BlobDepotId;

    public:
        TGroupAssimilator(ui32 groupId, TActorId blobDepotId)
            : GroupId(groupId)
            , BlobDepotId(blobDepotId)
        {}

        void Bootstrap() {
            (void)GroupId;
        }
    };

} // NKikimr::NBlobDepot
