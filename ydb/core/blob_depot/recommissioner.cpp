#include "blob_depot_tablet.h"
#include "recommissioner.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    using TRecommissioner = TBlobDepot::TGroupRecommissioner;

    void TRecommissioner::Bootstrap() {
        if (Token.expired()) {
            return PassAway();
        }

        Become(&TThis::StateFunc);
    }

    void TRecommissioner::PassAway() {
        if (!Token.expired()) {
        }
        TActorBootstrapped::PassAway();
    }

    STATEFN(TRecommissioner::StateFunc) {
        if (Token.expired()) {
            return PassAway();
        }

        switch (const ui32 type = ev->GetTypeRewrite()) {

            default:
                Y_DEBUG_ABORT("unexpected event Type# %08" PRIx32, type);
                YDB_LOG_CRIT("unexpected event",
                    {"Marker", "BDT00"},
                    {"Id", Self->GetLogId()},
                    {"Type", type});
                break;
        }
    }

    void TBlobDepot::StartGroupRecommissioner() {
        if (Config.GetIsDecommittingGroup() && DecommitState != EDecommitState::Done) {
           Y_ABORT_UNLESS(!GroupRecommissionerId);
           Y_ABORT_UNLESS(Data->IsLoaded());
           GroupRecommissionerId = RegisterWithSameMailbox(new TGroupRecommissioner(this));
        }
    }

} // NKikimr::NBlobDepot
