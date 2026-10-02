#include "flat_exec_commit_mgr.h"
#include "flat_exec_seat.h"
#include "probes.h"

LWTRACE_USING(TABLET_FLAT_PROVIDER)

namespace NKikimr {
namespace NTabletFlatExecutor {

    void TCommitManager::TrackCommitTxs(TLogCommit &commit) {
        while (TSeat *seat = commit.PopTx()) {
            LWTRACK(TransactionReadWriteCommit, seat->Self->Orbit, seat->UniqID, commit.Step);
        }
    }

}
}
