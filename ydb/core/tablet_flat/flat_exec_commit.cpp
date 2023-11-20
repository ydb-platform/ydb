#include "flat_exec_commit.h"
#include "flat_exec_seat.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    void TLogCommit::PushTx(TSeat* seat) noexcept {
        Y_DEBUG_ABORT_UNLESS(!seat->NextCommitTx);
        if (LastTx) {
            Y_DEBUG_ABORT_UNLESS(!LastTx->NextCommitTx);
            Y_DEBUG_ABORT_UNLESS(LastTx != seat);
            LastTx->NextCommitTx = seat;
            LastTx = seat;
        } else {
            FirstTx = seat;
            LastTx = seat;
        }
    }

    TSeat* TLogCommit::PopTx() noexcept {
        TSeat* seat = FirstTx;
        if (seat) {
            FirstTx = seat->NextCommitTx;
            seat->NextCommitTx = nullptr;
            if (!FirstTx) {
                LastTx = nullptr;
            }
        }
        return seat;
    }

}
}
