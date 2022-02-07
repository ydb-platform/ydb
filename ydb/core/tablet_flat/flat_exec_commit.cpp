#include "flat_exec_commit.h"
#include "flat_exec_seat.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    void TLogCommit::PushTx(TSeat* seat) noexcept {
        Y_VERIFY_DEBUG(!seat->NextCommitTx);
        if (LastTx) {
            Y_VERIFY_DEBUG(!LastTx->NextCommitTx);
            Y_VERIFY_DEBUG(LastTx != seat);
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
