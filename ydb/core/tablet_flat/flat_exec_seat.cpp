#include "flat_exec_seat.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    void TSeat::Complete(const TActorContext& ctx, bool isRW) noexcept {
        for (auto& callback : OnPersistent) {
            callback();
        }
        Self->Complete(ctx);

        Self->TxSpan.Attribute("rw", isRW);
        Self->TxSpan.EndOk();
    }

    void TSeat::Terminate(ETerminationReason reason, const TActorContext& ctx) noexcept {
        Self->Terminate(reason, ctx);

        Self->TxSpan.EndError("Terminated");
    }

} // namespace NTabletFlatExecutor
} // namespace NKikimr
