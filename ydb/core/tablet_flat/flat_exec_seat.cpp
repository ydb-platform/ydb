#include "flat_exec_seat.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    void TSeat::Complete(const TActorContext& ctx, bool isRW) {
        if (Y_UNLIKELY(IsTerminated())) {
            Y_ENSURE(!isRW, "Terminating a read-write transaction");
            Self->Terminate(TerminationReason, ctx);
            Self->TxSpan.EndError("Terminated");
            return;
        }

        NWilson::TSpan span(TWilsonTablet::TabletDetailed, Self->TxSpan.GetTraceId(), "Tablet.Transaction.Complete");
        for (auto& callback : OnPersistent) {
            callback();
        }
        Self->Complete(ctx);
        span.End();

        Self->TxSpan.Attribute("rw", isRW);
        Self->TxSpan.EndOk();
    }

} // namespace NTabletFlatExecutor
} // namespace NKikimr
