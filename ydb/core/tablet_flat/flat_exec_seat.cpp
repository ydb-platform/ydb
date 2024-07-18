#include "flat_exec_seat.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    void TSeat::Complete(const TActorContext& ctx, bool isRW) noexcept {
        NWilson::TSpan span(TWilsonTablet::TabletDetailed, Self->TxSpan.GetTraceId(), "Tablet.Transaction.Complete");
        for (auto& callback : OnPersistent) {
            callback();
        }
        Self->Complete(ctx);
        span.End();

        Self->TxSpan.Attribute("rw", isRW);
        Self->TxSpan.EndOk();
    }

    void TSeat::Terminate(ETerminationReason reason, const TActorContext& ctx) noexcept {
        Self->Terminate(reason, ctx);

        Self->TxSpan.EndError("Terminated");
    }

} // namespace NTabletFlatExecutor
} // namespace NKikimr
