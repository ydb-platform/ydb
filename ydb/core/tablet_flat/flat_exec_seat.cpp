#include "flat_exec_seat.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    void TSeat::Complete(const TActorContext& ctx) noexcept {
        for (auto& callback : OnPersistent) {
            callback();
        }
        Self->Complete(ctx);
    }

} // namespace NTabletFlatExecutor
} // namespace NKikimr
