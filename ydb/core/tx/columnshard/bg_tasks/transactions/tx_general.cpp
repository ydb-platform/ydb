#include "tx_general.h"
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>

namespace NKikimr::NOlap::NBackground {

void TTxGeneral::Complete(const TActorContext& ctx) {
    DoComplete(ctx);
    if (!!ProgressActorId) {
        ctx.Send(*ProgressActorId, new TEvLocalTransactionCompleted(TxInternalId));
    }
}

}
