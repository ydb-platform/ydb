#include "olap/bg_tasks/events/global.h"
#include "olap/bg_tasks/transactions/tasks_list.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

void TSchemeShard::Handle(NBackground::TEvListRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(new NBackground::TTxTasksList(this, ev), ctx);
}

} // NKikimr
