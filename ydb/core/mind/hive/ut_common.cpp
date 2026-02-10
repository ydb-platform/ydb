#include "ut_common.h"

namespace NKikimr {

NKikimrTabletBase::TEvGetCountersResponse GetCounters(TTestActorRuntime& runtime, ui64 tabletId) {
    const auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(tabletId, sender, new TEvTablet::TEvGetCounters);
    auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvGetCountersResponse>(sender);

    return ev->Get()->Record;
}

ui64 GetSimpleCounter(TTestActorRuntime& runtime, ui64 tabletId,
        NHive::ESimpleCounters counter)
{
  return GetCounters(runtime, tabletId)
      .GetTabletCounters()
      .GetAppCounters()
      .GetSimpleCounters(counter)
      .GetValue();
}

}
