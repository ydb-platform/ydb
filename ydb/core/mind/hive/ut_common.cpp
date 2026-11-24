#include "ut_common.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace {

// The response skips unnamed (deprecated) counters, so find a counter by name, not by index
template <class TCounters>
ui64 FindCounter(const TCounters& counters, const google::protobuf::EnumValueDescriptor* value) {
    const TString name = value->file()->options().GetExtension(TabletTypeName) + "/"
        + value->options().GetExtension(CounterOpts).GetName();
    for (const auto& counter : counters) {
        if (counter.GetName() == name) {
            return counter.GetValue();
        }
    }
    UNIT_FAIL("counter not found: " << name);
    return 0;
}

}

NKikimrTabletBase::TEvGetCountersResponse GetCounters(TTestActorRuntime& runtime, ui64 tabletId) {
    const auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(tabletId, sender, new TEvTablet::TEvGetCounters);
    auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvGetCountersResponse>(sender);

    return ev->Get()->Record;
}

ui64 GetSimpleCounter(TTestActorRuntime& runtime, ui64 tabletId,
        NHive::ESimpleCounters counter)
{
  return FindCounter(
      GetCounters(runtime, tabletId).GetTabletCounters().GetAppCounters().GetSimpleCounters(),
      NHive::ESimpleCounters_descriptor()->FindValueByNumber(counter));
}

ui64 GetCumulativeCounter(TTestActorRuntime& runtime, ui64 tabletId,
        NHive::ECumulativeCounters counter)
{
  return FindCounter(
      GetCounters(runtime, tabletId).GetTabletCounters().GetAppCounters().GetCumulativeCounters(),
      NHive::ECumulativeCounters_descriptor()->FindValueByNumber(counter));
}

}
