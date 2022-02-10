#include "tablet.h"

#include "probes.h"
#include "tablet_impl.h"

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

namespace NKikimr {
namespace NKesus {

IActor* CreateKesusTablet(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TKesusTablet(tablet, info);
}

void AddKesusProbesList() {
    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(KESUS_QUOTER_PROVIDER));
}

}
}
