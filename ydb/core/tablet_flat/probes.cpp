#include "probes.h"

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <mutex>

LWTRACE_DEFINE_PROVIDER(TABLET_FLAT_PROVIDER)

namespace NKikimr {
namespace NTabletFlatExecutor {

    void RegisterTabletFlatProbes() {
        static std::once_flag flag;

        std::call_once(flag, []{
            NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(TABLET_FLAT_PROVIDER));
        });
    }

}
}
