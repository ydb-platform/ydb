#include "probes.h"

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <mutex>

LWTRACE_DEFINE_PROVIDER(DATASHARD_PROVIDER)

namespace NKikimr::NDataShard {

    void RegisterDataShardProbes() {
        static std::once_flag flag;

        std::call_once(flag, []{
            NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(DATASHARD_PROVIDER));
        });
    }

}
