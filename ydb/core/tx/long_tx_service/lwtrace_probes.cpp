#include "lwtrace_probes.h"

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <mutex>

LWTRACE_DEFINE_PROVIDER(LONG_TX_SERVICE_PROVIDER)

namespace NKikimr {
namespace NLongTxService {

    void RegisterLongTxServiceProbes() {
        static std::once_flag flag;

        std::call_once(flag, []{
            NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(LONG_TX_SERVICE_PROVIDER));
        });
    }

}
}
