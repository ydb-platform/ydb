#include "interconnect_common.h"

namespace NActors {

    double TInterconnectProxyCommon::CalculateNetworkUtilization() {
        const ui64 sessions = NumSessionsWithDataInQueue.load();
        const ui64 ts = GetCycleCountFast();
        const ui64 prevts = CyclesOnLastSwitch.exchange(ts);
        const ui64 passed = ts - prevts;
        const ui64 zero = CyclesWithZeroSessions.exchange(0) + (sessions ? 0 : passed);
        const ui64 nonzero = CyclesWithNonzeroSessions.exchange(0) + (sessions ? passed : 0);
        return (double)nonzero / (zero + nonzero);
    }

    void TInterconnectProxyCommon::AddSessionWithDataInQueue() {
        if (!NumSessionsWithDataInQueue++) {
            const ui64 ts = GetCycleCountFast();
            const ui64 prevts = CyclesOnLastSwitch.exchange(ts);
            CyclesWithZeroSessions += ts - prevts;
        }
    }

    void TInterconnectProxyCommon::RemoveSessionWithDataInQueue() {
        if (!--NumSessionsWithDataInQueue) {
            const ui64 ts = GetCycleCountFast();
            const ui64 prevts = CyclesOnLastSwitch.exchange(ts);
            CyclesWithNonzeroSessions += ts - prevts;
        }
    }

}
