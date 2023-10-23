#pragma once
#include "defs.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {
namespace NLongTxService {

    struct TLongTxServiceCounters : public TAtomicRefCount<TLongTxServiceCounters> {
        using TGroupPtr = TIntrusivePtr<NMonitoring::TDynamicCounters>;
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

        TCounterPtr AcquireReadSnapshotInRequests;
        TCounterPtr AcquireReadSnapshotOutRequests;
        TCounterPtr AcquireReadSnapshotInInFlight;
        TCounterPtr AcquireReadSnapshotOutInFlight;

        explicit TLongTxServiceCounters(const TGroupPtr& group);
    };

    struct TLongTxServiceSettings {
        TIntrusiveConstPtr<TLongTxServiceCounters> Counters;
        // TODO: add settings for long tx service
    };

    IActor* CreateLongTxService(const TLongTxServiceSettings& settings = {});

} // namespace NLongTxService
} // namespace NKikimr
