#pragma once

#include <util/generic/hash_set.h>
#include <util/stream/buffer.h>
#include <util/system/pipe.h>

#include <array>

namespace NKikimr {

class TTraceCollector : public TSingletonTraits<TTraceCollector> {
    class TPipeConnection;
    class TStackTrace;

public:
    static const THashSet<int> DEFAULT_SIGNALS;

    TTraceCollector(const THashSet<int>& signalHandlers);
    ~TTraceCollector();

private:
    // Main process routines
    void SetSignalHandlers();
    void RestoreSignalHandlers();

    // Forked process routines
    void RunChildMain();
    TString Symbolize(const TStackTrace& stackTrace) const;

private:
    const THashSet<int> HandledSignals;
    THolder<TPipeConnection> Connection;
    pid_t CollectorPid;
    std::array<struct sigaction, NSIG> OldActions;
};

void WaitSignals();

} // namespace NKikimr
