#include "log_shuttle.h"
#include "probes.h"

namespace NLWTrace {
    LWTRACE_USING(LWTRACE_INTERNAL_PROVIDER);

#ifdef LWTRACE_DISABLE

    template <class TDepot>
    bool TLogShuttle<TDepot>::DoFork(TShuttlePtr& child) {
        Y_UNUSED(child);
        return false;
    }

    template <class TDepot>
    bool TLogShuttle<TDepot>::DoJoin(const TShuttlePtr& child) {
        Y_UNUSED(child);
        return false;
    }

#else

    template <class TDepot>
    bool TLogShuttle<TDepot>::DoFork(TShuttlePtr& child) {
        if (child = Executor->RentShuttle()) {
            child->SetParentSpanId(GetSpanId());
            Executor->Cast(child)->SetIgnore(true);
            TParams params;
            params.Param[0].CopyConstruct<ui64>(child->GetSpanId());
            bool result = DoAddProbe(&LWTRACE_GET_NAME(Fork).Probe, params, 0);
            TUserSignature<ui64>::DestroyParams(params);
            return result;
        }
        AtomicIncrement(ForkFailed);
        return false;
    }

    template <class TDepot>
    bool TLogShuttle<TDepot>::DoJoin(const TShuttlePtr& shuttle) {
        auto* child = Executor->Cast(shuttle);
        TParams params;
        params.Param[0].CopyConstruct<ui64>(child->GetSpanId());
        params.Param[1].CopyConstruct<ui64>(child->TrackLog.Items.size());
        bool result = DoAddProbe(&LWTRACE_GET_NAME(Join).Probe, params, 0);
        TUserSignature<ui64, ui64>::DestroyParams(params);
        if (result) {
            with_lock (Lock) {
                ssize_t n = MaxTrackLength - TrackLog.Items.size();
                for (auto& item: child->TrackLog.Items) {
                    if (n-- <= 0) {
                        TrackLog.Truncated = true;
                        break;
                    }
                    TrackLog.Items.emplace_back(item);
                }
            }
        }
        AtomicAdd(ForkFailed, AtomicGet(child->ForkFailed));
        Executor->ParkShuttle(child);
        return result;
    }

#endif

    template class TLogShuttle<TDurationDepot>;
    template class TLogShuttle<TCyclicDepot>;
}
